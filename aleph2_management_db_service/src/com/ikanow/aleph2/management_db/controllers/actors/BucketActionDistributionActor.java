/*******************************************************************************
* Copyright 2015, The IKANOW Open Source Project.
* 
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License, version 3,
* as published by the Free Software Foundation.
* 
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Affero General Public License for more details.
* 
* You should have received a copy of the GNU Affero General Public License
* along with this program. If not, see <http://www.gnu.org/licenses/>.
******************************************************************************/
package com.ikanow.aleph2.management_db.controllers.actors;

import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.curator.framework.CuratorFramework;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException.NoNodeException;

import scala.PartialFunction;
import scala.concurrent.duration.FiniteDuration;
import scala.runtime.BoxedUnit;

import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.SetOnce;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.UuidUtils;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage.BucketActionEventBusWrapper;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage.BucketActionHandlerMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage.BucketActionIgnoredMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage.BucketActionCollectedRepliesMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage.BucketActionTimeoutMessage;
import com.ikanow.aleph2.management_db.services.ManagementDbActorContext;
import com.ikanow.aleph2.management_db.utils.ActorUtils;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.japi.pf.ReceiveBuilder;

/** This actor's role is to send out the received bucket update messages, to marshal the replies
 *  and to send out a combined set of replies to the sender
 * @author acp
 *
 */
public class BucketActionDistributionActor extends AbstractActor {
	private static final Logger _logger = LogManager.getLogger();	
	
	///////////////////////////////////////////
	
	// State
	
	protected class MutableState {
		
		public MutableState() {}		
		protected final List<BasicMessageBean> reply_list = new LinkedList<BasicMessageBean>();
		protected final HashSet<String> data_import_manager_set = new HashSet<String>();
		protected final HashSet<String> down_targeted_clients = new HashSet<String>();
		protected final SetOnce<ActorRef> original_sender = new SetOnce<ActorRef>();
		protected final SetOnce<Boolean> restrict_replies = new SetOnce<Boolean>();
		protected final SetOnce<BucketActionMessage> original_message = new SetOnce<BucketActionMessage>();
	}
	protected final MutableState _state = new MutableState();
	protected final FiniteDuration _timeout;	
	protected final ManagementDbActorContext _system_context;
	
	///////////////////////////////////////////
	
	// Constructor
	
	/** Should only ever be called by the actor system, not by users
	 */
	public BucketActionDistributionActor(final Optional<FiniteDuration> timeout) {
		_timeout = timeout.orElse(BucketActionSupervisor.DEFAULT_TIMEOUT); // (Default timeout 5s) 
		_system_context = ManagementDbActorContext.get();
	}
	
	///////////////////////////////////////////
	
	// State Transitions
	
	private PartialFunction<Object, BoxedUnit> _stateIdle = ReceiveBuilder
			.match(BucketActionMessage.class, 
				m -> {
					this.broadcastAction(m);
					this.checkIfComplete();
				})
			.build();
			
	private PartialFunction<Object, BoxedUnit> _stateAwaitingReplies = ReceiveBuilder
			.match(BucketActionHandlerMessage.class, 
				m -> {
					if (_state.data_import_manager_set.remove(m.source()) || !_state.restrict_replies.get())
					{
						// (note - overwriting the bean message source with the hostname)
						_state.reply_list.add(BeanTemplateUtils
												.clone(m.reply()).with(BasicMessageBean::source, m.source()).done());
						this.checkIfComplete();
					}
				})
			.match(BucketActionIgnoredMessage.class, 
				m -> {
					if (_state.data_import_manager_set.remove(m.source())) {
						this.checkIfComplete();
					}
				})
			.match(BucketActionTimeoutMessage.class, 
				m -> {
					this.sendReplyAndClose();
				})				
			.build();
	
	///////////////////////////////////////////

	// Initial State
	
	 @Override
	 public PartialFunction<Object, BoxedUnit> receive() {
	    return _stateIdle;
	 }
	
	///////////////////////////////////////////

	// Actions
	
	protected void broadcastAction(final BucketActionMessage message) {
		try {
			_state.original_sender.set(this.sender());
			_state.original_message.set(message);
			
			// 1) Get a list of potential actors 
			
			// 1a) Check how many people are registered as listening from zookeeper/curator

			CuratorFramework curator = _system_context.getDistributedServices().getCuratorFramework();
			
			try {
				_state.data_import_manager_set.addAll(curator.getChildren().forPath(ActorUtils.BUCKET_ACTION_ZOOKEEPER));
			}
			catch (NoNodeException e) { 
				// This is OK
				_logger.info("actor_id=" + this.self().toString() + "; zk_path_not_found=" + ActorUtils.BUCKET_ACTION_ZOOKEEPER);
			}			
			if (!message.handling_clients().isEmpty()) { // Intersection of: targeted clients and available clients
				_state.data_import_manager_set.retainAll(message.handling_clients());
				_state.restrict_replies.set(true);
				if (message.handling_clients().size() != _state.data_import_manager_set.size()) {
					//OK what's happened here is that a targeted client is not currently up, we'll just save this
					//and insert them back in at the end (but without timing out)
					_state.down_targeted_clients.addAll(message.handling_clients());
					_state.down_targeted_clients.removeAll(_state.data_import_manager_set);
				}
			}
			else { // (just set the corresponding set_once)
				_state.restrict_replies.set(false);					
			}
			
			//(log)
			_logger.info("message_id=" + message
					+ "; actor_id=" + this.self().toString()
					+ "; candidates_found=" + _state.data_import_manager_set.size());
			
			// 2) Then message all of the actors that we believe are present and wait for the response

			if (!_state.data_import_manager_set.isEmpty()) {
			
				_system_context.getBucketActionMessageBus().publish(new BucketActionEventBusWrapper(this.self(), message));
				
				// 2b) Schedule a timeout
				
				_system_context.getActorSystem().scheduler().scheduleOnce(_timeout, 
							this.self(), new BucketActionTimeoutMessage(UuidUtils.get().getRandomUuid()), 
								_system_context.getActorSystem().dispatcher(), null);
	
				// 3) Transition state
				
				context().become(_stateAwaitingReplies);
			}
			//(else we're going to insta terminate anyway)
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}	
	protected void checkIfComplete() {
		if (_state.data_import_manager_set.isEmpty()) {
			this.sendReplyAndClose();
		}
	}
	protected void sendReplyAndClose() {
		//(log)
		_logger.info("actor_id=" + this.self().toString()
				+ "; replies=" + _state.reply_list.size() + "; timeouts=" + _state.data_import_manager_set.size() + "; down=" + _state.down_targeted_clients.size());

		// (can mutate this since we're about to delete the entire object)
		_state.down_targeted_clients.addAll(_state.data_import_manager_set);
		
		List<BasicMessageBean> convert_timeouts_to_replies = _state.down_targeted_clients.stream()
																.map(source-> {
																	return new BasicMessageBean(
																			new Date(), // date
																			false, // success
																			source,
																			_state.original_message.get().getClass().getSimpleName(),
																			null, // message code
																			"Timeout",
																			null // details
																			);
																})
																.collect(Collectors.toList());
		_state.reply_list.addAll(convert_timeouts_to_replies);
		
		_state.original_sender.get().tell(new BucketActionCollectedRepliesMessage(_state.reply_list, _state.down_targeted_clients), 
									this.self());		
		this.context().stop(this.self());
	}
}
