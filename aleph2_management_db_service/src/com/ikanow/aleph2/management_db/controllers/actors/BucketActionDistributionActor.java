/*******************************************************************************
 * Copyright 2015, The IKANOW Open Source Project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
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
import com.ikanow.aleph2.data_model.utils.Patterns;
import com.ikanow.aleph2.data_model.utils.SetOnce;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.UuidUtils;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage.BucketActionEventBusWrapper;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage.BucketActionAnalyticJobMessage.JobMessageType;
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
	 * @param timeout - the timeout for the request
	 * @param not_used - just for consistency with BucketActionChooseActor
	 */
	public BucketActionDistributionActor(final Optional<FiniteDuration> timeout, final String not_used) {
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
			.match(BucketActionCollectedRepliesMessage.class, 
					m -> {
						if (_state.data_import_manager_set.remove(m.source()) || !_state.restrict_replies.get())
						{
							_state.reply_list.addAll(m.replies()
									.stream()
									.map(msg -> BeanTemplateUtils.clone(msg).with(BasicMessageBean::source, m.source()).done())
									.collect(Collectors.toList())
									);
							this.checkIfComplete();
						}
					})
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
				_logger.warn("bucket=" + _state.original_message.get().bucket().full_name()
						+ "; actor_id=" + this.self().toString() + "; zk_path_not_found=" + ActorUtils.BUCKET_ACTION_ZOOKEEPER);
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
			if (shouldLog(_state.original_message.get()))
				_logger.info("bucket=" + _state.original_message.get().bucket().full_name()
						+ "; message_id=" + message
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
				
				if (message instanceof BucketActionMessage.BucketActionAnalyticJobMessage) {
					//These message types are fire+forget
					_state.data_import_manager_set.clear();
					sendReplyAndClose();
				}
				else {
					context().become(_stateAwaitingReplies);
				}
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
		if (shouldLog(_state.original_message.get()) || !_state.down_targeted_clients.isEmpty())
			_logger.info("bucket=" + _state.original_message.get().bucket().full_name()
					+ "; actor_id=" + this.self().toString()
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
		
		_state.original_sender.get().tell(new BucketActionCollectedRepliesMessage(this.getClass().getSimpleName(), _state.reply_list, _state.down_targeted_clients), 
									this.self());		
		this.context().stop(this.self());
	}

	////////////////////////////////////////////////////////////////
	
	/** Handy utility for deciding when to log
	 * @param message
	 * @return
	 */
	private static boolean shouldLog(final BucketActionMessage message) {
		return _logger.isDebugEnabled() 
				||
				Patterns.match(message).<Boolean>andReturn()
					.when(BucketActionMessage.BucketActionOfferMessage.class, 
							msg -> Patterns.match(Optional.ofNullable(msg.message_type()).orElse("")).<Boolean>andReturn()
										.when(type -> BucketActionMessage.PollFreqBucketActionMessage.class.toString().equals(type), __ -> false)
										.when(type -> type.isEmpty(), __ -> false) // (leave "" as a catch all for "don't log")
										.otherwise(__ -> true))
					.when(BucketActionMessage.PollFreqBucketActionMessage.class, __ -> false)
					.when(BucketActionMessage.BucketActionAnalyticJobMessage.class, msg -> (JobMessageType.check_completion != msg.type()))
					.otherwise(__ -> true)
					;		
	}
}
