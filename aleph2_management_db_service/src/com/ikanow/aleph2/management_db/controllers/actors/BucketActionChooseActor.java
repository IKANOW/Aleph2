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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.checkerframework.checker.nullness.qual.NonNull;

import scala.PartialFunction;
import scala.concurrent.duration.FiniteDuration;
import scala.runtime.BoxedUnit;

import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.SetOnce;
import com.ikanow.aleph2.data_model.utils.UuidUtils;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage.BucketActionEventBusWrapper;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage.BucketActionOfferMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage.BucketActionHandlerMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage.BucketActionWillAcceptMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage.BucketActionIgnoredMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage.BucketActionCollectedRepliesMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage.BucketActionTimeoutMessage;
import com.ikanow.aleph2.management_db.services.ManagementDbActorContext;
import com.ikanow.aleph2.management_db.utils.ActorUtils;
import com.sun.istack.internal.logging.Logger;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.japi.pf.ReceiveBuilder;

/** This actor's role is to send out the received bucket update messages, to marshal the replies
 *  and to send out a combined set of replies to the sender
 * @author acp
 *
 */
public class BucketActionChooseActor extends AbstractActor {

	public static final Logger _logger = Logger.getLogger(BucketActionChooseActor.class);
		
	///////////////////////////////////////////
	
	// State
	
	protected class MutableState {
		public MutableState() {}		
		protected void reset() {
			reply_list.clear();
			data_import_manager_set.clear();
			targeted_actor = null;
			current_timeout_id = null;
		}
		protected final List<ActorRef> reply_list = new LinkedList<ActorRef>();
		protected final HashSet<String> data_import_manager_set = new HashSet<String>();
		protected final SetOnce<ActorRef> original_sender = new SetOnce<ActorRef>();
		protected final SetOnce<BucketActionMessage> original_message = new SetOnce<BucketActionMessage>();
		// (These are genuinely mutable, can change if the actor resets and tries a different target)
		protected ActorRef targeted_actor = null;
		protected String current_timeout_id = null;
		protected int tries = 0;
	}
	protected final MutableState _state = new MutableState();
	protected final FiniteDuration _timeout;	
	protected final ManagementDbActorContext _system_context;
	protected static final int MAX_TRIES = 3; // (this is a pretty low probability event) 
	
	///////////////////////////////////////////
	
	// Constructor
	
	/** Should only ever be called by the actor system, not by users
	 */
	public BucketActionChooseActor(final @NonNull Optional<FiniteDuration> timeout) {
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

	private PartialFunction<Object, BoxedUnit> _stateGettingCandidates = ReceiveBuilder
			.match(BucketActionWillAcceptMessage.class, 
				m -> {
					_state.data_import_manager_set.remove(m.source());
					_state.reply_list.add(this.sender());
					this.checkIfComplete();
				})
			.match(BucketActionIgnoredMessage.class, 
				m -> {
					_state.data_import_manager_set.remove(m.source());
					this.checkIfComplete();
				})
			.match(BucketActionTimeoutMessage.class, m -> m.uuid().equals(_state.current_timeout_id),
				m -> {
					this.pickAndSend();
				})				
			.build();
	
	private PartialFunction<Object, BoxedUnit> _stateAwaitingReply = ReceiveBuilder
			.match(BucketActionHandlerMessage.class, 
				m -> {
					// (note - overwrite the source field of the bean with the host)
					this.sendReplyAndClose(BeanTemplateUtils
							.clone(m.reply()).with(BasicMessageBean::source, m.source()).done());
				})
			.match(BucketActionIgnoredMessage.class, __ -> this.sender().equals(_state.targeted_actor),
				m -> {
					this.abortAndRetry(true);
				})
			.match(BucketActionTimeoutMessage.class, m -> m.uuid().equals(_state.current_timeout_id),
				m -> {
					this.abortAndRetry(true);
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
	
	protected void abortAndRetry(boolean allow_retries) {
		
		_logger.info("actor_id=" + this.self().toString()
				+ "; abort_tries=" + _state.tries + "; allow_retries=" + allow_retries 
				);
		
		if (allow_retries && (++_state.tries < MAX_TRIES)) {
			_state.reset();
			
			this.broadcastAction(_state.original_message.get());
			this.checkIfComplete();
		}
		else { // Just terminate with a "nothing to say" request
			_state.original_sender.get().tell(new BucketActionCollectedRepliesMessage(
					Arrays.asList(), _state.data_import_manager_set), 
					this.self());		
			this.context().stop(this.self());			
		}
	}
	 
	protected void pickAndSend() {
		if (!_state.reply_list.isEmpty()) {
			// Pick at random from the actors that replied
			final Random r = new Random();
			_state.targeted_actor = _state.reply_list.get(r.nextInt(_state.reply_list.size()));
			
			_logger.info("actor_id=" + this.self().toString()
					+ "; picking_actor=" + _state.targeted_actor 
					);
			
			// Forward the message on
			_state.targeted_actor.tell(_state.original_message, this.self());
			
			// Schedule a timeout
			_state.current_timeout_id = UuidUtils.get().getRandomUuid();
			_system_context.getActorSystem().scheduler().scheduleOnce(_timeout, 
						this.self(), new BucketActionTimeoutMessage(_state.current_timeout_id), 
						_system_context.getActorSystem().dispatcher(), null);
			
			this.context().become(_stateAwaitingReply);
		}
		else { // Must have timed out getting any replies, just terminate
			this.abortAndRetry(false);
		}
	}
	
	protected void broadcastAction(final @NonNull BucketActionMessage message) {
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
			
			//(log)
			_logger.info("message_id=" + message
					+ "; actor_id=" + this.self().toString()
					+ "; candidates_found=" + _state.data_import_manager_set.size());
			
			// 2) Then message all of the actors that we believe are present and wait for the response
			
			if (!_state.data_import_manager_set.isEmpty()) {
				
				_system_context.getBucketActionMessageBus().publish(new BucketActionEventBusWrapper
						(this.self(), new BucketActionOfferMessage(message.bucket())));
				
				_state.current_timeout_id = UuidUtils.get().getRandomUuid();
				_system_context.getActorSystem().scheduler().scheduleOnce(_timeout, 
							this.self(), new BucketActionTimeoutMessage(_state.current_timeout_id), 
							_system_context.getActorSystem().dispatcher(), null);
				
				this.context().become(_stateGettingCandidates);
			}
			//(else we're going to insta terminate anyway)			
		}
		catch (Exception e) {
			throw new RuntimeException();
		}
	}	
	protected void checkIfComplete() {
		if (_state.data_import_manager_set.isEmpty()) {
			this.pickAndSend();
		}
	}
	protected void sendReplyAndClose(final @NonNull BasicMessageBean reply) {
		_state.original_sender.get().tell(new BucketActionCollectedRepliesMessage(Arrays.asList(reply), Collections.emptySet()), 
				this.self());		
		this.context().stop(this.self());
	}

}
