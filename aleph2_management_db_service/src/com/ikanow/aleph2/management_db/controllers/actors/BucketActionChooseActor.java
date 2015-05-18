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

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import org.apache.curator.framework.CuratorFramework;
import org.checkerframework.checker.nullness.qual.NonNull;

import scala.concurrent.duration.FiniteDuration;

import com.ikanow.aleph2.data_model.utils.Patterns;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage;
import com.ikanow.aleph2.management_db.services.ManagementDbActorContext;
import com.ikanow.aleph2.management_db.utils.ActorUtils;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;

//TODO (ALEPH-19): switch to AbstractActor if BucketActionDistributionAction testing goes well

/** This actor's role is to send out the received bucket update messages, to marshal the replies
 *  and to send out a combined set of replies to the sender
 * @author acp
 *
 */
public class BucketActionChooseActor extends UntypedActor {

	protected final ManagementDbActorContext _system_context;
	
	/** Should only ever be called by the actor system, not by users
	 */
	public BucketActionChooseActor(final @NonNull Optional<FiniteDuration> timeout) {
		_timeout = timeout.orElse(BucketActionSupervisor.DEFAULT_TIMEOUT); // (Default timeout 5s) 
		_system_context = ManagementDbActorContext.get();
	}
	
	/* (non-Javadoc)
	 * @see akka.actor.UntypedActor#onReceive(java.lang.Object)
	 */
	@Override
	public void onReceive(Object untyped_message) throws Exception {
		
		_state.updateState(
			Patterns.match(untyped_message).<StateName>andReturn()
					.when(BucketActionMessage.class, __ -> StateName.IDLE == _state.getState(), 
							m -> {
								return this.onNewBucketActionMessage(m);
							})
					.when(BucketActionReplyMessage.BucketActionWillAcceptMessage.class, __ -> StateName.GETTING_CANDIDATES == _state.getState(),
							m -> {
								_state.data_import_manager_set.remove(m.uuid());
								_state.reply_list.add(this.getSender());
								return this.checkIfComplete();
							})
					.when(BucketActionReplyMessage.BucketActionIgnoredMessage.class, __ -> StateName.GETTING_CANDIDATES == _state.getState(),
							m -> {
								_state.data_import_manager_set.remove(m.uuid());
								return this.checkIfComplete();
							})
					.when(BucketActionReplyMessage.BucketActionTimeoutMessage.class, __ -> StateName.GETTING_CANDIDATES == _state.getState(),
							__ -> {
								return this.pickAndSend();
							})
					.when(BucketActionReplyMessage.BucketActionIgnoredMessage.class, __ -> StateName.AWAITING_REPLY == _state.getState(),
							m -> {
								//TODO: something bad has happened, start all over again (up to 3 times adding node to discard pile, then fail?)
								return StateName.GETTING_CANDIDATES; //(or send error -> COMPLETE)
							})
					.when(BucketActionReplyMessage.BucketActionHandlerMessage.class, __ -> StateName.AWAITING_REPLY == _state.getState(),
							m -> {
								//TODO: format reply and send on
								return StateName.COMPLETE;
							})
					.otherwise(m -> {
						this.unhandled(m);
						return _state.getState();
					})
				);
	}
	
	///////////////////////////////////////////

	// Actions
	
	@NonNull
	protected StateName pickAndSend() {
		//TODO
		return StateName.AWAITING_REPLY;
	}
	
	@NonNull
	protected StateName onNewBucketActionMessage(final @NonNull BucketActionMessage message) {
		try {
			_state.original_sender = this.getSender();
			
			// 1) Get a list of potential actors 
			
			// 1a) Check how many people are registered as listening from zookeeper/curator
			
			CuratorFramework curator = _system_context.getDistributedServices().getCuratorFramework();
			
			_state.data_import_manager_set.addAll(curator.getChildren().forPath(ActorUtils.BUCKET_ACTION_ZOOKEEPER));
			
			// 2) Then message all of the actors who replied that they were interested and wait for the response
			
			_system_context.getBucketActionMessageBus().publish(new BucketActionMessage.BucketActionEventBusWrapper(this.self(), message));
			
			_system_context.getActorSystem().scheduler().scheduleOnce(_timeout, 
						this.getSelf(), new BucketActionReplyMessage.BucketActionTimeoutMessage(), 
						_system_context.getActorSystem().dispatcher(), null);
			
			return StateName.GETTING_CANDIDATES;
		}
		catch (Exception e) {
			throw new RuntimeException();
		}
	}	
	@NonNull
	protected StateName checkIfComplete() {
		if (_state.data_import_manager_set.isEmpty()) {
			return this.pickAndSend();
		}
		else {
			return _state.getState();
		}
	}
	@NonNull
	protected StateName sendReplyAndClose() {
		//TODO single message
		return StateName.COMPLETE;
	}

	///////////////////////////////////////////
	
	// State
	
	public enum StateName { IDLE, GETTING_CANDIDATES, AWAITING_REPLY, COMPLETE } 
	protected class MutableState {
		protected StateName state_name;
		
		public void updateState(StateName new_state) {
			state_name = new_state;
			if (StateName.COMPLETE == new_state) {
				getContext().stop(self());
			}
		}
		public StateName getState() {
			return state_name;
		}
		public MutableState() {
			reply_list = new LinkedList<ActorRef>(); 
			data_import_manager_set = new HashSet<String>();
			state_name = StateName.IDLE;
		}		
		protected ActorRef original_sender = null;
		protected final List<ActorRef> reply_list;
		protected final HashSet<String> data_import_manager_set;
	}
	final protected MutableState _state = new MutableState();
	final protected FiniteDuration _timeout;
	
}
