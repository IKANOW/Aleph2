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
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.checkerframework.checker.nullness.qual.NonNull;

import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.Patterns;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage;
import com.ikanow.aleph2.management_db.services.ManagementDbActorContext;
import com.ikanow.aleph2.management_db.utils.ActorUtils;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.UntypedActor;

/** This actor's role is to send out the received bucket update messages, to marshal the replies
 *  and to send out a combined set of replies to the sender
 * @author acp
 *
 */
public class BucketActionDistributionActor extends UntypedActor {

	protected final ManagementDbActorContext _context;
	
	/** Should only ever be called by the actor system, not by users
	 */
	public BucketActionDistributionActor(final @NonNull Optional<FiniteDuration> timeout) {
		_timeout = timeout.orElse(Duration.create(5, TimeUnit.SECONDS)); // (Default timeout 5s) 
		_context = ManagementDbActorContext.get();
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
					.when(BucketActionReplyMessage.BucketActionTimeoutMessage.class, __ -> StateName.AWAITING_REPLIES == _state.getState(),
							__ -> {
								return this.sendReplyAndClose();
							})
					.when(BucketActionReplyMessage.BucketActionIgnoredMessage.class, __ -> StateName.AWAITING_REPLIES == _state.getState(),
							m -> {
								_state.data_import_manager_set.remove(m.uuid());
								return this.checkIfComplete();
							})
					.when(BucketActionReplyMessage.BucketActionHandlerMessage.class, __ -> StateName.AWAITING_REPLIES == _state.getState(),
							m -> {
								_state.reply_list.add(m.reply());
								_state.data_import_manager_set.remove(m.uuid());
								return this.checkIfComplete();
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
	protected StateName onNewBucketActionMessage(final @NonNull BucketActionMessage message) {
		try {
			_state.original_sender = this.getSender();
			
			// 1) Get a list of potential actors 
			
			// 1a) Check how many people are registered as listening from zookeeper/curator
			
			CuratorFramework curator = _context.getDistributedServices().getCuratorFramework();
			
			_state.data_import_manager_set.addAll(curator.getChildren().forPath(ActorUtils.BUCKET_ACTION_ACTOR));
			
			// 2) Then message all of the actors who replied that they were interested and wait for the response
			
			final ActorSelection bucket_action_actors = _context.getActorSystem().actorSelection(ActorUtils.BUCKET_ACTION_ACTOR);
			
			bucket_action_actors.tell(message, this.getSelf());
			
			_context.getActorSystem().scheduler().scheduleOnce(Duration.create(10, TimeUnit.SECONDS), 
						this.getSelf(), new BucketActionReplyMessage.BucketActionTimeoutMessage(), 
							_context.getActorSystem().dispatcher(), null);
			
			return StateName.AWAITING_REPLIES;
		}
		catch (Exception e) {
			throw new RuntimeException();
		}
	}	
	@NonNull
	protected StateName checkIfComplete() {
		if (_state.data_import_manager_set.isEmpty()) {
			return this.sendReplyAndClose();
		}
		else {
			return _state.getState();
		}
	}
	@NonNull
	protected StateName sendReplyAndClose() {
		_state.original_sender.tell(new BucketActionReplyMessage.BucketActionCollectedRepliesMessage(_state.reply_list, _state.data_import_manager_set.size()), 
									this.getSelf());		
		this.getContext().stop(this.self());
		
		return StateName.COMPLETE;
	}

	///////////////////////////////////////////
	
	// State
	
	public enum StateName { IDLE, AWAITING_REPLIES, COMPLETE } 
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
			reply_list = new LinkedList<BasicMessageBean>(); 
			data_import_manager_set = new HashSet<String>();
			state_name = StateName.IDLE;
		}		
		protected ActorRef original_sender = null;
		protected final List<BasicMessageBean> reply_list;
		protected final HashSet<String> data_import_manager_set;
	}
	final protected MutableState _state = new MutableState();
	final protected FiniteDuration _timeout;
	
}
