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

import scala.PartialFunction;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;
import scala.runtime.BoxedUnit;

import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage;
import com.ikanow.aleph2.management_db.services.ManagementDbActorContext;
import com.ikanow.aleph2.management_db.utils.ActorUtils;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.japi.pf.ReceiveBuilder;

/** This actor's role is to send out the received bucket update messages, to marshal the replies
 *  and to send out a combined set of replies to the sender
 * @author acp
 *
 */
public class BucketActionDistributionActor extends AbstractActor {

	///////////////////////////////////////////
	
	// State
	
	protected class MutableState {
		
		public MutableState() {
			reply_list = new LinkedList<BasicMessageBean>(); 
			data_import_manager_set = new HashSet<String>();
		}		
		protected ActorRef original_sender = null;
		protected final List<BasicMessageBean> reply_list;
		protected final HashSet<String> data_import_manager_set;
	}
	final protected MutableState _state = new MutableState();
	final protected FiniteDuration _timeout;
	
	protected final ManagementDbActorContext _system_context;
	
	///////////////////////////////////////////
	
	// Constructor
	
	/** Should only ever be called by the actor system, not by users
	 */
	public BucketActionDistributionActor(final @NonNull Optional<FiniteDuration> timeout) {
		_timeout = timeout.orElse(Duration.create(10, TimeUnit.SECONDS)); // (Default timeout 5s) 
		_system_context = ManagementDbActorContext.get();
	}
	
	///////////////////////////////////////////
	
	// State Transitions
	
	private PartialFunction<Object, BoxedUnit> _stateIdle = ReceiveBuilder
			.match(BucketActionMessage.class, 
				m -> {
					this.broadcastAction(m);
				})
			.build();
			
	private PartialFunction<Object, BoxedUnit> _stateAwaitingReplies = ReceiveBuilder
			.match(BucketActionReplyMessage.BucketActionHandlerMessage.class, 
				m -> {
					_state.reply_list.add(m.reply());
					_state.data_import_manager_set.remove(m.uuid());
					this.checkIfComplete();
				})
			.match(BucketActionReplyMessage.BucketActionIgnoredMessage.class, 
				m -> {
					_state.data_import_manager_set.remove(m.uuid());
					this.checkIfComplete();
				})
			.match(BucketActionReplyMessage.BucketActionTimeoutMessage.class, 
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
	
	protected void broadcastAction(final @NonNull BucketActionMessage message) {
		try {
			_state.original_sender = this.sender();
			
			// 1) Get a list of potential actors 
			
			// 1a) Check how many people are registered as listening from zookeeper/curator
			
			CuratorFramework curator = _system_context.getDistributedServices().getCuratorFramework();
			
			_state.data_import_manager_set.addAll(curator.getChildren().forPath(ActorUtils.BUCKET_ACTION_ACTOR));
			
			// 2) Then message all of the actors who replied that they were interested and wait for the response
			
			final ActorSelection bucket_action_actors = _system_context.getActorSystem().actorSelection(ActorUtils.BUCKET_ACTION_ACTOR);
			
			bucket_action_actors.tell(message, this.self());
			
			// 2b) Schedule a timeout
			
			_system_context.getActorSystem().scheduler().scheduleOnce(Duration.create(10, TimeUnit.SECONDS), 
						this.self(), new BucketActionReplyMessage.BucketActionTimeoutMessage(), 
							_system_context.getActorSystem().dispatcher(), null);

			// 3) Transition state
			
			context().become(_stateAwaitingReplies);
		}
		catch (Exception e) {
			throw new RuntimeException();
		}
	}	
	protected void checkIfComplete() {
		if (_state.data_import_manager_set.isEmpty()) {
			this.sendReplyAndClose();
		}
	}
	protected void sendReplyAndClose() {
		_state.original_sender.tell(new BucketActionReplyMessage.BucketActionCollectedRepliesMessage(_state.reply_list, _state.data_import_manager_set.size()), 
									this.self());		
		this.getContext().stop(this.self());
	}
}
