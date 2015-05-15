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
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;

import scala.concurrent.duration.Duration;

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
	
	protected static class MutableState {
		public boolean inProgress() {
			return original_sender != null;
		}
		public MutableState() {
			reply_list = new LinkedList<BasicMessageBean>(); 
			data_import_manager_set = new HashSet<String>();
		}
		
		protected ActorRef original_sender = null;
		protected final List<BasicMessageBean> reply_list;
		protected final HashSet<String> data_import_manager_set;
	}
	final MutableState _state = new MutableState();	
	
	/** Should only ever be called by the actor system, not by users
	 */
	public BucketActionDistributionActor() {
		_context = ManagementDbActorContext.get();
	}
	
	/* (non-Javadoc)
	 * @see akka.actor.UntypedActor#onReceive(java.lang.Object)
	 */
	@Override
	public void onReceive(Object untyped_message) throws Exception {
		
		if (untyped_message instanceof BucketActionMessage) {

			// Internal error I think? One of these should be created for every message
			if (_state.inProgress()) {
				unhandled(untyped_message);
				return;
			}
			
			// New action needs to be distributed:
			BucketActionMessage message = (BucketActionMessage)untyped_message;			
			
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
		}
		else if (untyped_message instanceof BucketActionReplyMessage) {
			// Handle replies from either the system (timeout) or the actors (ignored/handled - not handled includes errors)
			
			Patterns.match(untyped_message).andAct()
				.when(BucketActionReplyMessage.BucketActionTimeoutMessage.class,
						__ -> {
							this.sendReplyAndClose();
						})
				.when(BucketActionReplyMessage.BucketActionIgnoredMessage.class,
						m -> {
							_state.data_import_manager_set.remove(m.uuid());
							this.checkIfComplete();
						})
				.when(BucketActionReplyMessage.BucketActionHandlerMessage.class,
						m -> {
							_state.reply_list.add(m.reply());
							_state.data_import_manager_set.remove(m.uuid());
							this.checkIfComplete();
						})
				.otherwise(m -> this.unhandled(m));
		}
		else { // unhandled
			this.unhandled(untyped_message);
		}
	}
	protected void checkIfComplete() {
		if (_state.data_import_manager_set.isEmpty()) {
			this.sendReplyAndClose();
		}
	}
	protected void sendReplyAndClose() {
		_state.original_sender.tell(new BucketActionReplyMessage.BucketActionCollectedRepliesMessage(_state.reply_list), this.getSelf());		
		this.getContext().stop(this.self());
	}

}
