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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException.NoNodeException;

import scala.PartialFunction;
import scala.Tuple2;
import scala.concurrent.duration.FiniteDuration;
import scala.runtime.BoxedUnit;

import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.SetOnce;
import com.ikanow.aleph2.data_model.utils.Tuples;
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

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.japi.pf.ReceiveBuilder;

/** This actor's role is to send out the received bucket update messages, to marshal the replies
 *  and to send out a combined set of replies to the sender
 * @author acp
 *
 */
public class BucketActionChooseActor extends AbstractActor {

	private static final Logger _logger = LogManager.getLogger();	
	
	///////////////////////////////////////////
	
	// State
	
	protected class MutableState {
		public MutableState() {}		
		protected void reset() {
			reply_list.clear();
			data_import_manager_set.clear();
			targeted_source = null;
			current_timeout_id = null;
		}
		protected final List<Tuple2<String, ActorRef>> reply_list = new LinkedList<Tuple2<String, ActorRef>>();
		protected final HashSet<String> data_import_manager_set = new HashSet<String>();
		protected final SetOnce<ActorRef> original_sender = new SetOnce<ActorRef>();
		protected final SetOnce<BucketActionMessage> original_message = new SetOnce<BucketActionMessage>();
		// (These are genuinely mutable, can change if the actor resets and tries a different target)
		protected Tuple2<String, ActorRef> targeted_source; 
		protected String current_timeout_id = null;
		protected int tries = 0;
		protected HashSet<String> blacklist = new HashSet<String>();
	}
	protected final MutableState _state = new MutableState();
	protected final FiniteDuration _timeout;	
	protected final ManagementDbActorContext _system_context;
	protected static final int MAX_TRIES = 3; // (this is a pretty low probability event)
	protected final String _zookeeper_path; // the zk path on which interested nodes are listening - currently ActorUtils .BUCKET_ACTION_ZOOKEEPER or .STREAMING_ENRICHMENT_ZOOKEEPER 
	
	///////////////////////////////////////////
	
	// Constructor
	
	/** Should only ever be called by the actor system, not by users
	 * @param timeout - the timeout for the request
	 * @param zookeeper_path - currently either BUCKET_ACTION_ZOOKEEPER (talk to harvester) or STREAMING_ENRICHMENT_ZOOKEEPER (talk to streaming (storm) enrichment)
	 */
	public BucketActionChooseActor(final Optional<FiniteDuration> timeout, final String zookeeper_path) {
		_zookeeper_path = zookeeper_path;
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
					_state.reply_list.add(Tuples._2T(m.source(), this.sender()));
					this.checkIfComplete();
				})
			.match(BucketActionIgnoredMessage.class, 
				m -> {
					_state.data_import_manager_set.remove(m.source());
					this.checkIfComplete();
				})
			.match(BucketActionHandlerMessage.class, // Can receive this if the call errors, just treat it like an ignored
				m -> {
					_state.data_import_manager_set.remove(m.source());
					this.checkIfComplete();
				})
			.match(BucketActionTimeoutMessage.class, m -> m.source().equals(_state.current_timeout_id),
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
			.match(BucketActionIgnoredMessage.class, __ -> this.sender().equals(_state.targeted_source._2()),
				m -> {
					this.abortAndRetry(true);
				})
			.match(BucketActionTimeoutMessage.class, m -> m.source().equals(_state.current_timeout_id),
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
		
		_logger.info("bucket=" + _state.original_message.get().bucket().full_name()
				+ "; actor_id=" + this.self().toString()
				+ "; abort_tries=" + _state.tries + "; allow_retries=" + allow_retries 
				);
		
		if (allow_retries && (++_state.tries < MAX_TRIES)) {
			_state.blacklist.add(_state.targeted_source._1());
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
			_state.targeted_source = _state.reply_list.get(r.nextInt(_state.reply_list.size()));
			
			_logger.info("bucket=" + _state.original_message.get().bucket().full_name()
					+ "; actor_id=" + this.self().toString()
					+ "; picking_actor=" + _state.targeted_source._2() 
					+ "; picking_source=" + _state.targeted_source._1() 
					);
			
			// Forward the message on
			_state.targeted_source._2().tell(_state.original_message.get(), this.self());
			
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
	
	protected void broadcastAction(final BucketActionMessage message) {
		try {
			_state.original_sender.set(this.sender());
			_state.original_message.set(message);
			
			// 1) Get a list of potential actors 
			
			// 1a) Check how many people are registered as listening from zookeeper/curator
			
			CuratorFramework curator = _system_context.getDistributedServices().getCuratorFramework();
			
			try {
				_state.data_import_manager_set.addAll(curator.getChildren().forPath(_zookeeper_path));
				
			}
			catch (NoNodeException e) { 
				// This is OK
				_logger.info("bucket=" + _state.original_message.get().bucket().full_name() 
						+ " ;actor_id=" + this.self().toString() + "; zk_path_not_found=" + _zookeeper_path);
			}
			
			// Remove any blacklisted nodes:
			_state.data_import_manager_set.removeAll(_state.blacklist);
			
			//(log)
			_logger.info("bucket=" + _state.original_message.get().bucket().full_name()
					+ "; message_id=" + message
					+ "; actor_id=" + this.self().toString()
					+ "; candidates_found=" + _state.data_import_manager_set.size()
					+ "; blacklisted=" + _state.blacklist.size()
					);
			
			// 2) Then message all of the actors that we believe are present and wait for the response
			
			if (!_state.data_import_manager_set.isEmpty()) {
				
				_system_context.getMessageBus(_zookeeper_path).publish(new BucketActionEventBusWrapper
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
	protected void sendReplyAndClose(final BasicMessageBean reply) {
		_state.original_sender.get().tell(new BucketActionCollectedRepliesMessage(Arrays.asList(reply), Collections.emptySet()), 
				this.self());		
		this.context().stop(this.self());
	}

}
