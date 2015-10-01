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
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent;
import com.ikanow.aleph2.data_model.utils.SetOnce;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage.UpdateBucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketMgmtMessage.BucketTimeoutMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage.BucketActionCollectedRepliesMessage;
import com.ikanow.aleph2.management_db.services.ManagementDbActorContext;

import fj.Unit;
import akka.actor.Cancellable;
import akka.actor.UntypedActor;

/** This actor is a singleton, ie runs on only one node in the cluster
 *  its role is to monitor the test queue looking for tests that need to expire
 *  then send a message out to any harvesters that might be running them
 *  (ie use Akka's scheduler to monitor pings every 1s if the test queue has anything in it, 10s otherwise?) 
 * @author cburch
 *
 */
public class BucketTestCycleSingletonActor extends UntypedActor {
	private static final Logger _logger = LogManager.getLogger();
	protected final ManagementDbActorContext _system_context;
	protected final IManagementDbService _underlying_management_db;
	protected final SetOnce<ICrudService<BucketTimeoutMessage>> _bucket_test_queue = new SetOnce<>();
	protected final SetOnce<Cancellable> _ticker = new SetOnce<>();
	
	public BucketTestCycleSingletonActor() {
		_system_context = ManagementDbActorContext.get();
		_underlying_management_db = _system_context.getServiceContext().getService(IManagementDbService.class, Optional.empty()).orElse(null);		
		if (null != _underlying_management_db) {
			final FiniteDuration poll_delay = Duration.create(1, TimeUnit.SECONDS);
			final FiniteDuration poll_frequency = Duration.create(1, TimeUnit.SECONDS);
			_ticker.set(this.context().system().scheduler()
					.schedule(poll_delay, poll_frequency, this.self(), "Tick", this.context().system().dispatcher(), null));			
			_logger.info("BucketTestCycleSingletonActor has started on this node.");						
		}	
		
	}
	
	/** For some reason can run into guice problems with doing this in the c'tor
	 *  so do it here instead
	 */
	protected void setup() {
		if ( (_bucket_test_queue.isSet())) {
			return;
		}
		_bucket_test_queue.set(_underlying_management_db.getBucketTestQueue(BucketTimeoutMessage.class));

		// ensure bucket test queue is optimized:
		_bucket_test_queue.get().optimizeQuery(Arrays.asList(BeanTemplateUtils.from(BucketTimeoutMessage.class).field(BucketTimeoutMessage::timeout_on)));
	}

	/* (non-Javadoc)
	 * @see akka.actor.UntypedActor#onReceive(java.lang.Object)
	 */
	@Override
	public void onReceive(Object message) throws Exception {
		//Note: we don't bother checking what the message is, we always assume it means we should check
		//i.e. we only think it's going to be the string "Tick" sent from the scheduler we setup in the c'tor
		setup();
		final Date now = new Date();
		final QueryComponent<BucketTimeoutMessage> recent_messages = 
				CrudUtils.allOf(BucketTimeoutMessage.class).rangeBelow(BucketTimeoutMessage::timeout_on, now, false);
		
		CompletableFuture<ICrudService.Cursor<BucketTimeoutMessage>> matches = _bucket_test_queue.get().getObjectsBySpec(recent_messages);
		
		matches.thenAccept(m -> {
			final List<BucketTimeoutMessage> msgs = StreamSupport.stream(m.spliterator(), false).collect(Collectors.toList());
			if (!msgs.isEmpty()) {
				//loop over each expired test item
				msgs.stream().forEach(msg -> {
					_logger.info("TestCycleActor found an expired test item, sending stop message");
					
					//(push 5 minutes away - will try to clean up later if anything goes wrong for any reason, but means next few iterations of this thread
					// will work)
					final UpdateComponent<BucketTimeoutMessage> update = CrudUtils.update(BucketTimeoutMessage.class)
							.set(BucketTimeoutMessage::timeout_on, new Date(now.getTime() + 300L*1000L));
					
					final CompletableFuture<Boolean> update_message = _bucket_test_queue.get().updateObjectById(msg._id(), update);
					
					_logger.debug("Update message timeout: " + update.getAll());												
					
					//delete item from the queue after stop message returns successfully
					update_message.thenCompose(__ -> {
						
						//send stop message
						final UpdateBucketActionMessage stop_message = 
								new UpdateBucketActionMessage(msg.bucket(), false, msg.bucket(), msg.handling_clients());
						
						// (note this deliberately calls askDistributionActor (vs askBucketActionActor) because we don't know which node it's running on, so we'll send to all of them)
						final CompletableFuture<BucketActionCollectedRepliesMessage> stop_future = BucketActionSupervisor.askDistributionActor(
								_system_context.getBucketActionSupervisor(), 
								_system_context.getActorSystem(), 
								stop_message, 
								Optional.empty());
						_logger.debug("Sent stop message for bucket: " + msg.bucket().full_name());												
					
						return stop_future;
					})
					.thenApply(reply -> {	
						//stop completed successfully
						_bucket_test_queue.get().deleteObjectById(msg._id())
							.thenApply(d -> { 
								//item deleted successfully
								_logger.debug("deleted test queue item successfully");
								return Unit.unit();
							}).exceptionally(t-> {
								//failed to delete item
								_logger.error("Error removing test item: " + msg.bucket().full_name() + " from test queue", t);								
								return Unit.unit();
							});
						
						return Unit.unit();
					}).exceptionally(t -> {
						//stop failed - will try again in 5 minutes
						_logger.error("Error stopping job: " + msg.bucket().full_name(), t);
						
						return Unit.unit();
					});
				}); //end for each			
			}
		}).exceptionally(t -> {
			return null;
		});		
	}
	
	/* (non-Javadoc)
	 * @see akka.actor.UntypedActor#postStop()
	 */
	@Override
	public void postStop() {
		if ( _ticker.isSet()) {
			_ticker.get().cancel();
		}
		_logger.info("BucketDeletionSingletonActor has stopped on this node.");								
	}
}
