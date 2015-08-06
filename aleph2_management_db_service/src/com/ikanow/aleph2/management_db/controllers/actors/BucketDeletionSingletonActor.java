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






import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent;
import com.ikanow.aleph2.management_db.data_model.BucketMgmtMessage.BucketDeletionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketMgmtMessage.BucketMgmtEventBusWrapper;
import com.ikanow.aleph2.management_db.services.ManagementDbActorContext;






import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.UntypedActor;
import akka.event.japi.LookupEventBus;

/** This actor is a singleton, ie runs on only one node in the cluster
 *  its role is to monitor the test queue looking for tests that need to expire
 *  then send a message out to any harvesters that might be running them
 *  and then once confirmed stopped, place the test bucket on the delete queue
 *  (ie use Akka's scheduler to monitor pings every 1s if the test queue has anything in it, 10s otherwise?) 
 * @author cburch
 *
 */
public class BucketDeletionSingletonActor extends UntypedActor {
	private static final Logger _logger = LogManager.getLogger();	

	protected final Cancellable _ticker; //(just in case we want it later)
	
	protected final ManagementDbActorContext _actor_context;
	protected final IServiceContext _context;
	protected final IManagementDbService _core_management_db;
	protected final ICrudService<BucketDeletionMessage> _bucket_deletion_queue;
	
	protected final LookupEventBus<BucketMgmtEventBusWrapper, ActorRef, String> _bucket_deletion_bus;
	
	public BucketDeletionSingletonActor() {
		_logger.info("BucketDeletionSingletonActor has started on this node.");
		
		final FiniteDuration poll_frequency = Duration.create(10, TimeUnit.SECONDS);
		_ticker = this.context().system().scheduler()
			.schedule(poll_frequency, poll_frequency, this.self(), "Tick", this.context().system().dispatcher(), null);
		
		_actor_context = ManagementDbActorContext.get();
		_context = _actor_context.getServiceContext();
		_core_management_db = _context.getCoreManagementDbService();
		_bucket_deletion_queue = _core_management_db.getBucketDeletionQueue(BucketDeletionMessage.class);
		
		_bucket_deletion_bus = _actor_context.getDeletionMgmtBus();
		
		// ensure bucket deletion queue is optimized:
		_bucket_deletion_queue.optimizeQuery(Arrays.asList(BeanTemplateUtils.from(BucketDeletionMessage.class).field(BucketDeletionMessage::delete_on)));
	}
	
	/* (non-Javadoc)
	 * @see akka.actor.UntypedActor#onReceive(java.lang.Object)
	 */
	@Override
	public void onReceive(Object message) throws Exception {
		final ActorRef self = this.self();
		if (String.class.isAssignableFrom(message.getClass())) { // tick!
			
			final Date now = new Date();
			final QueryComponent<BucketDeletionMessage> recent_messages = 
					CrudUtils.allOf(BucketDeletionMessage.class).rangeAbove(BucketDeletionMessage::delete_on, now, false);

			CompletableFuture<ICrudService.Cursor<BucketDeletionMessage>> matches = _bucket_deletion_queue.getObjectsBySpec(recent_messages);
			
			matches.thenAccept(m -> {
				
				final List<BucketDeletionMessage> msgs = StreamSupport.stream(m.spliterator(), false).collect(Collectors.toList());
				if (!msgs.isEmpty()) {
					final QueryComponent<BucketDeletionMessage> recent_msg_ids = 
							CrudUtils.allOf(BucketDeletionMessage.class)
								.withAny(BucketDeletionMessage::_id, 
									msgs.stream().map(msg -> msg._id()).collect(Collectors.toList()));
					
					final UpdateComponent<BucketDeletionMessage> update_time = CrudUtils.update(BucketDeletionMessage.class)
							.set(BucketDeletionMessage::delete_on, new Date(now.getTime() + 3600L))
							.increment(BucketDeletionMessage::deletion_attempts, 1)
							;
					
					// Update the 
					_bucket_deletion_queue.updateObjectsBySpec(recent_msg_ids, Optional.of(false), update_time);
					
					// Send out the buckets for proper deletion
					msgs.forEach(msg -> {
						_bucket_deletion_bus.publish(new BucketMgmtEventBusWrapper(self, msg));
					});
				}
			});
		}
		else if (BucketDeletionMessage.class.isAssignableFrom(message.getClass())) { // deletion was successful
			final BucketDeletionMessage msg = (BucketDeletionMessage)message;
			_logger.info("Confirmed deletion of bucket: " + msg.bucket().full_name());
			_bucket_deletion_queue.deleteObjectById(msg._id());
		}
	}
}
