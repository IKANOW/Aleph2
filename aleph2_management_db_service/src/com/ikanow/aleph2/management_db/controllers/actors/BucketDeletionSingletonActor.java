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
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.SetOnce;
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

	protected final ManagementDbActorContext _actor_context;
	protected final IServiceContext _context;
	protected final IManagementDbService _core_management_db;
	protected final SetOnce<ICrudService<BucketDeletionMessage>> _bucket_deletion_queue = new SetOnce<>();
	protected final SetOnce<Cancellable> _ticker = new SetOnce<>();
	
	protected final LookupEventBus<BucketMgmtEventBusWrapper, ActorRef, String> _bucket_deletion_bus;
	
	/** Akka c'tor
	 */
	public BucketDeletionSingletonActor() {		
		_actor_context = ManagementDbActorContext.get();
		_bucket_deletion_bus = _actor_context.getDeletionMgmtBus();
		
		_context = _actor_context.getServiceContext();
		_core_management_db = Lambdas.get(() -> { try { return _context.getCoreManagementDbService(); } catch (Exception e) { return null; } });

		if (null != _core_management_db) {
			final FiniteDuration poll_delay = Duration.create(1, TimeUnit.SECONDS);
			final FiniteDuration poll_frequency = Duration.create(10, TimeUnit.SECONDS);
			_ticker.set(this.context().system().scheduler()
						.schedule(poll_delay, poll_frequency, this.self(), "Tick", this.context().system().dispatcher(), null));
			
			_logger.info("BucketDeletionSingletonActor has started on this node.");						
		}		
	}
	
	/** For some reason can run into guice problems with doing this in the c'tor
	 *  so do it here instead
	 */
	protected void setup() {
		if ((null == _core_management_db) || (_bucket_deletion_queue.isSet())) {
			return;
		}
		_bucket_deletion_queue.set(_core_management_db.getBucketDeletionQueue(BucketDeletionMessage.class));

		// ensure bucket deletion queue is optimized:
		_bucket_deletion_queue.get().optimizeQuery(Arrays.asList(BeanTemplateUtils.from(BucketDeletionMessage.class).field(BucketDeletionMessage::delete_on)));
		// (this optimization lets deletion messages be manipulated 
		_bucket_deletion_queue.get().optimizeQuery(
				Arrays.asList(
						BeanTemplateUtils.from(BucketDeletionMessage.class).field(BucketDeletionMessage::bucket)
						+ "." +
						BeanTemplateUtils.from(DataBucketBean.class).field(DataBucketBean::full_name)));
	}
	
	/* (non-Javadoc)
	 * @see akka.actor.UntypedActor#onReceive(java.lang.Object)
	 */
	@Override
	public void onReceive(Object message) throws Exception {
		setup();
		
		final ActorRef self = this.self();
		if (String.class.isAssignableFrom(message.getClass())) { // tick!
			
			final Date now = new Date();
			final QueryComponent<BucketDeletionMessage> recent_messages = 
					CrudUtils.allOf(BucketDeletionMessage.class).rangeBelow(BucketDeletionMessage::delete_on, now, false);

			CompletableFuture<ICrudService.Cursor<BucketDeletionMessage>> matches = _bucket_deletion_queue.get().getObjectsBySpec(recent_messages);
			
			matches.thenAccept(m -> {
				
				final List<BucketDeletionMessage> msgs = StreamSupport.stream(m.spliterator(), false).collect(Collectors.toList());
				if (!msgs.isEmpty()) {
					final QueryComponent<BucketDeletionMessage> recent_msg_ids = 
							CrudUtils.allOf(BucketDeletionMessage.class)
								.withAny(BucketDeletionMessage::_id, 
									msgs.stream().map(msg -> msg._id()).collect(Collectors.toList()));
					
					final UpdateComponent<BucketDeletionMessage> update_time = CrudUtils.update(BucketDeletionMessage.class)
							.set(BucketDeletionMessage::delete_on, new Date(now.getTime() + 3600000L))
							.increment(BucketDeletionMessage::deletion_attempts, 1)
							;
					
					// Update the buckets' times - will try again in an hour if the delete fails for any reason
					_bucket_deletion_queue.get().updateObjectsBySpec(recent_msg_ids, Optional.of(false), update_time);
					
					// Send out the buckets for proper deletion
					msgs.forEach(msg -> {
						_bucket_deletion_bus.publish(new BucketMgmtEventBusWrapper(self, msg));
					});
					_logger.info("Prepared for deletion num_buckets=" + msgs.size()); 
				}
			});
		}
		else if (BucketDeletionMessage.class.isAssignableFrom(message.getClass())) { // deletion was successful
			final BucketDeletionMessage msg = (BucketDeletionMessage)message;
			_logger.info("Confirmed deletion of bucket: " + msg.bucket().full_name());
			_bucket_deletion_queue.get().deleteObjectById(msg._id());
		}
	}
	
	/* (non-Javadoc)
	 * @see akka.actor.UntypedActor#postStop()
	 */
	@Override
	public void postStop() {
		if (_ticker.isSet()) {
			_ticker.get().cancel();
		}
		_logger.info("BucketDeletionSingletonActor has stopped on this node.");								
	}
}
