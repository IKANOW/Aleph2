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
import java.util.List;
import java.util.Optional;
import java.util.Set;
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
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.Cursor;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.SetOnce;
import com.ikanow.aleph2.data_model.utils.TimeUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage.PollFreqBucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage.BucketActionCollectedRepliesMessage;
import com.ikanow.aleph2.management_db.services.ManagementDbActorContext;

import akka.actor.Cancellable;
import akka.actor.UntypedActor;

/** This actor is a singleton, ie runs on only one node in the cluster
 *  its role is to monitor the bucket db looking for buckets that have a next_date < now
 *  then send a message out to any harvesters to do an onPollFrequency call
 * @author cburch
 *
 */
public class BucketPollFreqSingletonActor extends UntypedActor {
	private static final Logger _logger = LogManager.getLogger();
	protected final ManagementDbActorContext _actor_context;
	protected final IServiceContext _context;
	protected final IManagementDbService _core_mgmt_db;
	protected final ManagementDbActorContext _system_context;
	protected final IManagementDbService _underlying_management_db;	
	protected final SetOnce<ICrudService<DataBucketBean>> _bucket_crud = new SetOnce<>();
	protected final SetOnce<ICrudService<DataBucketStatusBean>> _bucket_status_crud = new SetOnce<>();
	protected final SetOnce<Cancellable> _ticker = new SetOnce<>();
	
	public BucketPollFreqSingletonActor() {
		_system_context = ManagementDbActorContext.get();
		_actor_context = ManagementDbActorContext.get();
		_context = _actor_context.getServiceContext();
		_core_mgmt_db = _context.getCoreManagementDbService();
		_underlying_management_db = _system_context.getServiceContext().getService(IManagementDbService.class, Optional.empty()).orElse(null);		
		if (null != _underlying_management_db) {
			final FiniteDuration poll_delay = Duration.create(1, TimeUnit.SECONDS);
			final FiniteDuration poll_frequency = Duration.create(1, TimeUnit.SECONDS);
			_ticker.set(this.context().system().scheduler()
					.schedule(poll_delay, poll_frequency, this.self(), "Tick", this.context().system().dispatcher(), null));					
			_logger.info("BucketPollSingletonActor has started on this node.");						
		}			
	}
	
	/** For some reason can run into guice problems with doing this in the c'tor
	 *  so do it here instead
	 */
	protected void setup() {
		//TODO get the bucket db and optimize it's query path
		if (!_bucket_crud.isSet()) { // (for some reason, core_mdb.anything() can fail in the c'tor)			
			_bucket_crud.set(_underlying_management_db.getDataBucketStore());
			_bucket_status_crud.set(_underlying_management_db.getDataBucketStatusStore());
		}
	}

	/* (non-Javadoc)
	 * @see akka.actor.UntypedActor#onReceive(java.lang.Object)
	 */
	@Override
	public void onReceive(Object message) throws Exception {
		//Note: we don't bother checking what the message is, we always assume it means we should check
		//i.e. we only think it's going to be the string "Tick" sent from the scheduler we setup in the c'tor
		setup();
		getExpiredBuckets().thenAccept(m -> {
			final List<DataBucketBean> buckets = StreamSupport.stream(m.spliterator(), false).collect(Collectors.toList());			
			buckets.stream().forEach(bucket -> {
				_logger.debug("Poll Expired for bucket: " + bucket.full_name());				
				updateBucketNextPollTime(bucket).thenAccept(x -> {
					_logger.debug("Sending poll message");
					sendPollMessage(bucket);
				}).exceptionally(t-> {
					_logger.error("Error updating bucket next poll time: " + t.getMessage());
					return null;
				});			
			});
		}).exceptionally(t -> {
			_logger.error("Error retrieving expired buckets", t);
			return null;
		});		
	}

	private CompletableFuture<BucketActionCollectedRepliesMessage> sendPollMessage(final DataBucketBean bucket) {		
		final CompletableFuture<BucketActionCollectedRepliesMessage> poll_future = BucketActionSupervisor.askBucketActionActor(Optional.empty(),
				_actor_context.getBucketActionSupervisor(), 
				_actor_context.getActorSystem(), 
				new PollFreqBucketActionMessage(bucket), 
				Optional.empty());			
		_logger.debug("Sent poll message from actor for bucket: " + bucket.full_name());
		return poll_future;
	}

	private CompletableFuture<Cursor<DataBucketBean>> getExpiredBuckets() {
		//they are buckets where suspended:false AND next_poll_date < now
		final QueryComponent<DataBucketStatusBean> non_suspended_expired_buckets = CrudUtils.allOf(DataBucketStatusBean.class).when(DataBucketStatusBean::suspended, false).rangeBelow(DataBucketStatusBean::next_poll_date, new Date(), false);
		//first get the status items where suspended:false and next_poll_date < now
		return _bucket_status_crud.get().getObjectsBySpec(non_suspended_expired_buckets).thenCompose(c -> {
			//match those to databuckets with the ids
			Set<String> bucket_ids = Optionals.streamOf(c.iterator(), false).map(b->b._id()).collect(Collectors.toSet());			
			final QueryComponent<DataBucketBean> matching_ids = CrudUtils.allOf(DataBucketBean.class).withAny(DataBucketBean::_id, bucket_ids);			
			return _bucket_crud.get().getObjectsBySpec(matching_ids);
		});
	}
	
	private CompletableFuture<Boolean> updateBucketNextPollTime(DataBucketBean bucket)  {
		//update this buckets with a new next_date		
		//TODO handle validation failure (shouldn't happen if poll date was already set? (i.e. instead of calling .success())
		final Date next_poll_date = TimeUtils.getSchedule(bucket.poll_frequency(), Optional.of(new Date())).success();
		_logger.debug("Setting next poll time to: " + next_poll_date.toString());
		final QueryComponent<DataBucketStatusBean> expired_bucket_status = 
				CrudUtils.allOf(DataBucketStatusBean.class).when(DataBucketStatusBean::_id, bucket._id());
		final UpdateComponent<DataBucketStatusBean> update = CrudUtils.update(DataBucketStatusBean.class)
				.set(DataBucketStatusBean::next_poll_date, next_poll_date);
		return _bucket_status_crud.get().updateObjectBySpec(expired_bucket_status, Optional.of(false), update);
	}

	/* (non-Javadoc)
	 * @see akka.actor.UntypedActor#postStop()
	 */
	@Override
	public void postStop() {
		if ( _ticker.isSet()) {
			_ticker.get().cancel();
		}
		_logger.info("BucketPollSingletonActor has stopped on this node.");								
	}
}
