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
package com.ikanow.aleph2.data_import_manager.analytics.actors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.junit.Before;
import org.junit.Test;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.ikanow.aleph2.data_import_manager.analytics.actors.AnalyticsTriggerWorkerActor.TickSpacingService;
import com.ikanow.aleph2.data_import_manager.analytics.utils.TestAnalyticTriggerCrudUtils;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean.AnalyticThreadComplexTriggerBean.TriggerType;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.shared.ProcessingTestSpecBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.BucketUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent;
import com.ikanow.aleph2.data_model.utils.UuidUtils;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerMessage;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerStateBean;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage.BucketActionAnalyticJobMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage.BucketActionAnalyticJobMessage.JobMessageType;
import com.ikanow.aleph2.management_db.services.ManagementDbActorContext;
import com.ikanow.aleph2.management_db.utils.ActorUtils;

public class TestAnalyticsTriggerWorkerActor extends TestAnalyticsTriggerWorkerCommon {
	private static final Logger _logger = LogManager.getLogger();	

	///////////////////////////////////////////////////////////////////////////////////////
	
	// SETUP
	
	// This one always accepts, but then refuses when it comes down to it...
	public static class TestActor extends UntypedActor {
		public TestActor() {
			_logger.info("started TestActor");
		}
		@Override
		public void onReceive(Object arg0) throws Exception {
			if (arg0 instanceof BucketActionMessage.BucketActionOfferMessage) {
				_logger.info("Accept OFFER");
				this.sender().tell(new BucketActionReplyMessage.BucketActionWillAcceptMessage("dummy_data_bucket_change_actor"), this.self());
			}
			else {
				_logger.info("Received message! " + arg0.getClass().toString());
				if (arg0 instanceof BucketActionAnalyticJobMessage) {
					final BucketActionAnalyticJobMessage msg = (BucketActionAnalyticJobMessage) arg0;
					_logger.info("Message details: " + msg.type() + " jobs : " + Optionals.ofNullable(msg.jobs()).stream().map(j -> j.name()).collect(Collectors.joining(";")));
					
					final DataBucketBean bucket = msg.bucket();
					if (null != bucket.harvest_configs()) {
						_num_received_errors.incrementAndGet();						
					}
					if (null != bucket.multi_node_enabled()) {
						if (bucket.multi_node_enabled()) {
							_num_received_errors.incrementAndGet();
						}
					}
				}				
				_num_received.incrementAndGet();
			}
		}		
	};
	
	protected static ActorRef _trigger_worker = null;
	protected static ActorRef _dummy_data_bucket_change_actor = null;	
	protected static ICrudService<AnalyticTriggerStateBean> _test_crud;
	protected static ICrudService<DataBucketStatusBean> _status_crud;

	@Before
	@Override
	public void test_Setup() throws Exception {
		_logger.info("running child test_Setup: " + (null == _trigger_worker));
		super.test_Setup();
		if (null != _trigger_worker) {
			return;
		}
		_num_received_errors.set(0L); //(do this for every test)
				
		_trigger_worker = _actor_context.getActorSystem().actorOf(
				Props.create(com.ikanow.aleph2.data_import_manager.analytics.actors.AnalyticsTriggerWorkerActor.class),
				UuidUtils.get().getRandomUuid()
				//hostname + ActorNameUtils.ANALYTICS_TRIGGER_WORKER_SUFFIX
				);

		_test_crud = _service_context.getService(IManagementDbService.class, Optional.empty()).get().getAnalyticBucketTriggerState(AnalyticTriggerStateBean.class);
		_test_crud.deleteDatastore().get();
		
		_status_crud = _service_context.getService(IManagementDbService.class, Optional.empty()).get().getDataBucketStatusStore();
		_status_crud.deleteDatastore().get();
		
		final String dummy_data_bucket_change_actor = UuidUtils.get().getRandomUuid();
		_dummy_data_bucket_change_actor = ManagementDbActorContext.get().getActorSystem().actorOf(Props.create(TestActor.class), dummy_data_bucket_change_actor);
		ManagementDbActorContext.get().getAnalyticsMessageBus().subscribe(_dummy_data_bucket_change_actor, ActorUtils.BUCKET_ANALYTICS_ZOOKEEPER);
		ManagementDbActorContext.get().getDistributedServices()
			.getCuratorFramework().create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
			.forPath(ActorUtils.BUCKET_ANALYTICS_ZOOKEEPER + "/" +  "dummy_data_bucket_change_actor");				
	}
	
	///////////////////////////////////////////////////////////////////////////////////////

	// MISC TESTING
	
	@Test
	public void test_workerTickSeparation() throws InterruptedException {
		TickSpacingService test = new TickSpacingService();
		
		assertEquals(true, test.grabMutex());
		assertEquals(false, test.grabMutex());
		test.releaseMutex();
		assertEquals(true, test.grabMutex());
		assertEquals(false, test.grabMutex());
		test.releaseMutex();
	}
	
	///////////////////////////////////////////////////////////////////////////////////////
	
	// BASIC TRIGGER LIFECYCLE
	
	@Test
	public void test_bucketLifecycle() throws InterruptedException {
		System.out.println("Starting test_bucketLifecycle");
		
		// Going to create a bucket, update it, and then suspend it
		// Note not much validation here, since the actual triggers etc are created by the utils functions, which are tested separately
		
		// Create the usual "manual trigger" bucket
		
		final DataBucketBean manual_trigger_bucket = TestAnalyticTriggerCrudUtils.buildBucket("/test/trigger", true);

		insertStatusForBucket(manual_trigger_bucket);
		
		final ICrudService<AnalyticTriggerStateBean> trigger_crud = 
				_service_context.getCoreManagementDbService().readOnlyVersion().getAnalyticBucketTriggerState(AnalyticTriggerStateBean.class);		
		trigger_crud.deleteDatastore().join();
						
		// 1) Send a message to the worker to fill in that bucket
		{
			final BucketActionMessage.NewBucketActionMessage msg = 
					new BucketActionMessage.NewBucketActionMessage(manual_trigger_bucket, false);
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish
			waitForData(() -> trigger_crud.countObjects().join(), 7, true);
			//(status)
			waitForData(() -> getLastRunTime(manual_trigger_bucket.full_name(), Optional.empty(), true), -1L, true);
			waitForData(() -> getLastRunTime(manual_trigger_bucket.full_name(), Optional.empty(), false), -1L, true);
			
			// Check the DB
		
			assertEquals(7L, trigger_crud.countObjects().join().intValue());
			// Check they aren't suspended:
			assertEquals(7L, trigger_crud.countObjectsBySpec(
					CrudUtils.allOf(AnalyticTriggerStateBean.class)
						.when(AnalyticTriggerStateBean::is_bucket_suspended, false)
					).join().intValue());
			
			// underlying status
			assertEquals(-1L, getLastRunTime(manual_trigger_bucket.full_name(), Optional.empty(), true).longValue());
			assertEquals(-1L, getLastRunTime(manual_trigger_bucket.full_name(), Optional.empty(), false).longValue());
		}
		
		// 2) Update in suspended mode
		{
			final BucketActionMessage.UpdateBucketActionMessage msg = 
					new BucketActionMessage.UpdateBucketActionMessage(manual_trigger_bucket, false, manual_trigger_bucket, new HashSet<String>());
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish			
			waitForData(() -> trigger_crud.countObjectsBySpec(
												CrudUtils.allOf(AnalyticTriggerStateBean.class)
												.when(AnalyticTriggerStateBean::is_bucket_suspended, true)
											).join(), 7, true);
			
			// Check the DB
		
			assertEquals(7L, trigger_crud.countObjects().join().intValue());
			// Check they are suspended:
			assertEquals(7L, trigger_crud.countObjectsBySpec(
					CrudUtils.allOf(AnalyticTriggerStateBean.class)
						.when(AnalyticTriggerStateBean::is_bucket_suspended, true)
					).join().intValue());
			
			// underlying status
			assertEquals(-1L, getLastRunTime(manual_trigger_bucket.full_name(), Optional.empty(), true).longValue());
			assertEquals(-1L, getLastRunTime(manual_trigger_bucket.full_name(), Optional.empty(), false).longValue());
		}
		
		// 3) Delete
		{
			final BucketActionMessage.DeleteBucketActionMessage msg = 
					new BucketActionMessage.DeleteBucketActionMessage(manual_trigger_bucket, new HashSet<String>());
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish			
			waitForData(() -> trigger_crud.countObjects().join(), 0, false);
			
			// Check the DB
		
			assertEquals(0L, trigger_crud.countObjects().join().intValue());
			
			// underlying status
			assertEquals(-1L, getLastRunTime(manual_trigger_bucket.full_name(), Optional.empty(), true).longValue());
			assertEquals(-1L, getLastRunTime(manual_trigger_bucket.full_name(), Optional.empty(), false).longValue());
		}
		// Check no malformed buckets
		assertEquals(0L, _num_received_errors.get());
	}
	
	@Test
	public void test_bucketTestLifecycle() throws InterruptedException {
		System.out.println("Starting test_bucketTestLifecycle");
		
		// Going to create a bucket, update it, and then suspend it
		// Note not much validation here, since the actual triggers etc are created by the utils functions, which are tested separately
		
		// Create the usual "manual trigger" bucket
		
		final DataBucketBean manual_trigger_bucket_original = TestAnalyticTriggerCrudUtils.buildBucket("/test/trigger", true);
		
		final DataBucketBean manual_trigger_bucket = BucketUtils.convertDataBucketBeanToTest(manual_trigger_bucket_original, "alex"); 
		
		insertStatusForBucket(manual_trigger_bucket);
		
		final ICrudService<AnalyticTriggerStateBean> trigger_crud = 
				_service_context.getCoreManagementDbService().readOnlyVersion().getAnalyticBucketTriggerState(AnalyticTriggerStateBean.class);
		trigger_crud.deleteDatastore().join();

		// 1) Send a message to the worker to fill in that bucket
		{
			final BucketActionMessage.TestBucketActionMessage msg = 
					new BucketActionMessage.TestBucketActionMessage(manual_trigger_bucket, BeanTemplateUtils.build(ProcessingTestSpecBean.class).done().get());
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish			
			waitForData(() -> trigger_crud.countObjects().join(), 7, true);
			//(status)
			waitForData(() -> getLastRunTime(manual_trigger_bucket.full_name(), Optional.empty(), true), -1L, true);
			waitForData(() -> getLastRunTime(manual_trigger_bucket.full_name(), Optional.empty(), false), -1L, true);
			
			// Check the DB
		
			assertEquals(7L, trigger_crud.countObjects().join().intValue());
			// Check they aren't suspended:
			assertEquals(7L, trigger_crud.countObjectsBySpec(
					CrudUtils.allOf(AnalyticTriggerStateBean.class)
						.when(AnalyticTriggerStateBean::is_bucket_suspended, false)
					).join().intValue());
			
			// underlying status
			assertEquals(-1L, getLastRunTime(manual_trigger_bucket.full_name(), Optional.empty(), true).longValue());
			assertEquals(-1L, getLastRunTime(manual_trigger_bucket.full_name(), Optional.empty(), false).longValue());
		}
		
		// 2) Update in suspended mode (will delete because it's a test)
		{
			final BucketActionMessage.UpdateBucketActionMessage msg = 
					new BucketActionMessage.UpdateBucketActionMessage(manual_trigger_bucket, false, manual_trigger_bucket, new HashSet<String>());
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish			
			waitForData(() -> trigger_crud.countObjects().join(), 0, false);
			
			// Check the DB
		
			assertEquals(0L, trigger_crud.countObjects().join().intValue());
			
			// underlying status
			assertEquals(-1L, getLastRunTime(manual_trigger_bucket.full_name(), Optional.empty(), true).longValue());
			assertEquals(-1L, getLastRunTime(manual_trigger_bucket.full_name(), Optional.empty(), false).longValue());
		}
		// Check no malformed buckets
		assertEquals(0L, _num_received_errors.get());
	}
	
	///////////////////////////////////////////////////////////////////////////////////////
	
	// MULTIPLE INTERNAL TRIGGERS

	@Test
	public void test_jobTriggerScenario() throws InterruptedException, IOException {
		System.out.println("Starting test_jobTriggerScenario");

		// Tests a manual bucket that has inter-job dependencies
		
		final ICrudService<AnalyticTriggerStateBean> trigger_crud = 
				_service_context.getCoreManagementDbService().readOnlyVersion().getAnalyticBucketTriggerState(AnalyticTriggerStateBean.class);
		trigger_crud.deleteDatastore().join();

		test_jobTriggerScenario_actuallyRun(trigger_crud);		
	}
	
	public void test_jobTriggerScenario_actuallyRun(final ICrudService<AnalyticTriggerStateBean> trigger_crud) throws InterruptedException, IOException {
		
		// Setup:
		
		final String json_bucket = Resources.toString(Resources.getResource("com/ikanow/aleph2/data_import_manager/analytics/actors/simple_job_deps_bucket.json"), Charsets.UTF_8);		
		final DataBucketBean bucket = BeanTemplateUtils.from(json_bucket, DataBucketBean.class).get();
		
		insertStatusForBucket(bucket);		
		
		// This can run inside another job so need to be a bit		
		long prev = _num_received.get();
		_num_received.set(0L);		
		Supplier<Long> getCount = () -> trigger_crud.countObjectsBySpec(
				CrudUtils.allOf(AnalyticTriggerStateBean.class).when(AnalyticTriggerStateBean::bucket_name, bucket.full_name())).join();
		
		// (have to save the bucket to make trigger checks work correctly)
		_service_context.getService(IManagementDbService.class, Optional.empty()).get().getDataBucketStore().storeObject(bucket, true).join();			
		
		// 1) Send a message to the worker to fill in that bucket
		{
			final BucketActionMessage.NewBucketActionMessage msg = 
					new BucketActionMessage.NewBucketActionMessage(bucket, false);
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish			
			waitForData(getCount, 3, true);
			//(status)
			waitForData(() -> getLastRunTime(bucket.full_name(), Optional.empty(), true), -1L, true);
			waitForData(() -> getLastRunTime(bucket.full_name(), Optional.empty(), false), -1L, true);
			
			// Check the DB
		
			assertEquals(3L, getCount.get().intValue());
			// Check they aren't suspended:
			assertEquals(3L, trigger_crud.countObjectsBySpec(
					CrudUtils.allOf(AnalyticTriggerStateBean.class)
						.when(AnalyticTriggerStateBean::bucket_name, bucket.full_name())
						.when(AnalyticTriggerStateBean::is_bucket_suspended, false)
					).join().intValue());
			
			// Check the message bus - nothing yet!
			
			assertEquals(0, _num_received.get());
			
			// underlying status
			assertEquals(-1L, getLastRunTime(bucket.full_name(), Optional.empty(), true).longValue());
			assertEquals(-1L, getLastRunTime(bucket.full_name(), Optional.empty(), false).longValue());			
		}
		
		// 2) Perform a trigger check, make sure that nothing has activated
		{
			
			final AnalyticTriggerMessage msg = new AnalyticTriggerMessage(new AnalyticTriggerMessage.AnalyticsTriggerActionMessage());
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			//(code coverage! this will be ignored due to spacing)
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish			
			Thread.sleep(1000L);
			
			// Check the DB
		
			// (ie no active records)
			assertEquals(3L, getCount.get().intValue());
			
			// Check the message bus - nothing yet!
			
			assertEquals(0, _num_received.get());
			
			// underlying status
			assertEquals(-1L, getLastRunTime(bucket.full_name(), Optional.empty(), true).longValue());
			assertEquals(-1L, getLastRunTime(bucket.full_name(), Optional.empty(), false).longValue());			
		}		
		
		// 3a) Let the worker know that job1 has started (which should also launch the bucket)
		final Date now_stage3a = new Date();
		{
			final BucketActionMessage.BucketActionAnalyticJobMessage inner_msg = 
					new BucketActionMessage.BucketActionAnalyticJobMessage(bucket, 
							bucket.analytic_thread().jobs().stream().filter(j -> j.name().equals("initial_phase")).collect(Collectors.toList()),
							JobMessageType.starting);

			final AnalyticTriggerMessage msg = new AnalyticTriggerMessage(inner_msg);			
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish			
			waitForData(getCount, 5, true);
			//(status)
			waitForData(() -> getLastRunTime(bucket.full_name(), Optional.empty(), true), now_stage3a.getTime(), true);
			waitForData(() -> getLastRunTime(bucket.full_name(), Optional.empty(), false), -1L, true);
			
			// Check the DB
		
			// (ie creates a bucket active record and a job active record)
			assertEquals(5L, getCount.get().intValue());
			
			// Confirm the extra 2 records are as above
			assertEquals(1L, trigger_crud.countObjectsBySpec(
					CrudUtils.allOf(AnalyticTriggerStateBean.class)
						.when(AnalyticTriggerStateBean::bucket_name, bucket.full_name())
						.when(AnalyticTriggerStateBean::trigger_type, TriggerType.none)
						.withNotPresent(AnalyticTriggerStateBean::job_name)
					).join().intValue());
			assertEquals(1L, trigger_crud.countObjectsBySpec(
					CrudUtils.allOf(AnalyticTriggerStateBean.class)
						.when(AnalyticTriggerStateBean::bucket_name, bucket.full_name())
						.when(AnalyticTriggerStateBean::trigger_type, TriggerType.none)
						.when(AnalyticTriggerStateBean::job_name, "initial_phase")
					).join().intValue());
			
			// Check the message bus - nothing yet!
			
			assertEquals(0, _num_received.get());
			
			// underlying status: 1st job started, bucket started
			//global
			assertTrue(getLastRunTime(bucket.full_name(), Optional.empty(), true).longValue() >= now_stage3a.getTime());
			assertEquals(-1L, getLastRunTime(bucket.full_name(), Optional.empty(), false).longValue());
			//job #1 "initial phase"
			assertTrue(getLastRunTime(bucket.full_name(), Optional.of("initial_phase"), true).longValue() >= now_stage3a.getTime());
			assertEquals(-1L, getLastRunTime(bucket.full_name(), Optional.of("initial_phase"), false).longValue());						
		}
		
		// 3b) Send a job completion message
		Thread.sleep(100L); // (just make sure this is != now_stage3a)
		final Date now_stage3b = new Date();
		{
			final BucketActionMessage.BucketActionAnalyticJobMessage inner_msg = 
					new BucketActionMessage.BucketActionAnalyticJobMessage(bucket, 
							bucket.analytic_thread().jobs().stream().filter(j -> j.name().equals("initial_phase")).collect(Collectors.toList()),
							JobMessageType.stopping);
			
			final AnalyticTriggerMessage msg = new AnalyticTriggerMessage(inner_msg);			
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish			
			waitForData(getCount, 4, false);
			waitForData(() -> getLastRunTime(bucket.full_name(), Optional.of("initial_phase"), false), -1L, false);
			
			// Check the DB
			
			// (ie active job is removed, bucket remains)
			assertEquals(4L, getCount.get().intValue());
			
			// Confirm the extra record is as above
			assertEquals(1L, trigger_crud.countObjectsBySpec(
					CrudUtils.allOf(AnalyticTriggerStateBean.class)
						.when(AnalyticTriggerStateBean::bucket_name, bucket.full_name())
						.when(AnalyticTriggerStateBean::trigger_type, TriggerType.none)
						.withNotPresent(AnalyticTriggerStateBean::job_name)
					).join().intValue());
			
			// Check the message bus - nothing yet!
			
			assertEquals(0, _num_received.get());
			
			// underlying status: 1st job stopped, bucket unchanged
			//global
			assertTrue(getLastRunTime(bucket.full_name(), Optional.empty(), true).longValue() >= now_stage3a.getTime());
			assertEquals(-1L, getLastRunTime(bucket.full_name(), Optional.empty(), false).longValue());
			//job #1 "initial phase"
			final long last_time = getLastRunTime(bucket.full_name(), Optional.of("initial_phase"), false).longValue();
			assertTrue("Time errors: " + new Date(last_time) + " >= " + now_stage3a + " < " + now_stage3b, 
					(last_time >= now_stage3a.getTime()) && (last_time < now_stage3b.getTime()));
			assertEquals(-1L, getLastRunTime(bucket.full_name(), Optional.of("initial_phase"), true).longValue());
		}		
		
		// 4) Perform a trigger check, make sure that only the right job ("next_phase") has started
		Thread.sleep(100L); // (just make sure this is != now_stage3a)
		final Date now_stage4 = new Date();
		{
			final AnalyticTriggerMessage msg = new AnalyticTriggerMessage(new AnalyticTriggerMessage.AnalyticsTriggerActionMessage());
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish			
			waitForData(getCount, 5, true);
			waitForData(() -> _num_received.get(), 1, true);
			//(status)
			waitForData(() -> getLastRunTime(bucket.full_name(), Optional.of("next_phase"), true), now_stage4.getTime(), true);
			waitForData(() -> getLastRunTime(bucket.full_name(), Optional.of("next_phase"), false), -1L, true);
			
			// Check the DB
		
			// (bucket active still present, now "next_phase" has started)
			assertEquals(5L, getCount.get().intValue());
			
			// Confirm the extra 2 records are as above
			assertEquals(1L, trigger_crud.countObjectsBySpec(
					CrudUtils.allOf(AnalyticTriggerStateBean.class)
						.when(AnalyticTriggerStateBean::bucket_name, bucket.full_name())
						.when(AnalyticTriggerStateBean::trigger_type, TriggerType.none)
						.withNotPresent(AnalyticTriggerStateBean::job_name)
					).join().intValue());			
			assertEquals(1L, trigger_crud.countObjectsBySpec(
					CrudUtils.allOf(AnalyticTriggerStateBean.class)
						.when(AnalyticTriggerStateBean::trigger_type, TriggerType.none)
						.when(AnalyticTriggerStateBean::job_name, "next_phase")
					).join().intValue());
			
			// Check the message bus - should have received a start message for the triggered job
			
			assertEquals(1, _num_received.get());

			// underlying status: 1st job stopped, 2nd job started, bucket unchanged
			//global
			assertTrue(getLastRunTime(bucket.full_name(), Optional.empty(), true).longValue() >= now_stage3a.getTime());
			assertEquals(-1L, getLastRunTime(bucket.full_name(), Optional.empty(), false).longValue());
			//job #1 "initial phase"
			final long last_time = getLastRunTime(bucket.full_name(), Optional.of("initial_phase"), false).longValue();
			assertTrue("Time errors: " + new Date(last_time) + " >= " + now_stage3a + " < " + now_stage3b, 
					(last_time >= now_stage3a.getTime()) && (last_time < now_stage3b.getTime()));
			assertEquals(-1L, getLastRunTime(bucket.full_name(), Optional.of("initial_phase"), true).longValue());
			//job #2: next phase
			assertTrue(getLastRunTime(bucket.full_name(), Optional.of("next_phase"), true).longValue() >= now_stage4.getTime());
			assertEquals(-1L, getLastRunTime(bucket.full_name(), Optional.of("next_phase"), false).longValue());						
		}		
		
		// 5) Send a job completion message
		Thread.sleep(100L); // (just make sure this is != now_stage3a)
		final Date now_stage5 = new Date();
		{
			final BucketActionMessage.BucketActionAnalyticJobMessage inner_msg = 
					new BucketActionMessage.BucketActionAnalyticJobMessage(bucket, 
							bucket.analytic_thread().jobs().stream().filter(j -> j.name().equals("next_phase")).collect(Collectors.toList()),
							JobMessageType.stopping);
			
			final AnalyticTriggerMessage msg = new AnalyticTriggerMessage(inner_msg);			
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish			
			waitForData(getCount, 4, false);
			//(status)
			waitForData(() -> getLastRunTime(bucket.full_name(), Optional.of("next_phase"), false), -1L, false);
			
			// Check the DB
			
			// (ie active job is removed, bucket remains)
			assertEquals(4L, getCount.get().intValue());
			
			// Confirm the extra record is as above
			assertEquals(1L, trigger_crud.countObjectsBySpec(
					CrudUtils.allOf(AnalyticTriggerStateBean.class)
						.when(AnalyticTriggerStateBean::bucket_name, bucket.full_name())
						.when(AnalyticTriggerStateBean::trigger_type, TriggerType.none)
						.withNotPresent(AnalyticTriggerStateBean::job_name)
					).join().intValue());
			
			// Check the message bus - no change
			
			assertEquals(1, _num_received.get());
			
			// underlying status: 1st job stopped, 2nd job stopped, bucket unchanged
			//global
			assertTrue(getLastRunTime(bucket.full_name(), Optional.empty(), true).longValue() >= now_stage3a.getTime());
			assertEquals(-1L, getLastRunTime(bucket.full_name(), Optional.empty(), false).longValue());
			//job #1 "initial phase"
			{
				final long last_time = getLastRunTime(bucket.full_name(), Optional.of("initial_phase"), false).longValue();
				assertTrue("Time errors: " + new Date(last_time) + " >= " + now_stage3a + " < " + now_stage3b, 
						(last_time >= now_stage3a.getTime()) && (last_time < now_stage3b.getTime()));
				assertEquals(-1L, getLastRunTime(bucket.full_name(), Optional.of("initial_phase"), true).longValue());
			}
			//job #2: next phase
			{
				final long last_time = getLastRunTime(bucket.full_name(), Optional.of("next_phase"), false).longValue();
				assertTrue("Time errors: " + new Date(last_time) + " >= " + now_stage4 + " < " + now_stage5, 
						(last_time >= now_stage4.getTime()) && (last_time < now_stage5.getTime()));
				assertEquals(-1L, getLastRunTime(bucket.full_name(), Optional.of("next_phase"), true).longValue());
			}
		}
		
		// 6) Perform a trigger check, make sure that only the right job ("final_phase") has started 
		Thread.sleep(100L); // (just make sure this is != now_stage3a)
		final Date now_stage6 = new Date();
		{
			final AnalyticTriggerMessage msg = new AnalyticTriggerMessage(new AnalyticTriggerMessage.AnalyticsTriggerActionMessage());
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish			
			waitForData(getCount, 5, true);
			waitForData(() -> _num_received.get(), 2, true);
			//(status)
			waitForData(() -> getLastRunTime(bucket.full_name(), Optional.of("final_phase"), true), now_stage6.getTime(), true);
			waitForData(() -> getLastRunTime(bucket.full_name(), Optional.of("final_phase"), false), -1L, true);

			// Check the DB
		
			// (bucket active still present, now "next_phase" has started)
			assertEquals(5L, getCount.get().intValue());
			
			// Confirm the extra 2 records are as above
			assertEquals(1L, trigger_crud.countObjectsBySpec(
					CrudUtils.allOf(AnalyticTriggerStateBean.class)
						.when(AnalyticTriggerStateBean::bucket_name, bucket.full_name())
						.when(AnalyticTriggerStateBean::trigger_type, TriggerType.none)
						.withNotPresent(AnalyticTriggerStateBean::job_name)
					).join().intValue());			
			assertEquals(1L, trigger_crud.countObjectsBySpec(
					CrudUtils.allOf(AnalyticTriggerStateBean.class)
						.when(AnalyticTriggerStateBean::bucket_name, bucket.full_name())
						.when(AnalyticTriggerStateBean::trigger_type, TriggerType.none)
						.when(AnalyticTriggerStateBean::job_name, "final_phase")
					).join().intValue());
			
			// Check the message bus - should have received a start message for the triggered job
			
			assertEquals(2, _num_received.get()); //+1
			
			// underlying status: 1st job stopped, 2nd job stopped, 3rd job started bucket unchanged
			//global
			assertTrue(getLastRunTime(bucket.full_name(), Optional.empty(), true).longValue() >= now_stage3a.getTime());
			assertEquals(-1L, getLastRunTime(bucket.full_name(), Optional.empty(), false).longValue());
			//job #1 "initial phase"
			{
				final long last_time = getLastRunTime(bucket.full_name(), Optional.of("initial_phase"), false).longValue();
				assertTrue("Time errors: " + new Date(last_time) + " >= " + now_stage3a + " < " + now_stage3b, 
						(last_time >= now_stage3a.getTime()) && (last_time < now_stage3b.getTime()));
				assertEquals(-1L, getLastRunTime(bucket.full_name(), Optional.of("initial_phase"), true).longValue());
			}
			//job #2: next phase
			{
				final long last_time = getLastRunTime(bucket.full_name(), Optional.of("next_phase"), false).longValue();
				assertTrue("Time errors: " + new Date(last_time) + " >= " + now_stage4 + " < " + now_stage5, 
						(last_time >= now_stage4.getTime()) && (last_time < now_stage5.getTime()));
				assertEquals(-1L, getLastRunTime(bucket.full_name(), Optional.of("next_phase"), true).longValue());
			}
			//job #3: final phase
			{
				assertTrue(getLastRunTime(bucket.full_name(), Optional.of("final_phase"), true).longValue() >= now_stage6.getTime());
				assertEquals(-1L, getLastRunTime(bucket.full_name(), Optional.of("final_phase"), false).longValue());										
			}
		}		
		
		// 7) Stop the final job
		Thread.sleep(100L); // (just make sure this is != now_stage3a)
		final Date now_stage7 = new Date();
		{
			final BucketActionMessage.BucketActionAnalyticJobMessage inner_msg = 
					new BucketActionMessage.BucketActionAnalyticJobMessage(bucket, 
							bucket.analytic_thread().jobs().stream().filter(j -> j.name().equals("final_phase")).collect(Collectors.toList()),
							JobMessageType.stopping);
			
			final AnalyticTriggerMessage msg = new AnalyticTriggerMessage(inner_msg);			
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish			
			waitForData(getCount, 4, false);
			//(status)
			waitForData(() -> getLastRunTime(bucket.full_name(), Optional.of("final_phase"), false), -1L, false);
			
			// Check the DB
			
			// (ie active job is removed, bucket remains)
			assertEquals(4L, getCount.get().intValue());
			
			// Confirm the extra record is as above
			assertEquals(1L, trigger_crud.countObjectsBySpec(
					CrudUtils.allOf(AnalyticTriggerStateBean.class)
						.when(AnalyticTriggerStateBean::bucket_name, bucket.full_name())
						.when(AnalyticTriggerStateBean::trigger_type, TriggerType.none)
						.withNotPresent(AnalyticTriggerStateBean::job_name)
					).join().intValue());
			
			// Check the message bus - no change
			
			assertEquals(2, _num_received.get());
			
			// underlying status: 1st job stopped, 2nd job stopped, 3rd job stopped, bucket unchanged
			//global
			assertTrue(getLastRunTime(bucket.full_name(), Optional.empty(), true).longValue() >= now_stage3a.getTime());
			assertEquals(-1L, getLastRunTime(bucket.full_name(), Optional.empty(), false).longValue());
			//job #1 "initial phase"
			{
				final long last_time = getLastRunTime(bucket.full_name(), Optional.of("initial_phase"), false).longValue();
				assertTrue("Time errors: " + new Date(last_time) + " >= " + now_stage3a + " < " + now_stage3b, 
						(last_time >= now_stage3a.getTime()) && (last_time < now_stage3b.getTime()));
				assertEquals(-1L, getLastRunTime(bucket.full_name(), Optional.of("initial_phase"), true).longValue());
			}
			//job #2: next phase
			{
				final long last_time = getLastRunTime(bucket.full_name(), Optional.of("next_phase"), false).longValue();
				assertTrue("Time errors: " + new Date(last_time) + " >= " + now_stage4 + " < " + now_stage5, 
						(last_time >= now_stage4.getTime()) && (last_time < now_stage5.getTime()));
				assertEquals(-1L, getLastRunTime(bucket.full_name(), Optional.of("next_phase"), true).longValue());
			}
			//job #3: final phase
			{
				final long last_time = getLastRunTime(bucket.full_name(), Optional.of("final_phase"), false).longValue();
				assertTrue("Time errors: " + new Date(last_time) + " >= " + now_stage6 + " < " + now_stage7, 
						(last_time >= now_stage6.getTime()) && (last_time < now_stage7.getTime()));
				assertEquals(-1L, getLastRunTime(bucket.full_name(), Optional.of("final_phase"), true).longValue());
			}
		}
		
		// 8) Trigger - should complete the bucket (since has had bucket activated, 60s timeout doesn't apply) 
		Thread.sleep(100L); // (just make sure this is != now_stage3a)
		final Date now_stage8 = new Date();
		{
			final AnalyticTriggerMessage msg = new AnalyticTriggerMessage(new AnalyticTriggerMessage.AnalyticsTriggerActionMessage());
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish			
			waitForData(getCount, 3, false);
			waitForData(() -> _num_received.get(), 3, true);
			//(status)
			waitForData(() -> getLastRunTime(bucket.full_name(), Optional.empty(), false), -1L, false);
			
			// Check the DB
		
			// (all active records removed)
			assertEquals(3L, getCount.get().intValue());
			
			// Check the message bus - should have received a stop message for the bucket
			
			assertEquals(3, _num_received.get()); 
			
			// underlying status: 1st job stopped, 2nd job stopped, 3rd job stopped, bucket stopped
			//global
			{
				final long last_time = getLastRunTime(bucket.full_name(), Optional.empty(), false).longValue();
				assertTrue("Time errors: " + new Date(last_time) + " >= " + now_stage3a + " < " + now_stage8, 
						(last_time >= now_stage3a.getTime()) && (last_time < now_stage8.getTime()));
				assertEquals(-1L, getLastRunTime(bucket.full_name(), Optional.empty(), true).longValue());
			}
			//job #1 "initial phase"
			{
				final long last_time = getLastRunTime(bucket.full_name(), Optional.of("initial_phase"), false).longValue();
				assertTrue("Time errors: " + new Date(last_time) + " >= " + now_stage3a + " < " + now_stage3b, 
						(last_time >= now_stage3a.getTime()) && (last_time < now_stage3b.getTime()));
				assertEquals(-1L, getLastRunTime(bucket.full_name(), Optional.of("initial_phase"), true).longValue());
			}
			//job #2: next phase
			{
				final long last_time = getLastRunTime(bucket.full_name(), Optional.of("next_phase"), false).longValue();
				assertTrue("Time errors: " + new Date(last_time) + " >= " + now_stage4 + " < " + now_stage5, 
						(last_time >= now_stage4.getTime()) && (last_time < now_stage5.getTime()));
				assertEquals(-1L, getLastRunTime(bucket.full_name(), Optional.of("next_phase"), true).longValue());
			}
			//job #3: final phase
			{
				final long last_time = getLastRunTime(bucket.full_name(), Optional.of("final_phase"), false).longValue();
				assertTrue("Time errors: " + new Date(last_time) + " >= " + now_stage6 + " < " + now_stage7, 
						(last_time >= now_stage6.getTime()) && (last_time < now_stage7.getTime()));
				assertEquals(-1L, getLastRunTime(bucket.full_name(), Optional.of("final_phase"), true).longValue());
			}
		}
		_num_received.set(prev);
		
		// Check no malformed buckets
		assertEquals(0L, _num_received_errors.get());
	}
	
	///////////////////////////////////////////////////////////////////////////////////////
	
	// ENRICHMENT STYLE BUCKETS
	
	@Test
	public void test_enrichment_analyticForm() throws IOException, InterruptedException, ExecutionException, TimeoutException {
		System.out.println("Starting test_enrichment_analyticForm");
		
		final String json_bucket = Resources.toString(Resources.getResource("com/ikanow/aleph2/data_import_manager/analytics/actors/real_test_case_batch_2.json"), Charsets.UTF_8);		
		final DataBucketBean enrichment_bucket = BeanTemplateUtils.from(json_bucket, DataBucketBean.class).get();
		
		// (have to save the bucket to make trigger checks work correctly)
		_service_context.getService(IManagementDbService.class, Optional.empty()).get().getDataBucketStore().storeObject(enrichment_bucket, true).get(1, TimeUnit.MINUTES);		
		
		test_enrichment_common(enrichment_bucket);
	}

	@Test
	public void test_enrichment_enrichmentForm() throws IOException, InterruptedException {
		System.out.println("Starting test_enrichment_enrichmentForm");
		
		final String json_bucket_in_db = Resources.toString(Resources.getResource("com/ikanow/aleph2/data_import_manager/analytics/actors/batch_enrichment_test_in.json"), Charsets.UTF_8);		
		final String json_bucket_in_mem = Resources.toString(Resources.getResource("com/ikanow/aleph2/data_import_manager/analytics/actors/batch_enrichment_test_out.json"), Charsets.UTF_8);
		
		final DataBucketBean enrichment_bucket_in_db = BeanTemplateUtils.from(json_bucket_in_db, DataBucketBean.class).get();
		final DataBucketBean enrichment_bucket_in_mem = BeanTemplateUtils.from(json_bucket_in_mem, DataBucketBean.class).get();
		
		// (have to save the bucket to make trigger checks work correctly)
		_service_context.getService(IManagementDbService.class, Optional.empty()).get().getDataBucketStore().storeObject(enrichment_bucket_in_db, true).join();		
		
		test_enrichment_common(enrichment_bucket_in_mem);
	}
	
	public void test_enrichment_common(final DataBucketBean bucket) throws IOException, InterruptedException {
		System.out.println("Starting test_enrichment_common");

		//setup		
		final ICrudService<AnalyticTriggerStateBean> trigger_crud = 
				_service_context.getCoreManagementDbService().readOnlyVersion().getAnalyticBucketTriggerState(AnalyticTriggerStateBean.class);
		trigger_crud.deleteDatastore().join();
		
		_num_received.set(0L);
		
		insertStatusForBucket(bucket);				
		
		final AnalyticThreadJobBean job_to_run = bucket.analytic_thread().jobs().get(0);
		
		// 0) create input directory
				
		final String root_dir = _service_context.getStorageService().getBucketRootPath();
		final String suffix = IStorageService.TO_IMPORT_DATA_SUFFIX;
		createDirectory(root_dir, bucket.full_name(), suffix, 0, true);
		
		// 1) Send a message to the worker to fill in that bucket
		{
			final BucketActionMessage.NewBucketActionMessage msg = 
					new BucketActionMessage.NewBucketActionMessage(bucket, false);
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish	
			waitForData(() -> trigger_crud.countObjects().join(), 1, true);
			
			// Check the DB
		
			assertEquals(1L, trigger_crud.countObjects().join().intValue());
			
			// Check the message bus - nothing yet!
			
			assertEquals(0, _num_received.get());
		}
		
		// 2) Perform a trigger check, make sure that nothing has activated
		{
			final AnalyticTriggerMessage msg = new AnalyticTriggerMessage(new AnalyticTriggerMessage.AnalyticsTriggerActionMessage());
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish
			Thread.sleep(2000L);
			
			// Check the DB
		
			// (ie no active records)
			assertEquals(1L, trigger_crud.countObjects().join().intValue());
			
			// Check the message bus - nothing yet!
			
			assertEquals(0, _num_received.get());			
		}		
		
		// 3a) Add files to input directory, checks triggers (external triggers are slower so shouldn't trigger anything)
		{
			createDirectory(root_dir, bucket.full_name(), suffix, 1, false);
			
			//DEBUG: leave this in since failed vs travis
			_logger.info("(Added files)");
			printTriggerDatabase();
			
			final AnalyticTriggerMessage msg = new AnalyticTriggerMessage(new AnalyticTriggerMessage.AnalyticsTriggerActionMessage());
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish
			Thread.sleep(2000L);
			
			// Check the DB
		
			// (ie no active records)
			assertEquals(1L, trigger_crud.countObjects().join().intValue());
			
			// Check the message bus - nothing yet!
			
			assertEquals(0, _num_received.get());						
		}
		// 3b) reset triggers try again
		{
			resetTriggerCheckTimes(trigger_crud); // (since the external triggers
			
			final AnalyticTriggerMessage msg = new AnalyticTriggerMessage(new AnalyticTriggerMessage.AnalyticsTriggerActionMessage());
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish
			Thread.sleep(2000L);
			waitForData(() -> _num_received.get(), 1, true);
			
			// Check the DB
		
			// external trigger + bucket active record 
			assertEquals(2L, trigger_crud.countObjects().join().intValue());
			
			// Check the message bus - should get a job start notification
			
			assertEquals(1, _num_received.get());						
		}
		
		// 4) Check doesn't trigger when active, even with files in directories .. but also that it doesn't shut the job down if it takes >10s for a job to start
		{
			resetTriggerCheckTimes(trigger_crud); // (since the external triggers
			
			final AnalyticTriggerMessage msg = new AnalyticTriggerMessage(new AnalyticTriggerMessage.AnalyticsTriggerActionMessage());
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish
			waitForData(() -> trigger_crud.countObjects().join(), 2, true);
			
			// Check the DB
		
			// external trigger + bucket active record 
			assertEquals(2L, trigger_crud.countObjects().join().intValue());
			
			// Check the message bus - nothing else received
			
			assertEquals(1, _num_received.get());						
		}
		
		// 4b) I should now receive some started messages back - again should do nothing
		{
			final BucketActionMessage.BucketActionAnalyticJobMessage inner_msg = 
					new BucketActionMessage.BucketActionAnalyticJobMessage(bucket, 
							Arrays.asList(job_to_run),
							JobMessageType.starting);

			final AnalyticTriggerMessage msg = new AnalyticTriggerMessage(inner_msg);			
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish			
			waitForData(() -> trigger_crud.countObjects().join(), 3, true);
			
			// Check the DB
		
			// (ie creates a bucket active record and a job active record)
			assertEquals(3L, trigger_crud.countObjects().join().intValue());
			
			// Confirm the extra 2 records are as above
			assertEquals(1L, trigger_crud.countObjectsBySpec(
					CrudUtils.allOf(AnalyticTriggerStateBean.class)
						.when(AnalyticTriggerStateBean::trigger_type, TriggerType.none)
						.withNotPresent(AnalyticTriggerStateBean::job_name)
					).join().intValue());
			assertEquals(1L, trigger_crud.countObjectsBySpec(
					CrudUtils.allOf(AnalyticTriggerStateBean.class)
						.when(AnalyticTriggerStateBean::trigger_type, TriggerType.none)
						.when(AnalyticTriggerStateBean::job_name, job_to_run.name())
					).join().intValue());
			
			// Check the message bus - nothing changed
			
			assertEquals(1, _num_received.get());
		}		
		
		// 4c) Check doesn't trigger when active, even with files in directories .. 
		{
			resetTriggerCheckTimes(trigger_crud); // (since the external triggers
			
			final AnalyticTriggerMessage msg = new AnalyticTriggerMessage(new AnalyticTriggerMessage.AnalyticsTriggerActionMessage());
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish
			Thread.sleep(2000L);
			waitForData(() -> _num_received.get(), 2, true);
			
			// Check the DB
		
			// external trigger + bucket active record + job active record
			assertEquals(3L, trigger_crud.countObjects().join().intValue());
			
			// Check the message bus - get a check completion message
			
			assertEquals(2, _num_received.get());						
		}
		
		// 5) De-activate
		{
			final BucketActionMessage.BucketActionAnalyticJobMessage inner_msg = 
					new BucketActionMessage.BucketActionAnalyticJobMessage(bucket, 
							Arrays.asList(job_to_run),
							JobMessageType.stopping);

			final AnalyticTriggerMessage msg = new AnalyticTriggerMessage(inner_msg);			
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish			
			waitForData(() -> trigger_crud.countObjects().join(), 2, false);
			
			// Check the DB
		
			// (ie creates a bucket active record and the original trigger)
			assertEquals(2L, trigger_crud.countObjects().join().intValue());
			
			// Check the message bus - nothing new
			
			assertEquals(2, _num_received.get());									
		}		
		
		// 6) Check triggers again - first bucket gets turned off
		{
			resetTriggerCheckTimes(trigger_crud); // (since the external triggers
			
			final AnalyticTriggerMessage msg = new AnalyticTriggerMessage(new AnalyticTriggerMessage.AnalyticsTriggerActionMessage());
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish
			waitForData(() -> trigger_crud.countObjects().join(), 1, false);
			waitForData(() -> _num_received.get(), 3, true);
			
			// Check the DB
		
			// back to the external trigger
			assertEquals(1L, trigger_crud.countObjects().join().intValue());
			
			// Check the message bus - get a bucket stopping message
			
			assertEquals(3, _num_received.get());						
		}
		
		// 7) Check triggers again - finally .. we're off again since there are still files in the directory!
		{
			resetTriggerCheckTimes(trigger_crud); // (since the external triggers
			
			final AnalyticTriggerMessage msg = new AnalyticTriggerMessage(new AnalyticTriggerMessage.AnalyticsTriggerActionMessage());
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish
			waitForData(() -> trigger_crud.countObjects().join(), 1, false);
			waitForData(() -> _num_received.get(), 4, true);
			
			// Check the DB
		
			// external trigger + bucket active record + job active record
			assertEquals(2L, trigger_crud.countObjects().join().intValue());
			
			// Check the message bus - get a bucket starting message
			
			assertEquals(4, _num_received.get());						
		}
		// Check no malformed buckets
		assertEquals(0L, _num_received_errors.get());
	}
	
	///////////////////////////////////////////////////////////////////////////////////////
	
	// MULTIPLE EXTERNAL TRIGGERS
	
	@Test
	public void test_externalTriggers() throws IOException, InterruptedException {
		System.out.println("Starting test_externalTriggers");

		//setup		
		final String json = Resources.toString(Resources.getResource("com/ikanow/aleph2/data_import_manager/analytics/actors/trigger_bucket.json"), Charsets.UTF_8);

		final DataBucketBean bucket = BeanTemplateUtils.from(json, DataBucketBean.class).get();

		insertStatusForBucket(bucket);				
		
		// (have to save the bucket to make trigger checks work correctly)
		_service_context.getService(IManagementDbService.class, Optional.empty()).get().getDataBucketStore().storeObject(bucket, true).join();		

		final ICrudService<AnalyticTriggerStateBean> trigger_crud = 
				_service_context.getCoreManagementDbService().readOnlyVersion().getAnalyticBucketTriggerState(AnalyticTriggerStateBean.class);
		trigger_crud.deleteDatastore().join();

		_num_received.set(0L);

		Supplier<Long> getCount = () -> trigger_crud.countObjectsBySpec(
				CrudUtils.allOf(AnalyticTriggerStateBean.class).when(AnalyticTriggerStateBean::bucket_name, bucket.full_name())).join();		
		
		final AnalyticThreadJobBean job_to_run_first = bucket.analytic_thread().jobs().get(0);
		final AnalyticThreadJobBean job_to_run_last = bucket.analytic_thread().jobs().get(1);

		// 0) create input directory

		final String root_dir = _service_context.getStorageService().getBucketRootPath();
		final String suffix = IStorageService.TO_IMPORT_DATA_SUFFIX;
		createDirectory(root_dir, bucket.full_name(), suffix, 0, true);

		// 1) Send a message to the worker to fill in that bucket
		{
			final BucketActionMessage.NewBucketActionMessage msg = 
					new BucketActionMessage.NewBucketActionMessage(bucket, false);

			_trigger_worker.tell(msg, _trigger_worker);

			// Give it a couple of secs to finish	
			waitForData(getCount, 1, true);

			// Check the DB - 2 external triggers, 1 internal trigger 

			assertEquals(3L, getCount.get().longValue());

			// Check the message bus - nothing yet!

			assertEquals(0, _num_received.get());
		}
		
		// 2) Perform a trigger check, make sure that nothing has activated
		{
			final AnalyticTriggerMessage msg = new AnalyticTriggerMessage(new AnalyticTriggerMessage.AnalyticsTriggerActionMessage());
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish
			Thread.sleep(2000L);
			
			// Check the DB
		
			// (ie no active records)
			assertEquals(3L, getCount.get().longValue());
			
			// Check the message bus - nothing yet!
			
			assertEquals(0, _num_received.get());			
		}		
		
		// 3a) Add files to input directory, checks triggers (external triggers are slower so shouldn't trigger anything)
		{
			createDirectory(root_dir, bucket.full_name(), suffix, 1, false);
			
			//DEBUG: leave this in since failed vs travis
			_logger.info("(Added files)");
			printTriggerDatabase();
			
			final AnalyticTriggerMessage msg = new AnalyticTriggerMessage(new AnalyticTriggerMessage.AnalyticsTriggerActionMessage());
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish
			Thread.sleep(2000L);
			
			// Check the DB
		
			// (ie no active records)
			assertEquals(3L, getCount.get().longValue());
			
			// Check the message bus - nothing yet!
			
			assertEquals(0, _num_received.get());						
		}
		// 3b) reset triggers try again ... still nothing because we only have 1/2 matching triggers
		{
			resetTriggerCheckTimes(trigger_crud); // (since the external triggers
			
			final AnalyticTriggerMessage msg = new AnalyticTriggerMessage(new AnalyticTriggerMessage.AnalyticsTriggerActionMessage());
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish
			Thread.sleep(2000L);
			
			// Check the DB
			
			// (ie no active records)
			assertEquals(3L, getCount.get().longValue());
			
			// Check the message bus - nothing yet!
			
			assertEquals(0, _num_received.get());						
		}
		
		
		// 4a) Launch other bucket, checks triggers - should immediately fire since the "stop" of the previous bucket should update the check times of _both_ external dependencies
		{
			this.test_jobTriggerScenario_actuallyRun(trigger_crud);
			
			//DEBUG: leave this in since failed vs travis
			_logger.info("(Added files)");
			printTriggerDatabase();
			
			final AnalyticTriggerMessage msg = new AnalyticTriggerMessage(new AnalyticTriggerMessage.AnalyticsTriggerActionMessage());
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish
			waitForData(getCount, 4, true);
			waitForData(() -> _num_received.get(), 1, true);
			
			// Check the DB
		
			// 2 external triggers + internal trigger + bucket active record 
			assertEquals(4L, getCount.get().longValue());
			
			// Check the message bus - should get a job start notification
			
			assertEquals(1, _num_received.get());						
		}
		
		// 5) Check doesn't trigger when active, even with files in directories .. but also that it doesn't shut the job down if it takes >10s for a job to start
		{
			resetTriggerCheckTimes(trigger_crud); // (since the external triggers
			
			final AnalyticTriggerMessage msg = new AnalyticTriggerMessage(new AnalyticTriggerMessage.AnalyticsTriggerActionMessage());
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish
			Thread.sleep(2000L);
			
			// Check the DB
		
			// external trigger + bucket active record + job active record
			assertEquals(4L, getCount.get().longValue());
			
			// Check the message bus - nothing else received
			
			assertEquals(1, _num_received.get());						
		}
		
		// 5b) I should now receive some started messages back - again should do nothing
		{
			final BucketActionMessage.BucketActionAnalyticJobMessage inner_msg = 
					new BucketActionMessage.BucketActionAnalyticJobMessage(bucket, 
							Arrays.asList(job_to_run_first),
							JobMessageType.starting);

			final AnalyticTriggerMessage msg = new AnalyticTriggerMessage(inner_msg);			
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish			
			waitForData(getCount, 5, true);
			
			// Check the DB
		
			// (ie creates +2, a bucket active record and a job active record)
			assertEquals(5L, getCount.get().longValue());
			
			// Confirm the extra 2 records are as above
			assertEquals(1L, trigger_crud.countObjectsBySpec(
					CrudUtils.allOf(AnalyticTriggerStateBean.class)
						.when(AnalyticTriggerStateBean::bucket_name, bucket.full_name())					
						.when(AnalyticTriggerStateBean::trigger_type, TriggerType.none)
						.withNotPresent(AnalyticTriggerStateBean::job_name)
					).join().intValue());
			assertEquals(1L, trigger_crud.countObjectsBySpec(
					CrudUtils.allOf(AnalyticTriggerStateBean.class)
						.when(AnalyticTriggerStateBean::bucket_name, bucket.full_name())					
						.when(AnalyticTriggerStateBean::trigger_type, TriggerType.none)
						.when(AnalyticTriggerStateBean::job_name, job_to_run_first.name())
					).join().intValue());
			
			// Check the message bus - nothing changed
			
			assertEquals(1, _num_received.get());
		}		
		
		// 5c) Check doesn't trigger when active, even with files in directories .. 
		{
			resetTriggerCheckTimes(trigger_crud); // (since the external triggers are for N minutes time unless a trigger event has occurred)
			
			final AnalyticTriggerMessage msg = new AnalyticTriggerMessage(new AnalyticTriggerMessage.AnalyticsTriggerActionMessage());
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish
			Thread.sleep(2000L);
			waitForData(() -> _num_received.get(), 2, true);
			
			// Check the DB
		
			// external trigger + bucket active record + job active record
			assertEquals(5L, getCount.get().longValue());
			
			// Check the message bus - get a check completion message
			
			assertEquals(2, _num_received.get());						
		}
		
		// 6) OK send first job is complete
		
		{
			final BucketActionMessage.BucketActionAnalyticJobMessage inner_msg = 
					new BucketActionMessage.BucketActionAnalyticJobMessage(bucket, 
							Arrays.asList(job_to_run_first),
							JobMessageType.stopping);
			
			final AnalyticTriggerMessage msg = new AnalyticTriggerMessage(inner_msg);			
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish
			waitForData(getCount, 4, false);
			
			// Check the DB
			
			// external triggers + bucket active record
			assertEquals(4L, getCount.get().longValue());
			
			// Check the message bus - get a start message
			
			assertEquals(2, _num_received.get());									
		}
				
		// 7) Kicks off again next trigger
		{
			final AnalyticTriggerMessage msg = new AnalyticTriggerMessage(new AnalyticTriggerMessage.AnalyticsTriggerActionMessage());
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish
			waitForData(getCount, 5, true);
			waitForData(() -> _num_received.get(), 3, true);
			
			// Check the DB
			
			// external trigger + bucket active record + new job active record
			assertEquals(5L, getCount.get().longValue());
			
			// Check the message bus - get a check completion message
			
			assertEquals(3, _num_received.get());									
		}		
		
		// 8) OK send final job is complete
		
		{
			final BucketActionMessage.BucketActionAnalyticJobMessage inner_msg = 
					new BucketActionMessage.BucketActionAnalyticJobMessage(bucket, 
							Arrays.asList(job_to_run_last),
							JobMessageType.stopping);
			
			final AnalyticTriggerMessage msg = new AnalyticTriggerMessage(inner_msg);			
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish
			waitForData(getCount, 4, false);
			
			// Check the DB
			
			// external trigger + bucket active record 
			assertEquals(4L, getCount.get().longValue());
			
			// Check the message bus - no change
			
			assertEquals(3, _num_received.get());									
		}
				
		// 9) Kicks off again next trigger
		{
			final AnalyticTriggerMessage msg = new AnalyticTriggerMessage(new AnalyticTriggerMessage.AnalyticsTriggerActionMessage());
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish
			waitForData(getCount, 3, true);
			waitForData(() -> _num_received.get(), 4, true);
			
			// Check the DB
			
			// external triggers
			assertEquals(3L, getCount.get().longValue());
			
			// Check the message bus - get a thread stop message
			
			assertEquals(4, _num_received.get());									
		}		
		
		// 10) Finally check the (reset) external triggers - won't fire because even though the file is still present because the bucket dep has reset
		{
			final AnalyticTriggerMessage msg = new AnalyticTriggerMessage(new AnalyticTriggerMessage.AnalyticsTriggerActionMessage());
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish
			Thread.sleep(2000L);
			
			// Check the DB
			
			// external trigger + bucket active record + new job active record
			assertEquals(3L, getCount.get().longValue());
			
			// Check the message bus - get a check completion message
			
			assertEquals(4, _num_received.get());									
		}				
		// Check no malformed buckets
		assertEquals(0L, _num_received_errors.get());
	}

	/////////////////////////////////////////////////////
	/////////////////////////////////////////////////////

	// UTILITIES

	protected void createDirectory(final String root_dir, final String bucket_path, final String suffix, int files_to_create, boolean clear_dir) throws IOException {

		final String dir = root_dir + File.separator + bucket_path + File.separator + suffix;
		final File dir_file = new File(dir);

		if (clear_dir) {
			try {
				FileUtils.deleteDirectory(dir_file);
			}
			catch (Exception e) {} // prob just doesn't exist
		}

		try {
			FileUtils.forceMkdir(dir_file);
		}
		catch (Exception e) { // prob just already exists

		}
		assertTrue("Directory should exist: " + dir_file, dir_file.exists());

		for (int ii = 0; ii < files_to_create; ++ii) {
			final String file = dir + UuidUtils.get().getRandomUuid() + ".json";
			final File file_file = new File(file);
			FileUtils.touch(file_file);
			assertTrue("Newly created file should exist", file_file.exists());
			//System.out.println("Created file in " + dir + ": " + file + " .. " + file_file.lastModified());			
		}
	}

	protected static void insertStatusForBucket(final DataBucketBean bucket) {
		final DataBucketStatusBean status_bean =
				BeanTemplateUtils.build(DataBucketStatusBean.class)
					.with(DataBucketStatusBean::_id, bucket._id())
					.with(DataBucketStatusBean::bucket_path, bucket.full_name())
				.done().get()
				;
		
		_status_crud.storeObject(status_bean, true).join();		
	}
	
	/**
	 * @param bucket_name
	 * @param job_name
	 * @param curr_not_last - true to get curr_run, false to get last_run
	 * @return
	 */
	protected static Long getLastRunTime(final String bucket_name, final Optional<String> job_name, final boolean curr_not_last) {
		return _status_crud.getObjectBySpec(CrudUtils.allOf(DataBucketStatusBean.class).when(DataBucketStatusBean::bucket_path, bucket_name))
						.join()
						.<Long>map(status_bean -> {
							return job_name.map(jn -> 
								Optional.ofNullable(status_bean.analytic_state())
										.map(as -> as.get(jn))
										.map(d -> curr_not_last ? d.curr_run() : d.last_run())
										.map(d -> d.getTime()) 
										.orElse(-1L)
									)
									.orElseGet(() ->
										Optional.ofNullable(status_bean.global_analytic_state())
											.map(d -> curr_not_last ? d.curr_run() : d.last_run())
											.map(d -> d.getTime()) 
											.orElse(-1L)
									)
									;
						})
						.orElse(-1L)
						;
	}
	
	protected void waitForData(Supplier<Long> getCount, long exit_value, boolean ascending) {
		int ii = 0;
		long curr_val = -1;
		for (; ii < 10; ++ii) {
			curr_val = getCount.get();
			if (ascending && (curr_val >= exit_value)) break;
			else if (!ascending && (curr_val <= exit_value)) break;
			try { Thread.sleep(500L); } catch (Exception e) {}
		}
		System.out.println("(Waited " + ii/2 + " (secs) for count=" + curr_val + ")");
	}

	/** Utility to make trigger checks pending
	 */
	protected void resetTriggerCheckTimes(final ICrudService<AnalyticTriggerStateBean> trigger_crud) {
		final UpdateComponent<AnalyticTriggerStateBean> update =
				CrudUtils.update(AnalyticTriggerStateBean.class)
				.set(AnalyticTriggerStateBean::next_check, Date.from(Instant.now().minusSeconds(2L)));

		trigger_crud.updateObjectsBySpec(CrudUtils.allOf(AnalyticTriggerStateBean.class), Optional.of(false), update).join();
	}

	public void printTriggerDatabase()
	{
		List<com.fasterxml.jackson.databind.JsonNode> ll = Optionals.streamOf(_test_crud.getRawService().getObjectsBySpec(CrudUtils.allOf())
				.join().iterator(), true)
				.collect(Collectors.toList())
				;
		System.out.println("DB_Resources = \n" + 
				ll.stream().map(t -> t.toString()).collect(Collectors.joining("\n")));		
	}
}
