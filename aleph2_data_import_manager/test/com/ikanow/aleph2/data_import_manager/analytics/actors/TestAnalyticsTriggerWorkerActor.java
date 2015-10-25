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
package com.ikanow.aleph2.data_import_manager.analytics.actors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Date;
import java.util.HashSet;
import java.util.Optional;
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
import com.ikanow.aleph2.data_import_manager.analytics.utils.TestAnalyticTriggerCrudUtils;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean.AnalyticThreadComplexTriggerBean.TriggerType;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.ProcessingTestSpecBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.BucketUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent;
import com.ikanow.aleph2.data_model.utils.UuidUtils;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerMessage;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerStateBean;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage.BucketActionAnalyticJobMessage.JobMessageType;
import com.ikanow.aleph2.management_db.services.ManagementDbActorContext;
import com.ikanow.aleph2.management_db.utils.ActorUtils;

public class TestAnalyticsTriggerWorkerActor extends TestAnalyticsTriggerWorkerCommon {
	private static final Logger _logger = LogManager.getLogger();	

	// This one always accepts, but then refuses when it comes down to it...
	public static class TestActor extends UntypedActor {
		public TestActor() {
		}
		@Override
		public void onReceive(Object arg0) throws Exception {
			if (arg0 instanceof BucketActionMessage.BucketActionOfferMessage) {
				_logger.info("Accept OFFER");
				this.sender().tell(new BucketActionReplyMessage.BucketActionWillAcceptMessage("dummy_data_bucket_change_actor"), this.self());
			}
			else {
				_logger.info("Received message! " + arg0.getClass().toString());
				_num_received.incrementAndGet();
			}
		}		
	};
	
	protected static ActorRef _trigger_worker = null;
	protected static ActorRef _dummy_data_bucket_change_actor = null;	
	protected static ICrudService<AnalyticTriggerStateBean> _test_crud;

	@Before
	@Override
	public void test_Setup() throws Exception {
		if (null != _service_context) {
			return;
		}
		super.test_Setup();
				
		_trigger_worker = _actor_context.getActorSystem().actorOf(
				Props.create(com.ikanow.aleph2.data_import_manager.analytics.actors.AnalyticsTriggerWorkerActor.class),
				UuidUtils.get().getRandomUuid()
				//hostname + ActorNameUtils.ANALYTICS_TRIGGER_WORKER_SUFFIX
				);

		_test_crud = _service_context.getService(IManagementDbService.class, Optional.empty()).get().getAnalyticBucketTriggerState(AnalyticTriggerStateBean.class);
		_test_crud.deleteDatastore().get();
		
		final String dummy_data_bucket_change_actor = UuidUtils.get().getRandomUuid();
		_dummy_data_bucket_change_actor = ManagementDbActorContext.get().getActorSystem().actorOf(Props.create(TestActor.class), dummy_data_bucket_change_actor);
		ManagementDbActorContext.get().getAnalyticsMessageBus().subscribe(_dummy_data_bucket_change_actor, ActorUtils.BUCKET_ANALYTICS_ZOOKEEPER);
		ManagementDbActorContext.get().getDistributedServices()
			.getCuratorFramework().create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
			.forPath(ActorUtils.BUCKET_ANALYTICS_ZOOKEEPER + "/" +  "dummy_data_bucket_change_actor");				
	}
	
	
	@Test
	public void test_bucketLifecycle() throws InterruptedException {
		System.out.println("Starting test_bucketLifecycle");
		
		// Going to create a bucket, update it, and then suspend it
		// Note not much validation here, since the actual triggers etc are created by the utils functions, which are tested separately
		
		// Create the usual "manual trigger" bucket
		
		final DataBucketBean manual_trigger_bucket = TestAnalyticTriggerCrudUtils.buildBucket("/test/trigger", true);
		
		final ICrudService<AnalyticTriggerStateBean> trigger_crud = 
				_service_context.getCoreManagementDbService().readOnlyVersion().getAnalyticBucketTriggerState(AnalyticTriggerStateBean.class);		
		trigger_crud.deleteDatastore().join();
						
		// 1) Send a message to the worker to fill in that bucket
		{
			final BucketActionMessage.NewBucketActionMessage msg = 
					new BucketActionMessage.NewBucketActionMessage(manual_trigger_bucket, false);
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish
			waitForData(trigger_crud, 7, true);
			
			// Check the DB
		
			assertEquals(7L, trigger_crud.countObjects().join().intValue());
			// Check they aren't suspended:
			assertEquals(7L, trigger_crud.countObjectsBySpec(
					CrudUtils.allOf(AnalyticTriggerStateBean.class)
						.when(AnalyticTriggerStateBean::is_bucket_suspended, false)
					).join().intValue());
		}
		
		// 2) Update in suspended mode
		{
			final BucketActionMessage.UpdateBucketActionMessage msg = 
					new BucketActionMessage.UpdateBucketActionMessage(manual_trigger_bucket, false, manual_trigger_bucket, new HashSet<String>());
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish			
			waitForData(trigger_crud, 7, true);
			
			// Check the DB
		
			assertEquals(7L, trigger_crud.countObjects().join().intValue());
			// Check they are suspended:
			assertEquals(7L, trigger_crud.countObjectsBySpec(
					CrudUtils.allOf(AnalyticTriggerStateBean.class)
						.when(AnalyticTriggerStateBean::is_bucket_suspended, true)
					).join().intValue());			
		}
		
		// 3) Delete
		{
			final BucketActionMessage.DeleteBucketActionMessage msg = 
					new BucketActionMessage.DeleteBucketActionMessage(manual_trigger_bucket, new HashSet<String>());
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish			
			waitForData(trigger_crud, 0, false);
			
			// Check the DB
		
			assertEquals(0L, trigger_crud.countObjects().join().intValue());
		}
	}
	
	@Test
	public void test_bucketTestLifecycle() throws InterruptedException {
		System.out.println("Starting test_bucketTestLifecycle");
		
		// Going to create a bucket, update it, and then suspend it
		// Note not much validation here, since the actual triggers etc are created by the utils functions, which are tested separately
		
		// Create the usual "manual trigger" bucket
		
		final DataBucketBean manual_trigger_bucket_original = TestAnalyticTriggerCrudUtils.buildBucket("/test/trigger", true);
		
		final DataBucketBean manual_trigger_bucket = BucketUtils.convertDataBucketBeanToTest(manual_trigger_bucket_original, "alex"); 
		
		final ICrudService<AnalyticTriggerStateBean> trigger_crud = 
				_service_context.getCoreManagementDbService().readOnlyVersion().getAnalyticBucketTriggerState(AnalyticTriggerStateBean.class);
		trigger_crud.deleteDatastore().join();

		// 1) Send a message to the worker to fill in that bucket
		{
			final BucketActionMessage.TestBucketActionMessage msg = 
					new BucketActionMessage.TestBucketActionMessage(manual_trigger_bucket, BeanTemplateUtils.build(ProcessingTestSpecBean.class).done().get());
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish			
			waitForData(trigger_crud, 7, true);
			
			// Check the DB
		
			assertEquals(7L, trigger_crud.countObjects().join().intValue());
			// Check they aren't suspended:
			assertEquals(7L, trigger_crud.countObjectsBySpec(
					CrudUtils.allOf(AnalyticTriggerStateBean.class)
						.when(AnalyticTriggerStateBean::is_bucket_suspended, false)
					).join().intValue());
		}
		
		// 2) Update in suspended mode (will delete because it's a test)
		{
			final BucketActionMessage.UpdateBucketActionMessage msg = 
					new BucketActionMessage.UpdateBucketActionMessage(manual_trigger_bucket, false, manual_trigger_bucket, new HashSet<String>());
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish			
			waitForData(trigger_crud, 0, false);
			
			// Check the DB
		
			assertEquals(0L, trigger_crud.countObjects().join().intValue());
		}
		
	}
	
	@Test
	public void test_jobTriggerScenario() throws InterruptedException, IOException {
		System.out.println("Starting test_jobTriggerScenario");
		
		// Tests a manual bucket that has inter-job dependencies		
		final String json_bucket = Resources.toString(Resources.getResource("com/ikanow/aleph2/data_import_manager/analytics/actors/simple_job_deps_bucket.json"), Charsets.UTF_8);		
		final DataBucketBean bucket = BeanTemplateUtils.from(json_bucket, DataBucketBean.class).get();
		
		// (have to save the bucket to make trigger checks work correctly)
		_service_context.getService(IManagementDbService.class, Optional.empty()).get().getDataBucketStore().storeObject(bucket, true).join();
		
		final ICrudService<AnalyticTriggerStateBean> trigger_crud = 
				_service_context.getCoreManagementDbService().readOnlyVersion().getAnalyticBucketTriggerState(AnalyticTriggerStateBean.class);
		trigger_crud.deleteDatastore().join();
		
		_num_received.set(0L);
		
		// 1) Send a message to the worker to fill in that bucket
		{
			final BucketActionMessage.NewBucketActionMessage msg = 
					new BucketActionMessage.NewBucketActionMessage(bucket, false);
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish			
			waitForData(trigger_crud, 3, true);
			
			// Check the DB
		
			assertEquals(3L, trigger_crud.countObjects().join().intValue());
			// Check they aren't suspended:
			assertEquals(3L, trigger_crud.countObjectsBySpec(
					CrudUtils.allOf(AnalyticTriggerStateBean.class)
						.when(AnalyticTriggerStateBean::is_bucket_suspended, false)
					).join().intValue());
			
			// Check the message bus - nothing yet!
			
			assertEquals(0, _num_received.get());
		}
		
		// 2) Perform a trigger check, make sure that nothing has activated
		{
			final AnalyticTriggerMessage msg = new AnalyticTriggerMessage(new AnalyticTriggerMessage.AnalyticsTriggerActionMessage());
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish			
			Thread.sleep(1000L);
			
			// Check the DB
		
			// (ie no active records)
			assertEquals(3L, trigger_crud.countObjects().join().intValue());
			
			// Check the message bus - nothing yet!
			
			assertEquals(0, _num_received.get());			
		}		
		
		// 3a) Let the worker now that job1 has started (which should also launch the bucket)
		{
			final BucketActionMessage.BucketActionAnalyticJobMessage inner_msg = 
					new BucketActionMessage.BucketActionAnalyticJobMessage(bucket, 
							bucket.analytic_thread().jobs().stream().filter(j -> j.name().equals("initial_phase")).collect(Collectors.toList()),
							JobMessageType.starting);

			final AnalyticTriggerMessage msg = new AnalyticTriggerMessage(inner_msg);			
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish			
			waitForData(trigger_crud, 5, true);
			
			// Check the DB
		
			// (ie creates a bucket active record and a job active record)
			assertEquals(5L, trigger_crud.countObjects().join().intValue());
			
			// Confirm the extra 2 records are as above
			assertEquals(1L, trigger_crud.countObjectsBySpec(
					CrudUtils.allOf(AnalyticTriggerStateBean.class)
						.when(AnalyticTriggerStateBean::trigger_type, TriggerType.none)
						.withNotPresent(AnalyticTriggerStateBean::job_name)
					).join().intValue());
			assertEquals(1L, trigger_crud.countObjectsBySpec(
					CrudUtils.allOf(AnalyticTriggerStateBean.class)
						.when(AnalyticTriggerStateBean::trigger_type, TriggerType.none)
						.when(AnalyticTriggerStateBean::job_name, "initial_phase")
					).join().intValue());
			
			// Check the message bus - nothing yet!
			
			assertEquals(0, _num_received.get());
		}
		
		// 3b) Send a job completion message
		{
			final BucketActionMessage.BucketActionAnalyticJobMessage inner_msg = 
					new BucketActionMessage.BucketActionAnalyticJobMessage(bucket, 
							bucket.analytic_thread().jobs().stream().filter(j -> j.name().equals("initial_phase")).collect(Collectors.toList()),
							JobMessageType.stopping);
			
			final AnalyticTriggerMessage msg = new AnalyticTriggerMessage(inner_msg);			
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish			
			waitForData(trigger_crud, 4, false);
			
			// Check the DB
			
			// (ie active job is removed, bucket remains)
			assertEquals(4L, trigger_crud.countObjects().join().intValue());
			
			// Confirm the extra record is as above
			assertEquals(1L, trigger_crud.countObjectsBySpec(
					CrudUtils.allOf(AnalyticTriggerStateBean.class)
						.when(AnalyticTriggerStateBean::trigger_type, TriggerType.none)
						.withNotPresent(AnalyticTriggerStateBean::job_name)
					).join().intValue());
			
			// Check the message bus - nothing yet!
			
			assertEquals(0, _num_received.get());
		}
		
		
		// 4) Perform a trigger check, make sure that only the right job ("next_phase") has started
		{
			final AnalyticTriggerMessage msg = new AnalyticTriggerMessage(new AnalyticTriggerMessage.AnalyticsTriggerActionMessage());
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish			
			waitForData(trigger_crud, 5, true);

			// Check the DB
		
			// (bucket active still present, now "next_phase" has started)
			assertEquals(5L, trigger_crud.countObjects().join().intValue());
			
			// Confirm the extra 2 records are as above
			assertEquals(1L, trigger_crud.countObjectsBySpec(
					CrudUtils.allOf(AnalyticTriggerStateBean.class)
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
		}		
		
		// 5) Send a job completion message
		{
			final BucketActionMessage.BucketActionAnalyticJobMessage inner_msg = 
					new BucketActionMessage.BucketActionAnalyticJobMessage(bucket, 
							bucket.analytic_thread().jobs().stream().filter(j -> j.name().equals("next_phase")).collect(Collectors.toList()),
							JobMessageType.stopping);
			
			final AnalyticTriggerMessage msg = new AnalyticTriggerMessage(inner_msg);			
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish			
			waitForData(trigger_crud, 4, false);
			
			// Check the DB
			
			// (ie active job is removed, bucket remains)
			assertEquals(4L, trigger_crud.countObjects().join().intValue());
			
			// Confirm the extra record is as above
			assertEquals(1L, trigger_crud.countObjectsBySpec(
					CrudUtils.allOf(AnalyticTriggerStateBean.class)
						.when(AnalyticTriggerStateBean::trigger_type, TriggerType.none)
						.withNotPresent(AnalyticTriggerStateBean::job_name)
					).join().intValue());
			
			// Check the message bus - no change
			
			assertEquals(1, _num_received.get());
		}
		
		// 6) Perform a trigger check, make sure that only the right job ("final_phase") has started 
		{
			final AnalyticTriggerMessage msg = new AnalyticTriggerMessage(new AnalyticTriggerMessage.AnalyticsTriggerActionMessage());
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish			
			waitForData(trigger_crud, 5, true);

			// Check the DB
		
			// (bucket active still present, now "next_phase" has started)
			assertEquals(5L, trigger_crud.countObjects().join().intValue());
			
			// Confirm the extra 2 records are as above
			assertEquals(1L, trigger_crud.countObjectsBySpec(
					CrudUtils.allOf(AnalyticTriggerStateBean.class)
						.when(AnalyticTriggerStateBean::trigger_type, TriggerType.none)
						.withNotPresent(AnalyticTriggerStateBean::job_name)
					).join().intValue());			
			assertEquals(1L, trigger_crud.countObjectsBySpec(
					CrudUtils.allOf(AnalyticTriggerStateBean.class)
						.when(AnalyticTriggerStateBean::trigger_type, TriggerType.none)
						.when(AnalyticTriggerStateBean::job_name, "final_phase")
					).join().intValue());
			
			// Check the message bus - should have received a start message for the triggered job
			
			assertEquals(2, _num_received.get()); //+1
		}		
		
		// 7) Stop the final job
		{
			final BucketActionMessage.BucketActionAnalyticJobMessage inner_msg = 
					new BucketActionMessage.BucketActionAnalyticJobMessage(bucket, 
							bucket.analytic_thread().jobs().stream().filter(j -> j.name().equals("final_phase")).collect(Collectors.toList()),
							JobMessageType.stopping);
			
			final AnalyticTriggerMessage msg = new AnalyticTriggerMessage(inner_msg);			
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish			
			waitForData(trigger_crud, 4, false);
			
			// Check the DB
			
			// (ie active job is removed, bucket remains)
			assertEquals(4L, trigger_crud.countObjects().join().intValue());
			
			// Confirm the extra record is as above
			assertEquals(1L, trigger_crud.countObjectsBySpec(
					CrudUtils.allOf(AnalyticTriggerStateBean.class)
						.when(AnalyticTriggerStateBean::trigger_type, TriggerType.none)
						.withNotPresent(AnalyticTriggerStateBean::job_name)
					).join().intValue());
			
			// Check the message bus - no change
			
			assertEquals(2, _num_received.get());
		}
		
		// 8) Trigger - should complete the bucket
		{
			final AnalyticTriggerMessage msg = new AnalyticTriggerMessage(new AnalyticTriggerMessage.AnalyticsTriggerActionMessage());
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish			
			waitForData(trigger_crud, 3, false);
			
			// Check the DB
		
			// (all active records removed)
			assertEquals(3L, trigger_crud.countObjects().join().intValue());
			
			// Check the message bus - should have received a stop message for the bucket
			
			assertEquals(3, _num_received.get()); 
		}		
	}
	
	@Test
	public void test_enrichment_analyticForm() throws IOException, InterruptedException {
		final String json_bucket = Resources.toString(Resources.getResource("com/ikanow/aleph2/data_import_manager/analytics/actors/real_test_case_batch_2.json"), Charsets.UTF_8);		
		final DataBucketBean enrichment_bucket = BeanTemplateUtils.from(json_bucket, DataBucketBean.class).get();
		
		test_enrichment_common(enrichment_bucket);
	}
	
	@Test
	public void test_enrichment_enrichmentForm() {
		//TODO (ALEPH-12) - create a batch enrichment version of the bucket
	}
	
	public void test_enrichment_common(final DataBucketBean bucket) throws IOException, InterruptedException {

		//setup
		// (have to save the bucket to make trigger checks work correctly)
		_service_context.getService(IManagementDbService.class, Optional.empty()).get().getDataBucketStore().storeObject(bucket, true).join();
		
		final ICrudService<AnalyticTriggerStateBean> trigger_crud = 
				_service_context.getCoreManagementDbService().readOnlyVersion().getAnalyticBucketTriggerState(AnalyticTriggerStateBean.class);
		trigger_crud.deleteDatastore().join();
		
		_num_received.set(0L);
		
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
			waitForData(trigger_crud, 1, true);
			
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
			Thread.sleep(1000L);
			
			// Check the DB
		
			// (ie no active records)
			assertEquals(1L, trigger_crud.countObjects().join().intValue());
			
			// Check the message bus - nothing yet!
			
			assertEquals(0, _num_received.get());			
		}		
		
		// 3a) Add files to input directory, checks triggers (external triggers are slower so shouldn't trigger anything)
		{
			createDirectory(root_dir, bucket.full_name(), suffix, 1, false);
			
			final AnalyticTriggerMessage msg = new AnalyticTriggerMessage(new AnalyticTriggerMessage.AnalyticsTriggerActionMessage());
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish
			Thread.sleep(1000L);
			
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
			Thread.sleep(1000L);
			
			// Check the DB
		
			// external trigger + bucket active record + job active record
			assertEquals(1L, trigger_crud.countObjects().join().intValue());
			
			// Check the message bus - should get a job start notification
			
			assertEquals(1, _num_received.get());						
		}
		
		// 4) Check doesn't trigger when active, even with directories
		
		// 5) De-active
		
		// 6) Check triggers again
		
		//TODO (ALEPH-12)		
	}
	
	//TODO (ALEPH-12): more tests (bucket completion + file trigger)
	
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
	
	protected void waitForData(final ICrudService<AnalyticTriggerStateBean> trigger_crud, long exit_value, boolean ascending) {
		int ii = 0;
		long curr_val = -1;
		for (; ii < 10; ++ii) {
			try { Thread.sleep(500L); } catch (Exception e) {}
			curr_val = trigger_crud.countObjects().join();
			if (ascending && (curr_val >= exit_value)) break;
			else if (!ascending && (curr_val <= exit_value)) break;
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
}
