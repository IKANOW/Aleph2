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

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_import_manager.analytics.services.AnalyticStateTriggerCheckFactory;
import com.ikanow.aleph2.data_import_manager.analytics.utils.TestAnalyticTriggerCrudUtils;
import com.ikanow.aleph2.data_import_manager.data_model.DataImportConfigurationBean;
import com.ikanow.aleph2.data_import_manager.services.DataImportActorContext;
import com.ikanow.aleph2.data_import_manager.services.GeneralInformationService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean.AnalyticThreadComplexTriggerBean.TriggerType;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.ProcessingTestSpecBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.BucketUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.data_model.utils.UuidUtils;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.distributed_services.services.MockCoreDistributedServices;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerMessage;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerStateBean;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage.BucketActionAnalyticJobMessage.JobMessageType;
import com.ikanow.aleph2.management_db.services.ManagementDbActorContext;
import com.ikanow.aleph2.management_db.utils.ActorUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

public class TestAnalyticsTriggerWorkerActor {

	private static final Logger _logger = LogManager.getLogger();	

	@Inject 
	protected IServiceContext _service_context = null;	
	
	protected ICoreDistributedServices _cds = null;
	protected IManagementDbService _core_mgmt_db = null;
	protected IManagementDbService _under_mgmt_db = null;
	protected ManagementDbActorContext _actor_context = null;
	
	protected static AtomicLong _num_received = new AtomicLong();
	
	protected ActorRef _trigger_worker = null;
	ActorRef _dummy_data_bucket_change_actor = null;
	
	ICrudService<AnalyticTriggerStateBean> _test_crud;
	
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
	
	@SuppressWarnings("deprecation")
	@Before
	public void test_Setup() throws Exception {
		
		if (null != _service_context) {
			return;
		}
		final String temp_dir = System.getProperty("java.io.tmpdir") + File.separator;
		
		// OK we're going to use guice, it was too painful doing this by hand...				
		Config config = ConfigFactory.parseReader(new InputStreamReader(this.getClass().getResourceAsStream("test_data_bucket_change.properties")))
							.withValue("globals.local_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
							.withValue("globals.local_cached_jar_dir", ConfigValueFactory.fromAnyRef(temp_dir))
							.withValue("globals.distributed_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
							.withValue("globals.local_yarn_config_dir", ConfigValueFactory.fromAnyRef(temp_dir));
		
		Injector app_injector = ModuleUtils.createTestInjector(Arrays.asList(), Optional.of(config));	
		app_injector.injectMembers(this);
		
		_cds = _service_context.getService(ICoreDistributedServices.class, Optional.empty()).get();
		MockCoreDistributedServices mcds = (MockCoreDistributedServices) _cds;
		mcds.setApplicationName("DataImportManager");
		
		new ManagementDbActorContext(_service_context, true);		
		_actor_context = ManagementDbActorContext.get();
		
		_core_mgmt_db = _service_context.getCoreManagementDbService();		
		_under_mgmt_db = _service_context.getService(IManagementDbService.class, Optional.empty()).get();

		// need to create data import actor separately:
		@SuppressWarnings("unused")
		final DataImportActorContext singleton = new DataImportActorContext(_service_context, new GeneralInformationService(), 
				BeanTemplateUtils.build(DataImportConfigurationBean.class).done().get(),
				new AnalyticStateTriggerCheckFactory(_service_context));
		
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
	
	@After
	public void tidyUpActor() {
		if (null != _trigger_worker) {
			_trigger_worker.tell(akka.actor.PoisonPill.getInstance(), _trigger_worker);
		}
		//(don't need to delete ZK, it only runs once per job)
		
		//Not sure about the bus:
		ManagementDbActorContext.get().getAnalyticsMessageBus().unsubscribe(_dummy_data_bucket_change_actor);
	}
		
	@Test
	public void test_bucketLifecycle() throws InterruptedException {
		
		// Going to create a bucket, update it, and then suspend it
		// Note not much validation here, since the actual triggers etc are created by the utils functions, which are tested separately
		
		// Create the usual "manual trigger" bucket
		
		final DataBucketBean manual_trigger_bucket = TestAnalyticTriggerCrudUtils.buildBucket("/test/trigger", true);
		
		final ICrudService<AnalyticTriggerStateBean> trigger_crud = 
				_service_context.getCoreManagementDbService().readOnlyVersion().getAnalyticBucketTriggerState(AnalyticTriggerStateBean.class);
						
		// 1) Send a message to the worker to fill in that bucket
		{
			final BucketActionMessage.NewBucketActionMessage msg = 
					new BucketActionMessage.NewBucketActionMessage(manual_trigger_bucket, false);
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish			
			Thread.sleep(500L);
			
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
			Thread.sleep(500L);
			
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
			Thread.sleep(500L);
			
			// Check the DB
		
			assertEquals(0L, trigger_crud.countObjects().join().intValue());
		}
	}
	
	@Test
	public void test_bucketTestLifecycle() throws InterruptedException {
		
		// Going to create a bucket, update it, and then suspend it
		// Note not much validation here, since the actual triggers etc are created by the utils functions, which are tested separately
		
		// Create the usual "manual trigger" bucket
		
		final DataBucketBean manual_trigger_bucket_original = TestAnalyticTriggerCrudUtils.buildBucket("/test/trigger", true);
		
		final DataBucketBean manual_trigger_bucket = BucketUtils.convertDataBucketBeanToTest(manual_trigger_bucket_original, "alex"); 
		
		final ICrudService<AnalyticTriggerStateBean> trigger_crud = 
				_service_context.getCoreManagementDbService().readOnlyVersion().getAnalyticBucketTriggerState(AnalyticTriggerStateBean.class);
						
		// 1) Send a message to the worker to fill in that bucket
		{
			final BucketActionMessage.TestBucketActionMessage msg = 
					new BucketActionMessage.TestBucketActionMessage(manual_trigger_bucket, BeanTemplateUtils.build(ProcessingTestSpecBean.class).done().get());
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish			
			Thread.sleep(500L);
			
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
			Thread.sleep(500L);
			
			// Check the DB
		
			assertEquals(0L, trigger_crud.countObjects().join().intValue());
		}
		
	}
	
	@Test
	public void test_jobTriggerScenario() throws InterruptedException, IOException {
		
		// Tests a manual bucket that has inter-job dependencies		
		final String json_bucket = Resources.toString(Resources.getResource("com/ikanow/aleph2/data_import_manager/analytics/actors/simple_job_deps_bucket.json"), Charsets.UTF_8);		
		final DataBucketBean job_dep_bucket = BeanTemplateUtils.from(json_bucket, DataBucketBean.class).get();
		
		// (have to save the bucket to make trigger checks work correctly)
		_service_context.getService(IManagementDbService.class, Optional.empty()).get().getDataBucketStore().storeObject(job_dep_bucket, true).join();
		
		final ICrudService<AnalyticTriggerStateBean> trigger_crud = 
				_service_context.getCoreManagementDbService().readOnlyVersion().getAnalyticBucketTriggerState(AnalyticTriggerStateBean.class);
		
		// 1) Send a message to the worker to fill in that bucket
		{
			final BucketActionMessage.NewBucketActionMessage msg = 
					new BucketActionMessage.NewBucketActionMessage(job_dep_bucket, false);
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish			
			Thread.sleep(500L);
			
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
			Thread.sleep(500L);
			
			// Check the DB
		
			// (ie no active records)
			assertEquals(3L, trigger_crud.countObjects().join().intValue());
			
			// Check the message bus - nothing yet!
			
			assertEquals(0, _num_received.get());			
		}		
		
		// 3a) Let the worker now that job1 has started (which should also launch the bucket)
		{
			final BucketActionMessage.BucketActionAnalyticJobMessage inner_msg = 
					new BucketActionMessage.BucketActionAnalyticJobMessage(job_dep_bucket, 
							job_dep_bucket.analytic_thread().jobs().stream().filter(j -> j.name().equals("initial_phase")).collect(Collectors.toList()),
							JobMessageType.starting);

			final AnalyticTriggerMessage msg = new AnalyticTriggerMessage(inner_msg);			
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish			
			Thread.sleep(500L);
			
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
					new BucketActionMessage.BucketActionAnalyticJobMessage(job_dep_bucket, 
							job_dep_bucket.analytic_thread().jobs().stream().filter(j -> j.name().equals("initial_phase")).collect(Collectors.toList()),
							JobMessageType.stopping);
			
			final AnalyticTriggerMessage msg = new AnalyticTriggerMessage(inner_msg);			
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish			
			Thread.sleep(500L);
			
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
			Thread.sleep(500L);

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
					new BucketActionMessage.BucketActionAnalyticJobMessage(job_dep_bucket, 
							job_dep_bucket.analytic_thread().jobs().stream().filter(j -> j.name().equals("next_phase")).collect(Collectors.toList()),
							JobMessageType.stopping);
			
			final AnalyticTriggerMessage msg = new AnalyticTriggerMessage(inner_msg);			
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish			
			Thread.sleep(500L);
			
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
			Thread.sleep(500L);

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
					new BucketActionMessage.BucketActionAnalyticJobMessage(job_dep_bucket, 
							job_dep_bucket.analytic_thread().jobs().stream().filter(j -> j.name().equals("final_phase")).collect(Collectors.toList()),
							JobMessageType.stopping);
			
			final AnalyticTriggerMessage msg = new AnalyticTriggerMessage(inner_msg);			
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish			
			Thread.sleep(500L);
			
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
			Thread.sleep(500L);

			// Check the DB
		
			// (bucket active still present, now "next_phase" has started)
			assertEquals(3L, trigger_crud.countObjects().join().intValue());
			
			// Check the message bus - should have received a stop message for the bucket
			
			assertEquals(3, _num_received.get()); 
		}		
	}
	
	//TODO (ALEPH-12): more tests
	
	/////////////////////////////////////////////////////
	
	// UTILITIES
	
	/** Utility to make trigger checks pending
	 */
	protected void resetTriggerCheckTimes(final ICrudService<AnalyticTriggerStateBean> trigger_crud) {
		final UpdateComponent<AnalyticTriggerStateBean> update =
				CrudUtils.update(AnalyticTriggerStateBean.class)
					.set(AnalyticTriggerStateBean::next_check, Date.from(Instant.now().minusSeconds(2L)));
		
		trigger_crud.updateObjectsBySpec(CrudUtils.allOf(AnalyticTriggerStateBean.class), Optional.of(false), update).join();
	}
}
