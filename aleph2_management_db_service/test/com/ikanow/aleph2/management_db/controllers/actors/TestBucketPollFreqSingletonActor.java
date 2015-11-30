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

import static org.junit.Assert.*;

import java.io.File;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.data_import.HarvestControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean.MasterEnrichmentType;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.data_model.utils.UuidUtils;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.distributed_services.services.MockCoreDistributedServices;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage;
import com.ikanow.aleph2.management_db.services.ManagementDbActorContext;
import com.ikanow.aleph2.management_db.utils.ActorUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

public class TestBucketPollFreqSingletonActor {
	private static final Logger _logger = LogManager.getLogger();	

	@Inject 
	protected IServiceContext _service_context = null;	
	
	protected ICoreDistributedServices _cds = null;
	protected IManagementDbService _core_mgmt_db = null;
	protected ManagementDbActorContext _actor_context = null;
	
	// This one always accepts, and returns a message
	public static class TestActor_Accepter extends UntypedActor {
		public static long num_accepted_messages = 0;
		public TestActor_Accepter(String uuid) {
			this.uuid = uuid;
		}
		private final String uuid;
		@Override
		public void onReceive(Object arg0) throws Exception {
			if (arg0 instanceof BucketActionMessage.BucketActionOfferMessage) {
				_logger.info("Accept OFFER from: " + uuid);
				this.sender().tell(new BucketActionReplyMessage.BucketActionWillAcceptMessage(uuid), this.self());
			}
			else {
				_logger.info("Accept MESSAGE from: " + uuid);
				num_accepted_messages++;
				this.sender().tell(
						new BucketActionReplyMessage.BucketActionHandlerMessage(uuid, 
								new BasicMessageBean(
										new Date(),
										true,
										uuid + "replaceme", // (this gets replaced by the bucket)
										arg0.getClass().getSimpleName(),
										null,
										"handled",
										null									
										)),
						this.self());
			}
		}		
	}
	
	@SuppressWarnings("deprecation")
	@Before
	public void testSetup() throws Exception {
		//WARN if tests run concurrently this is going to break
		TestActor_Accepter.num_accepted_messages = 0; //reset the actor messages before each test
		
		if (null != _service_context) {
			return;
		}
		final String temp_dir = System.getProperty("java.io.tmpdir") + File.separator;
		
		ManagementDbActorContext.unsetSingleton();
		
		// OK we're going to use guice, it was too painful doing this by hand...				
		Config config = ConfigFactory.parseReader(new InputStreamReader(this.getClass().getResourceAsStream("actor_test.properties")))
							.withValue("globals.local_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
							.withValue("globals.local_cached_jar_dir", ConfigValueFactory.fromAnyRef(temp_dir))
							.withValue("globals.distributed_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
							.withValue("globals.local_yarn_config_dir", ConfigValueFactory.fromAnyRef(temp_dir));
		
		Injector app_injector = ModuleUtils.createTestInjector(Arrays.asList(), Optional.of(config));	
		app_injector.injectMembers(this);
		
		_cds = _service_context.getService(ICoreDistributedServices.class, Optional.empty()).get();
		MockCoreDistributedServices mcds = (MockCoreDistributedServices) _cds;
		mcds.setApplicationName("DataImportManager");
		
		_core_mgmt_db = _service_context.getCoreManagementDbService();
		
		//(this has to happen after the call to _service_context.getCoreManagementDbService() - bizarrely the actor context is not set before that?!)
		_actor_context = ManagementDbActorContext.get();		
		
		Thread.sleep(100L); // (since we're in test mode, give the injectors a few ms to sort themselves out - seems like a _very_ intemittent error can occur otherwise?)   
	}
	
	/**
	 * Puts items on the test queue and checks that they have been
	 * picked up by the actor appropriately.
	 * @throws Exception 
	 */
	@Test
	public void test_bucketPollFreqSingletonActor() throws Exception {	
		// Setup: register an accepting actor to listen:
		_logger.debug("ABOUT TO ADD A NEW ACTOR");		
		insertActor(TestActor_Accepter.class);
		assertEquals("No messages should be accepted yet", 0, TestActor_Accepter.num_accepted_messages);
		
		@SuppressWarnings("unchecked")
		final ICrudService<DataBucketStatusBean> underlying_crud_status = (
				ICrudService<DataBucketStatusBean>) this._core_mgmt_db.getDataBucketStatusStore().getUnderlyingPlatformDriver(ICrudService.class, Optional.empty()).get();				
		@SuppressWarnings("unchecked")
		final ICrudService<DataBucketBean> underlying_crud = (
				ICrudService<DataBucketBean>) this._core_mgmt_db.getDataBucketStore().getUnderlyingPlatformDriver(ICrudService.class, Optional.empty()).get();
		underlying_crud.deleteDatastore().get();
		underlying_crud_status.deleteDatastore().get();
		assertEquals(0, underlying_crud.countObjects().get().intValue());
		assertEquals(0, underlying_crud_status.countObjects().get().intValue());	
		
		//create a bucket that has a poll freq	
		final DataBucketBean bucket_poll_now = createBucket("5 min");		
		//create another that doesn't have one until later
		final DataBucketBean bucket_poll_later = createBucket("7 min");
		//create another that doesn't have a poll frequency
		final DataBucketBean bucket_no_poll = createBucket(null);
		
		//put it in the datastore		
		storeBucketAndStatus(bucket_poll_now, true, Optional.of(new Date()), underlying_crud_status, underlying_crud);
		storeBucketAndStatus(bucket_poll_later, true, Optional.of(new Date(System.currentTimeMillis() + 3600000)), underlying_crud_status, underlying_crud);
		storeBucketAndStatus(bucket_no_poll, true, Optional.empty(), underlying_crud_status, underlying_crud);
		final DataBucketStatusBean bucket_poll_now_status = underlying_crud_status.getObjectById(bucket_poll_now._id()).get().get();
		final DataBucketStatusBean bucket_poll_later_status = underlying_crud_status.getObjectById(bucket_poll_later._id()).get().get();
		
		//actor checks every second, give it long enough to hit a cycle
		Thread.sleep(5000);
		
		//get the (hopefully) updated bucket		
		final DataBucketStatusBean bucket_poll_now_updated = underlying_crud_status.getObjectById(bucket_poll_now._id()).get().get();
		final DataBucketStatusBean bucket_poll_later_updated = underlying_crud_status.getObjectById(bucket_poll_later._id()).get().get();
		final DataBucketStatusBean bucket_no_poll_updated = underlying_crud_status.getObjectById(bucket_no_poll._id()).get().get();
		//make sure the actor 
		//1. updates freq (means it received the messaage)
		assertTrue("Next poll time should have been greater than what we set poll_time to", bucket_poll_now_updated.next_poll_date().getTime() > bucket_poll_now_status.next_poll_date().getTime());
		assertTrue("Poll later time should not have been updated", bucket_poll_later_updated.next_poll_date().getTime() == bucket_poll_later_status.next_poll_date().getTime());
		assertTrue("Poll never time should not have been set", bucket_no_poll_updated.next_poll_date() == null );
		
		//2. tries to send out poll messsage	
		assertEquals("Actor should have received 1 poll request and accepted it", 1, TestActor_Accepter.num_accepted_messages);
	}
	
	@Test
	public void test_suspend() throws Exception {
		_logger.debug("ABOUT TO ADD A NEW ACTOR");				
		insertActor(TestActor_Accepter.class);
		assertEquals("No messages should be accepted yet", 0, TestActor_Accepter.num_accepted_messages);		
		
		@SuppressWarnings("unchecked")
		final ICrudService<DataBucketStatusBean> underlying_crud_status = (
				ICrudService<DataBucketStatusBean>) this._core_mgmt_db.getDataBucketStatusStore().getUnderlyingPlatformDriver(ICrudService.class, Optional.empty()).get();				
		@SuppressWarnings("unchecked")
		final ICrudService<DataBucketBean> underlying_crud = (
				ICrudService<DataBucketBean>) this._core_mgmt_db.getDataBucketStore().getUnderlyingPlatformDriver(ICrudService.class, Optional.empty()).get();
		underlying_crud.deleteDatastore().get();
		underlying_crud_status.deleteDatastore().get();
		assertEquals(0, underlying_crud.countObjects().get().intValue());
		assertEquals(0, underlying_crud_status.countObjects().get().intValue());
		
		//1. insert a bucket, ping pollFreq, see bucket respond
		final DataBucketBean bucket_poll_now = createBucket("5 min");
		
		//put it in the datastore
				
		storeBucketAndStatus(bucket_poll_now, true, Optional.of(new Date()), underlying_crud_status, underlying_crud);
		final DataBucketStatusBean bucket_poll_now_status = underlying_crud_status.getObjectById(bucket_poll_now._id()).get().get();
		assertEquals(1, underlying_crud.countObjects().get().intValue());
		assertEquals(1, underlying_crud_status.countObjects().get().intValue());
		//actor checks every second, give it long enough to hit a cycle
		Thread.sleep(5000);
		//get the (hopefully) updated bucket		
		final DataBucketStatusBean bucket_poll_now_updated = underlying_crud_status.getObjectById(bucket_poll_now._id()).get().get();
		assertTrue("Next poll time should have been greater than what we set poll_time to", bucket_poll_now_updated.next_poll_date().getTime() > bucket_poll_now_status.next_poll_date().getTime());
		assertEquals("Actor should have received 1 poll request and accepted it", 1, TestActor_Accepter.num_accepted_messages);
		
		//2a. suspend bucket
		_logger.debug("SUSPENDING BUCKET FOR POLL TEST");
		suspendBucket(bucket_poll_now, underlying_crud_status);
		//2b. set poll time to before now again
		_logger.debug("SETTING BUCKETS NEXT POLL TIME TO RIGHT NOW (WILL TRIGGER IF IT DOESNT IGNORE SUSPENDED BUCKETS)");
		final Date updated_poll_date = new Date();
		updateNextPollTime(bucket_poll_now, underlying_crud_status, updated_poll_date);
		assertEquals(1, underlying_crud.countObjects().get().intValue());
		assertEquals(1, underlying_crud_status.countObjects().get().intValue());	
		
		//3. ping pollFreq, bucket should not respond
		//actor checks every second, give it long enough to hit a cycle
		Thread.sleep(5000);
		final DataBucketStatusBean bucket_poll_untouched = underlying_crud_status.getObjectById(bucket_poll_now._id()).get().get();
		assertEquals("Next poll time should be the same as what we set poll_time to", bucket_poll_untouched.next_poll_date().getTime(), updated_poll_date.getTime());
		assertEquals("Actor should have received 2 poll request and accepted only 1", 1, TestActor_Accepter.num_accepted_messages);
	}
	
	@Test
	public void test_mulitple_poll_trigger() throws Exception {
		//tests multiple polls hitting to make sure the service works on more than 1 poll at a time
		// Setup: register an accepting actor to listen:
		_logger.debug("ABOUT TO ADD A NEW ACTOR");		
		insertActor(TestActor_Accepter.class);
		assertEquals("No messages should be accepted yet", 0, TestActor_Accepter.num_accepted_messages);
		
		//get datastores
		@SuppressWarnings("unchecked")
		final ICrudService<DataBucketStatusBean> underlying_crud_status = (
				ICrudService<DataBucketStatusBean>) this._core_mgmt_db.getDataBucketStatusStore().getUnderlyingPlatformDriver(ICrudService.class, Optional.empty()).get();				
		@SuppressWarnings("unchecked")
		final ICrudService<DataBucketBean> underlying_crud = (
				ICrudService<DataBucketBean>) this._core_mgmt_db.getDataBucketStore().getUnderlyingPlatformDriver(ICrudService.class, Optional.empty()).get();
		underlying_crud.deleteDatastore().get();
		underlying_crud_status.deleteDatastore().get();
		assertEquals(0, underlying_crud.countObjects().get().intValue());
		assertEquals(0, underlying_crud_status.countObjects().get().intValue());		
		
		//create a bucket that has a poll freq	
		final DataBucketBean bucket_poll_now_1 = createBucket("5 min");		
		//create another that doesn't have one until later
		final DataBucketBean bucket_poll_now_2 = createBucket("7 min");
		//create another that doesn't have a poll frequency
		final DataBucketBean bucket_no_poll = createBucket(null);		
		
		//put it in the datastore		
		storeBucketAndStatus(bucket_poll_now_1, true, Optional.of(new Date()), underlying_crud_status, underlying_crud);
		storeBucketAndStatus(bucket_poll_now_2, true, Optional.of(new Date()), underlying_crud_status, underlying_crud);
		storeBucketAndStatus(bucket_no_poll, true, Optional.empty(), underlying_crud_status, underlying_crud);
		
		final DataBucketStatusBean bucket_poll_now_1_status = underlying_crud_status.getObjectById(bucket_poll_now_1._id()).get().get();
		final DataBucketStatusBean bucket_poll_now_2_status = underlying_crud_status.getObjectById(bucket_poll_now_2._id()).get().get();
				
		//actor checks every second, give it long enough to hit a cycle
		Thread.sleep(5000);
		
		//get the (hopefully) updated bucket		
		final DataBucketStatusBean bucket_poll_now_1_updated = underlying_crud_status.getObjectById(bucket_poll_now_1._id()).get().get();
		final DataBucketStatusBean bucket_poll_now_2_updated = underlying_crud_status.getObjectById(bucket_poll_now_2._id()).get().get();
		final DataBucketStatusBean bucket_no_poll_updated = underlying_crud_status.getObjectById(bucket_no_poll._id()).get().get();
		//make sure the actor 
		//1. updates freq (means it received the messaage)
		assertTrue("Next poll time 1 should have been greater than what we set poll_time to", bucket_poll_now_1_updated.next_poll_date().getTime() > bucket_poll_now_1_status.next_poll_date().getTime());
		assertTrue("Next poll time 2 should have been greater than what we set poll_time to", bucket_poll_now_2_updated.next_poll_date().getTime() > bucket_poll_now_2_status.next_poll_date().getTime());
		assertTrue("Poll never time should not have been set", bucket_no_poll_updated.next_poll_date() == null );
		
		//2. tries to send out poll messsage	
		assertEquals("Actor should have received 2 poll request and accepted it", 2, TestActor_Accepter.num_accepted_messages);
		
	}

	@After
	public void cleanupTest() {
		_actor_context.onTestComplete();
	}
	
	protected DataBucketBean createBucket(String poll_frequency) {
		if ( poll_frequency != null ) {
			return BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::_id, UuidUtils.get().getRandomUuid())
					.with(DataBucketBean::full_name, "/test/path/" + UuidUtils.get().getRandomUuid())
					.with(DataBucketBean::owner_id, "test_owner_id")
					.with(DataBucketBean::created, new Date())
					.with(DataBucketBean::description, "asdf")
					.with(DataBucketBean::modified, new Date())
					.with(DataBucketBean::display_name, "asdf")							
					.with(DataBucketBean::poll_frequency, poll_frequency)									
					.with(DataBucketBean::multi_node_enabled, false) 
					.with(DataBucketBean::master_enrichment_type, MasterEnrichmentType.batch)
					.with(DataBucketBean::harvest_technology_name_or_id, "harvest_tech")
					.with(DataBucketBean::harvest_configs, 
						Arrays.asList(BeanTemplateUtils.build(HarvestControlMetadataBean.class)
								.with(HarvestControlMetadataBean::enabled, true)
								.done().get()))
					
					.done().get();
		} else {
			return BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::_id, UuidUtils.get().getRandomUuid())
					.with(DataBucketBean::full_name, "/test/path/" + UuidUtils.get().getRandomUuid())
					.with(DataBucketBean::owner_id, "test_owner_id")
					.with(DataBucketBean::created, new Date())
					.with(DataBucketBean::description, "asdf")
					.with(DataBucketBean::modified, new Date())
					.with(DataBucketBean::display_name, "asdf")				
					.with(DataBucketBean::multi_node_enabled, false) 
					.with(DataBucketBean::master_enrichment_type, MasterEnrichmentType.batch)
					.with(DataBucketBean::harvest_technology_name_or_id, "harvest_tech")
					.with(DataBucketBean::harvest_configs, 
						Arrays.asList(BeanTemplateUtils.build(HarvestControlMetadataBean.class)
								.with(HarvestControlMetadataBean::enabled, true)
								.done().get()))
					.done().get();
		}
	}
	
	private void storeBucketAndStatus(DataBucketBean bucket,
			boolean store_status, Optional<Date> next_poll_date,
			ICrudService<DataBucketStatusBean> underlying_crud_status,
			ICrudService<DataBucketBean> underlying_crud) throws InterruptedException, ExecutionException {				
		underlying_crud.storeObject(bucket).get();
		if (store_status) {
			if ( next_poll_date.isPresent()) {
				DataBucketStatusBean status_bean = BeanTemplateUtils.build(DataBucketStatusBean.class)
															.with(DataBucketStatusBean::_id, bucket._id())
															.with(DataBucketStatusBean::bucket_path, bucket.full_name())
															.with(DataBucketStatusBean::node_affinity, Collections.emptyList())
															.with(DataBucketStatusBean::suspended, false)
															.with(DataBucketStatusBean::next_poll_date, next_poll_date.get())
															.done().get();			
				underlying_crud_status.storeObject(status_bean).get();		
			} else {
				DataBucketStatusBean status_bean = BeanTemplateUtils.build(DataBucketStatusBean.class)
						.with(DataBucketStatusBean::_id, bucket._id())
						.with(DataBucketStatusBean::bucket_path, bucket.full_name())
						.with(DataBucketStatusBean::node_affinity, Collections.emptyList())
						.with(DataBucketStatusBean::suspended, false)						
						.done().get();			
				underlying_crud_status.storeObject(status_bean).get();
			}
		}
	}
	
	private void suspendBucket(final DataBucketBean bucket, final ICrudService<DataBucketStatusBean> underlying_crud_status) throws InterruptedException, ExecutionException {
		final UpdateComponent<DataBucketStatusBean> update = CrudUtils.update(DataBucketStatusBean.class)
				.set(DataBucketStatusBean::suspended, true);
		underlying_crud_status.updateObjectById(bucket._id(), update).get();
	}
	
	private void updateNextPollTime(final DataBucketBean bucket, ICrudService<DataBucketStatusBean> underlying_status_crud, final Date updated_poll_date) throws InterruptedException, ExecutionException {
		final UpdateComponent<DataBucketStatusBean> update = CrudUtils.update(DataBucketStatusBean.class)
				.set(DataBucketStatusBean::next_poll_date, updated_poll_date);
		underlying_status_crud.updateObjectById(bucket._id(), update).get();
	}
	
	public String insertActor(Class<? extends UntypedActor> actor_clazz) throws Exception {
		String uuid = UuidUtils.get().getRandomUuid();
		ManagementDbActorContext.get().getDistributedServices()
			.getCuratorFramework().create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
			.forPath(ActorUtils.BUCKET_ACTION_ZOOKEEPER + "/" + uuid);
		
		ActorRef handler = ManagementDbActorContext.get().getActorSystem().actorOf(Props.create(actor_clazz, uuid), uuid);
		ManagementDbActorContext.get().getBucketActionMessageBus().subscribe(handler, ActorUtils.BUCKET_ACTION_EVENT_BUS);

		return uuid;
	}
}
