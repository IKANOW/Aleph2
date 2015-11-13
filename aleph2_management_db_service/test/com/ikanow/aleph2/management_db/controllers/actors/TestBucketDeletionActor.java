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
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import scala.Tuple2;
import scala.concurrent.duration.Duration;
import akka.actor.ActorRef;
import akka.actor.Inbox;
import akka.actor.Props;
import akka.actor.UntypedActor;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.data_model.utils.UuidUtils;
import com.ikanow.aleph2.data_model.utils.FutureUtils.ManagementFuture;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.distributed_services.services.MockCoreDistributedServices;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage;
import com.ikanow.aleph2.management_db.data_model.BucketMgmtMessage.BucketDeletionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketMgmtMessage.BucketMgmtEventBusWrapper;
import com.ikanow.aleph2.management_db.services.DataBucketCrudService;
import com.ikanow.aleph2.management_db.services.ManagementDbActorContext;
import com.ikanow.aleph2.management_db.utils.ActorUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

public class TestBucketDeletionActor {
	private static final Logger _logger = LogManager.getLogger();		

	@Inject 
	protected IServiceContext _service_context = null;	
	
	protected ICoreDistributedServices _cds = null;
	protected IManagementDbService _core_mgmt_db = null;
	protected ManagementDbActorContext _actor_context = null;

	protected MockSearchIndexService _mock_index = null;
	
	/////////////////////////
	
	// SETUP
	
	protected static String _check_actor_called = null;
	
	// This one always accepts, and returns a message
	public static class TestActor_Accepter extends UntypedActor {
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
				_check_actor_called = uuid;
				
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
		
		if (null != _service_context) {
			_logger.info("(Skipping setup, already initialized");
			return;
		}
		final String temp_dir = System.getProperty("java.io.tmpdir") + File.separator;
		
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
		
		_mock_index = (MockSearchIndexService) _service_context.getSearchIndexService().get();
		
		new ManagementDbActorContext(_service_context, true);		
		_actor_context = ManagementDbActorContext.get();
		
		_logger.info("(Setup complete: " + _core_mgmt_db.getClass() + " / " + _mock_index.getClass() + ")");		
	}	
		
	public static class TestBean {};
	
	/////////////////////////
	
	// TEST
	
	@Test
	public void test_bucketDeletionActor_fullDelete() throws Exception {
		
		final DataBucketBean bucket = createBucketInfrastructure("/test/full/delete", true);
		
		final BucketDeletionMessage msg = new BucketDeletionMessage(bucket, new Date(), false);
		
		final Inbox inbox = Inbox.create(_actor_context.getActorSystem());		
		
		_actor_context.getDeletionMgmtBus().publish(new BucketMgmtEventBusWrapper(inbox.getRef(), msg));
		
		try {
			final BucketDeletionMessage reply_msg = (BucketDeletionMessage) inbox.receive(Duration.create(4L, TimeUnit.SECONDS));
			assertEquals(reply_msg._id(), "/test/full/delete");
			
			// check file system deleted:
			assertFalse("The file path has been deleted", new File(System.getProperty("java.io.tmpdir") + File.separator + bucket.full_name() + "/managed_bucket").exists());
			
			// check mock index deleted:
			assertEquals(1, _mock_index._handleBucketDeletionRequests.size());
			final Collection<Tuple2<String, Object>> deletions = _mock_index._handleBucketDeletionRequests.get("handleBucketDeletionRequest");
			assertEquals(1, deletions.size());
			assertEquals("/test/full/delete", deletions.iterator().next()._1());
			assertEquals(true, deletions.iterator().next()._2());
			_mock_index._handleBucketDeletionRequests.clear();
			
			// check state directory cleansed:
			checkStateDirectoriesCleaned(bucket);
		}
		catch (Exception e) {
			fail("Timed out waiting for reply from BucketDeletionActor (or incorrect error): " + e.getMessage());
		}
	}

	@Test
	public void test_bucketDeletionActor_fullDelete_notExist() throws Exception {		
		final DataBucketBean bucket = createBucketInfrastructure("/test/full/delete/missing", false);
		assertFalse("The file path next existed", new File(System.getProperty("java.io.tmpdir") + File.separator + bucket.full_name() + "/managed_bucket").exists());

		final BucketDeletionMessage msg = new BucketDeletionMessage(bucket, new Date(), false);
		
		final Inbox inbox = Inbox.create(_actor_context.getActorSystem());
		_actor_context.getDeletionMgmtBus().publish(new BucketMgmtEventBusWrapper(inbox.getRef(), msg));
		
		try {
			final BucketDeletionMessage reply_msg = (BucketDeletionMessage) inbox.receive(Duration.create(10L, TimeUnit.SECONDS));
			assertEquals(reply_msg._id(), "/test/full/delete/missing");
			
			// check state directory *NOT* cleansed in this case:
			checkStateDirectoriesNotCleaned(bucket);
		}		
		catch (Exception e) {
			fail("Timed out waiting for reply from BucketDeletionActor (or incorrect error): " + e.getMessage());
		}
	}	
	
	@Test
	public void test_bucketDeletionActor_fullDelete_beanStillPresent() throws Exception {
		final DataBucketBean bucket = createBucketInfrastructure("/test/full/delete/fail_bean_present", true);
		
		final ICrudService<DataBucketBean> underlying_crud = storeBucketAndStatus(bucket, false, null);
		_core_mgmt_db.getBucketDeletionQueue(BucketDeletionMessage.class).deleteDatastore().get();
		assertEquals(0, _core_mgmt_db.getBucketDeletionQueue(BucketDeletionMessage.class).countObjects().get().intValue());
		
		final BucketDeletionMessage msg = new BucketDeletionMessage(bucket, new Date(), false);
		
		final Inbox inbox = Inbox.create(_actor_context.getActorSystem());		
		
		_actor_context.getDeletionMgmtBus().publish(new BucketMgmtEventBusWrapper(inbox.getRef(), msg));
		
		try {
			while (true) {
				final BucketDeletionMessage reply_msg = (BucketDeletionMessage) inbox.receive(Duration.create(4L, TimeUnit.SECONDS));
				if (!String.class.isAssignableFrom(reply_msg.getClass())) { // (Else some dead letter)
					fail("Should have timed out because bucket deletion should have failed: " + reply_msg.getClass());
				}
			}
		}
		catch (Exception e) {
			// worked - check stuff still around: 
			assertEquals(0, underlying_crud.countObjects().get().intValue());

			// check was added to the deletion queue:
			assertEquals(1, _core_mgmt_db.getBucketDeletionQueue(BucketDeletionMessage.class).countObjects().get().intValue());
			
			// check file system not deleted:
			assertTrue("The file path has *not* been deleted", new File(System.getProperty("java.io.tmpdir") + File.separator + "data" + File.separator + bucket.full_name() + "/managed_bucket").exists());
			
			// check mock index not deleted:
			assertEquals(0, _mock_index._handleBucketDeletionRequests.size());			
			
			// check state directory *NOT* cleansed in this case:
			checkStateDirectoriesNotCleaned(bucket);
		}
		_core_mgmt_db.getBucketDeletionQueue(BucketDeletionMessage.class).deleteDatastore().get();
		assertEquals(0, _core_mgmt_db.getDataBucketStore().countObjects().get().intValue());
	}
	
	@Test
	public void test_bucketDeletionActor_purge_immediate() throws Exception {
		final Tuple2<String,ActorRef> host_actor = insertActor(TestActor_Accepter.class);
		
		final DataBucketBean bucket = createBucketInfrastructure("/test/purge/immediate", true);
		
		storeBucketAndStatus(bucket, true, host_actor._1());
		
		final ManagementFuture<Boolean> res = _core_mgmt_db.purgeBucket(bucket, Optional.empty());
		
		// check result
		assertTrue("Purge called succeeded: " + res.getManagementResults().get().stream().map(msg->msg.message()).collect(Collectors.joining(";")), res.get());
		assertEquals(3, res.getManagementResults().get().size());
		
		//check system state afterwards
		
		// Full filesystem exists
		assertTrue("The file path has *not* been deleted", new File(System.getProperty("java.io.tmpdir") + File.separator + "data" + File.separator + bucket.full_name() + "/managed_bucket").exists());
		
		// Data directories no longer exist
		assertFalse("The data path has been deleted", new File(System.getProperty("java.io.tmpdir") + File.separator + "data" + File.separator + bucket.full_name() + IStorageService.STORED_DATA_SUFFIX_PROCESSED + "/test").exists());
		
		// check state directory _not_ cleaned in this case (the harvester can always do this once that's been wired up):
		checkStateDirectoriesNotCleaned(bucket);

		// check mock index deleted:
		assertEquals(1, _mock_index._handleBucketDeletionRequests.size());
		final Collection<Tuple2<String, Object>> deletions = _mock_index._handleBucketDeletionRequests.get("handleBucketDeletionRequest");
		assertEquals(1, deletions.size());
		assertEquals("/test/purge/immediate", deletions.iterator().next()._1());
		assertEquals(false, deletions.iterator().next()._2());
		_mock_index._handleBucketDeletionRequests.clear();
		
		shutdownActor(host_actor._2());
	}

	@Test
	public void test_bucketDeletionActor_purge_immediate_noStatus() throws Exception {
		final DataBucketBean bucket = createBucketInfrastructure("/test/purge/immediate/nostatus", true);
		
		storeBucketAndStatus(bucket, false, null);		
		
		final ManagementFuture<Boolean> res = _core_mgmt_db.purgeBucket(bucket, Optional.empty());
		
		// check result
		assertFalse("Purge called failed: " + res.getManagementResults().get().stream().map(msg->msg.message()).collect(Collectors.joining(";")), res.get());
		assertEquals(3, res.getManagementResults().get().size());
	}

	@Test
	public void test_bucketDeletionActor_purge_delayed() throws Exception {
		_logger.info("Running test_bucketDeletionActor_purge_delayed");
		
		final Tuple2<String,ActorRef> host_actor = insertActor(TestActor_Accepter.class);
		
		final DataBucketBean bucket = createBucketInfrastructure("/test/purge/delayed", true);

		final IManagementDbService underlying_mgmt_db = _service_context.getService(IManagementDbService.class, Optional.empty()).get();

		storeBucketAndStatus(bucket, true, host_actor._1());
		
		underlying_mgmt_db.getBucketDeletionQueue(BucketDeletionMessage.class).deleteDatastore().get(30L, TimeUnit.SECONDS);
		assertEquals(0, underlying_mgmt_db.getBucketDeletionQueue(BucketDeletionMessage.class).countObjects().get(30L, TimeUnit.SECONDS).intValue());
		
		// (in practice means will have to wait up to 10 seconds...)
		final ManagementFuture<Boolean> res = _core_mgmt_db.purgeBucket(bucket, Optional.of(java.time.Duration.ofSeconds(1)));
		
		// check result
		assertTrue("Purge called succeeded", res.get(30L, TimeUnit.SECONDS));
		assertEquals(0, res.getManagementResults().get().size());
		
		// Wait for it to complete:
		for (int i = 0; i < 5; ++i) {
			Thread.sleep(1000L);
			if (underlying_mgmt_db.getBucketDeletionQueue(BucketDeletionMessage.class).countObjects().get(30L, TimeUnit.SECONDS) > 0) break;
		}
		_logger.info("Stored deletion object");
		assertEquals(1, underlying_mgmt_db.getBucketDeletionQueue(BucketDeletionMessage.class).countObjects().get().intValue());
		for (int i = 0; i < 20; ++i) {
			Thread.sleep(1000L);
			if (underlying_mgmt_db.getBucketDeletionQueue(BucketDeletionMessage.class).countObjects().get(30L, TimeUnit.SECONDS) == 0) break;
		}
		assertEquals(0, underlying_mgmt_db.getBucketDeletionQueue(BucketDeletionMessage.class).countObjects().get(30L, TimeUnit.SECONDS).intValue());
		_logger.info("deletion object processed");
		
		//check system state afterwards
		
		// Full filesystem exists
		assertTrue("The file path has *not* been deleted", new File(System.getProperty("java.io.tmpdir") + File.separator + "data" + File.separator + bucket.full_name() + "/managed_bucket").exists());
		
		// Data directories no longer exist
		assertFalse("The data path has been deleted", new File(System.getProperty("java.io.tmpdir") + File.separator + "data" + File.separator + bucket.full_name() + IStorageService.STORED_DATA_SUFFIX_PROCESSED + "/test").exists());
		
		// check state directory _not_ cleaned in this case (the harvester can always do this once that's been wired up):
		checkStateDirectoriesNotCleaned(bucket);

		assertEquals(_check_actor_called, host_actor._1());
		
		// check mock index deleted:
		assertEquals(1, _mock_index._handleBucketDeletionRequests.size());
		final Collection<Tuple2<String, Object>> deletions = _mock_index._handleBucketDeletionRequests.get("handleBucketDeletionRequest");
		assertEquals(1, deletions.size());
		assertEquals("/test/purge/delayed", deletions.iterator().next()._1());
		assertEquals(false, deletions.iterator().next()._2());
		_mock_index._handleBucketDeletionRequests.clear();
		
		shutdownActor(host_actor._2());		
		
		_logger.info("Completed test_bucketDeletionActor_purge_delayed");
	}
	
	@After
	public void cleanupTest() {
		_logger.info("Shutting down actor context");
		_actor_context.onTestComplete();
	}
	
	/////////////////////////
	
	// UTILITY
	
	public Tuple2<String,ActorRef> insertActor(Class<? extends UntypedActor> actor_clazz) throws Exception {
		String uuid = UuidUtils.get().getRandomUuid();
		ManagementDbActorContext.get().getDistributedServices()
			.getCuratorFramework().create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
			.forPath(ActorUtils.BUCKET_ACTION_ZOOKEEPER + "/" + uuid);
		
		ActorRef handler = ManagementDbActorContext.get().getActorSystem().actorOf(Props.create(actor_clazz, uuid), uuid);
		ManagementDbActorContext.get().getBucketActionMessageBus().subscribe(handler, ActorUtils.BUCKET_ACTION_EVENT_BUS);

		return Tuples._2T(uuid, handler);
	}
	
	public void shutdownActor(ActorRef to_remove) {
		ManagementDbActorContext.get().getBucketActionMessageBus().unsubscribe(to_remove);
	}
	
	protected void checkStateDirectoriesCleaned(DataBucketBean bucket) throws InterruptedException, ExecutionException {
		ICrudService<?> top_level_state_directory = _core_mgmt_db.getStateDirectory(Optional.empty(), Optional.empty());
		assertEquals(0, top_level_state_directory.countObjects().get().intValue());
		ICrudService<TestBean> harvest_state = _core_mgmt_db.getBucketHarvestState(TestBean.class, bucket, Optional.of("test1"));
		assertEquals(0, harvest_state.countObjects().get().intValue());
		ICrudService<TestBean> enrich_state = _core_mgmt_db.getBucketEnrichmentState(TestBean.class, bucket, Optional.of("test2"));
		assertEquals(0, enrich_state.countObjects().get().intValue());
		ICrudService<TestBean> analytics_state = _core_mgmt_db.getBucketAnalyticThreadState(TestBean.class, bucket, Optional.of("test3"));
		assertEquals(0, analytics_state.countObjects().get().intValue());		
	}
	protected void checkStateDirectoriesNotCleaned(DataBucketBean bucket) throws InterruptedException, ExecutionException {
		ICrudService<?> top_level_state_directory = _core_mgmt_db.getStateDirectory(Optional.empty(), Optional.empty());
		assertEquals(3, top_level_state_directory.countObjects().get().intValue());
		ICrudService<TestBean> harvest_state = _core_mgmt_db.getBucketHarvestState(TestBean.class, bucket, Optional.of("test1"));
		assertEquals(1, harvest_state.countObjects().get().intValue());
		ICrudService<TestBean> enrich_state = _core_mgmt_db.getBucketEnrichmentState(TestBean.class, bucket, Optional.of("test2"));
		assertEquals(1, enrich_state.countObjects().get().intValue());
		ICrudService<TestBean> analytics_state = _core_mgmt_db.getBucketAnalyticThreadState(TestBean.class, bucket, Optional.of("test3"));
		assertEquals(1, analytics_state.countObjects().get().intValue());		
	}

	protected ICrudService<DataBucketBean> storeBucketAndStatus(final DataBucketBean bucket, boolean store_status, String host) throws InterruptedException, ExecutionException {
		@SuppressWarnings("unchecked")
		final ICrudService<DataBucketBean> underlying_crud = (
				ICrudService<DataBucketBean>) this._core_mgmt_db.getDataBucketStore().getUnderlyingPlatformDriver(ICrudService.class, Optional.empty()).get();
		@SuppressWarnings("unchecked")
		final ICrudService<DataBucketStatusBean> underlying_crud_status = (
				ICrudService<DataBucketStatusBean>) this._core_mgmt_db.getDataBucketStatusStore().getUnderlyingPlatformDriver(ICrudService.class, Optional.empty()).get();		
		underlying_crud.deleteDatastore().get();
		underlying_crud_status.deleteDatastore().get();
		assertEquals(0, underlying_crud.countObjects().get().intValue());
		assertEquals(0, underlying_crud_status.countObjects().get().intValue());
		underlying_crud.storeObject(bucket).get();
		if (store_status) {
			DataBucketStatusBean status_bean = BeanTemplateUtils.build(DataBucketStatusBean.class)
														.with(DataBucketStatusBean::bucket_path, bucket.full_name())
														.with(DataBucketStatusBean::node_affinity, null == host ? Collections.emptyList() : Arrays.asList(host))
														.done().get();
			underlying_crud_status.storeObject(status_bean).get();
			assertEquals(1, underlying_crud_status.countObjects().get().intValue());
		}
		assertEquals(1, underlying_crud.countObjects().get().intValue());
		
		return underlying_crud;
	}
	
	public DataBucketBean createBucketInfrastructure(final String path, boolean create_file_path) throws Exception {
		_logger.info("CREATING BUCKET: " + path);
		
		// delete the existing path if present:
		try {
			FileUtils.deleteDirectory(new File(System.getProperty("java.io.tmpdir") + File.separator + "data" + File.separator + path));
		}
		catch (Exception e) {} // (fine, dir prob dones't delete)
		
		final DataBucketBean bucket = BeanTemplateUtils.build(DataBucketBean.class)
										.with("full_name", path)
										.with("harvest_technology_name_or_id", "test")
										.done().get();
		
		// Then create it:
		
		if (create_file_path) {
			DataBucketCrudService.createFilePaths(bucket, _service_context.getStorageService());
			final String bucket_path = System.getProperty("java.io.tmpdir") + File.separator + "data" + File.separator + bucket.full_name();
			assertTrue("The file path has been created", new File(bucket_path + "/managed_bucket").exists());
			FileUtils.writeStringToFile(new File(bucket_path + IStorageService.STORED_DATA_SUFFIX_PROCESSED + "/test"), "");
			assertTrue("The extra file path has been created", new File(bucket_path + IStorageService.STORED_DATA_SUFFIX_PROCESSED + "/test").exists());
		}
		
		// Also create a state directory object so we can check that gets deleted
		ICrudService<?> top_level_state_directory = _core_mgmt_db.getStateDirectory(Optional.empty(), Optional.empty());
		top_level_state_directory.deleteDatastore().get();
		assertEquals(0, top_level_state_directory.countObjects().get().intValue());
		ICrudService<TestBean> harvest_state = _core_mgmt_db.getBucketHarvestState(TestBean.class, bucket, Optional.of("test1"));
		harvest_state.deleteObjectsBySpec(CrudUtils.allOf(TestBean.class)).get();
		assertEquals(0, harvest_state.countObjects().get().intValue());
		ICrudService<TestBean> enrich_state = _core_mgmt_db.getBucketEnrichmentState(TestBean.class, bucket, Optional.of("test2"));
		enrich_state.deleteObjectsBySpec(CrudUtils.allOf(TestBean.class)).get();
		assertEquals(0, enrich_state.countObjects().get().intValue());
		ICrudService<TestBean> analytics_state = _core_mgmt_db.getBucketAnalyticThreadState(TestBean.class, bucket, Optional.of("test3"));
		analytics_state.deleteObjectsBySpec(CrudUtils.allOf(TestBean.class)).get();
		assertEquals(0, analytics_state.countObjects().get().intValue());
		assertEquals(3, top_level_state_directory.countObjects().get().intValue());
		harvest_state.storeObject(new TestBean()).get();
		enrich_state.storeObject(new TestBean()).get();
		analytics_state.storeObject(new TestBean()).get();
		assertEquals(1, harvest_state.countObjects().get().intValue());
		assertEquals(1, enrich_state.countObjects().get().intValue());
		assertEquals(1, analytics_state.countObjects().get().intValue());
		
		return bucket;
	}

}
