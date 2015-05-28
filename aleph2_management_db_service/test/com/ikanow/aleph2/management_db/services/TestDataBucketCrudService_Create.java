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
package com.ikanow.aleph2.management_db.services;

import static org.junit.Assert.*;

import java.io.File;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;

import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.MockServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.data_import.HarvestControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.FutureUtils.ManagementFuture;
import com.ikanow.aleph2.data_model.utils.UuidUtils;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.distributed_services.services.MockCoreDistributedServices;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionRetryMessage;
import com.ikanow.aleph2.management_db.mongodb.data_model.MongoDbManagementDbConfigBean;
import com.ikanow.aleph2.management_db.mongodb.services.MockMongoDbManagementDbService;
import com.ikanow.aleph2.management_db.utils.ActorUtils;
import com.ikanow.aleph2.management_db.utils.ManagementDbErrorUtils;
import com.ikanow.aleph2.shared.crud.mongodb.services.MockMongoDbCrudServiceFactory;
import com.ikanow.aleph2.storage_service_hdfs.services.MockHdfsStorageService;
import com.mongodb.MongoException;
import org.apache.log4j.Logger;

public class TestDataBucketCrudService_Create {

	public static final Logger _logger = Logger.getLogger(TestDataBucketCrudService_Create.class);	
	
	/////////////////////////////////////////////////////////////
	
	// Some test infrastructure
	
	// This is everything DI normally does for you!
	public GlobalPropertiesBean _globals;
	public MockHdfsStorageService _storage_service;
	public MockMongoDbManagementDbService _underlying_db_service;
	public CoreManagementDbService _core_db_service;
	public MockServiceContext _mock_service_context;
	public MockMongoDbCrudServiceFactory _crud_factory;
	public DataBucketCrudService _bucket_crud;
	public DataBucketStatusCrudService _bucket_status_crud;
	public SharedLibraryCrudService _shared_library_crud;
	public ManagementDbActorContext _db_actor_context;
	public ICoreDistributedServices _core_distributed_services;
	public ICrudService<DataBucketBean> _underlying_bucket_crud;
	public ICrudService<DataBucketStatusBean> _underlying_bucket_status_crud;
	public ICrudService<BucketActionRetryMessage> _bucket_action_retry_store;
	
	@Before
	public void setup() throws Exception {
		
		// Here's the setup that Guice normally gives you....
		final String tmpdir = System.getProperty("java.io.tmpdir");
		_globals = new GlobalPropertiesBean(tmpdir, tmpdir, tmpdir, tmpdir);
		_storage_service = new MockHdfsStorageService(_globals);
		_mock_service_context = new MockServiceContext();		
		_crud_factory = new MockMongoDbCrudServiceFactory();
		_underlying_db_service = new MockMongoDbManagementDbService(_crud_factory, new MongoDbManagementDbConfigBean(false), null);
		_core_distributed_services = new MockCoreDistributedServices();
		_mock_service_context.addService(GlobalPropertiesBean.class, Optional.empty(), new GlobalPropertiesBean(null, null, null, null));
		_mock_service_context.addService(IManagementDbService.class, Optional.empty(), _underlying_db_service);
		_mock_service_context.addService(ICoreDistributedServices.class, Optional.empty(), _core_distributed_services);
		_mock_service_context.addService(IStorageService.class, Optional.empty(),_storage_service);
		_db_actor_context = new ManagementDbActorContext(_mock_service_context, new LocalBucketActionMessageBus());
		_bucket_crud = new DataBucketCrudService(_mock_service_context, _db_actor_context);
		_bucket_status_crud = new DataBucketStatusCrudService(_mock_service_context, _db_actor_context);
		_shared_library_crud = new SharedLibraryCrudService(_mock_service_context);
		_core_db_service = new CoreManagementDbService(_mock_service_context, _bucket_crud, _bucket_status_crud, _shared_library_crud);
		_mock_service_context.addService(IManagementDbService.class, Optional.of("CoreManagementDbService"), _core_db_service);		
		
		_underlying_bucket_crud = _bucket_crud._underlying_data_bucket_db;
		_underlying_bucket_status_crud = _bucket_crud._underlying_data_bucket_status_db;
		_bucket_action_retry_store = _bucket_crud._bucket_action_retry_store;
	}	
	
	// Actors:
	
	// Test actors:
	// This one always refuses
	public static class TestActor_Refuser extends UntypedActor {
		public TestActor_Refuser(String uuid) {
			this.uuid = uuid;
		}
		private final String uuid;
		@Override
		public void onReceive(Object arg0) throws Exception {
			_logger.info("Refuse from: " + uuid);
			
			this.sender().tell(new BucketActionReplyMessage.BucketActionIgnoredMessage(uuid), this.self());
		}		
	}

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
	
	// This one always accepts, but then refuses when it comes down to it...
	public static class TestActor_Accepter_Refuser extends UntypedActor {
		public TestActor_Accepter_Refuser(String uuid) {
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
				_logger.info("Refuse MESSAGE from: " + uuid);
				this.sender().tell(new BucketActionReplyMessage.BucketActionIgnoredMessage(uuid), this.self());
			}
		}		
	}	
	
	public String insertActor(Class<? extends UntypedActor> actor_clazz) throws Exception {
		String uuid = UuidUtils.get().getRandomUuid();
		ManagementDbActorContext.get().getDistributedServices()
			.getCuratorFramework().create().creatingParentsIfNeeded()
			.forPath(ActorUtils.BUCKET_ACTION_ZOOKEEPER + "/" + uuid);
		
		ActorRef handler = ManagementDbActorContext.get().getActorSystem().actorOf(Props.create(actor_clazz, uuid), uuid);
		ManagementDbActorContext.get().getBucketActionMessageBus().subscribe(handler, ActorUtils.BUCKET_ACTION_EVENT_BUS);

		return uuid;
	}
	
	// Bucket insertion
	
	public void cleanDatabases() {
		
		_underlying_bucket_crud.deleteDatastore();
		_underlying_bucket_status_crud.deleteDatastore();
		_bucket_action_retry_store.deleteDatastore();
	}
	
	/////////////////////////////////////////////////////////////	
	/////////////////////////////////////////////////////////////	
	/////////////////////////////////////////////////////////////	
	
	// Store bucket
	
	@Test
	public void testValidateInsert() throws Exception {
		cleanDatabases();

		// 0) Start with a valid bucket:
		
		final DataBucketBean valid_bucket = 
				BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::_id, "id1")
				.with(DataBucketBean::full_name, "/name1/embedded/dir/")
				.with(DataBucketBean::display_name, "name1")
				.with(DataBucketBean::created, new Date())
				.with(DataBucketBean::modified, new Date())
				.with(DataBucketBean::owner_id, "owner1")
				.with(DataBucketBean::access_rights, BeanTemplateUtils.build(AuthorizationBean.class).done().get())
				.done().get();

		//(delete the file path)
		try {
			FileUtils.deleteDirectory(new File(System.getProperty("java.io.tmpdir") + valid_bucket.full_name()));
		}
		catch (Exception e) {} // (fine, dir prob dones't delete)
		assertFalse("The file path has been deleted", new File(System.getProperty("java.io.tmpdir") + valid_bucket.full_name() + "/managed_bucket").exists());
		
		// 1) Check needs status object to be present
		
		try {
			final ManagementFuture<Supplier<Object>> result = _bucket_crud.storeObject(valid_bucket);
			assertEquals(0, result.getManagementResults().get().size());			
			result.get();
			fail("Should have thrown an exception");
		}
		catch (Exception e) {
			System.out.println("expected, err=" + e.getCause().getMessage());
			assertEquals(RuntimeException.class, e.getCause().getClass());
		}		
		
		//(add the status object and try)
		final DataBucketStatusBean status = 
				BeanTemplateUtils.build(DataBucketStatusBean.class)
				.with(DataBucketStatusBean::_id, valid_bucket._id())
				.with(DataBucketStatusBean::bucket_path, valid_bucket.full_name())
				.with(DataBucketStatusBean::suspended, false)
				.done().get();
		
		assertEquals(0L, (long)_bucket_status_crud.countObjects().get());
		_bucket_status_crud.storeObject(status).get();
		assertEquals(1L, (long)_bucket_status_crud.countObjects().get());

		// Try again, assert - sort of works this time, creates the bucket but in suspended mode because there are no registered data import managers
		assertEquals(0L, (long)_bucket_crud.countObjects().get());
		final ManagementFuture<Supplier<Object>> insert_future = _bucket_crud.storeObject(valid_bucket);
		final BasicMessageBean err_msg = insert_future.getManagementResults().get().iterator().next();
		assertEquals(false, err_msg.success());
		assertEquals(ErrorUtils.get(ManagementDbErrorUtils.NO_DATA_IMPORT_MANAGERS_STARTED_SUSPENDED, valid_bucket.full_name()), err_msg.message());
		assertEquals(valid_bucket._id(), insert_future.get().get());
		final DataBucketStatusBean status_after = _bucket_status_crud.getObjectById(valid_bucket._id()).get().get();
		assertEquals(0, status_after.node_affinity().size());
		assertEquals(true, status_after.suspended());
		assertTrue("The file path has been built", new File(System.getProperty("java.io.tmpdir") + valid_bucket.full_name() + "/managed_bucket").exists());
		assertEquals(1L, (long)_bucket_crud.countObjects().get());
				
		//////////////////////////
		
		// Validation _errors_
		
		// 2) Missing field
		
		final DataBucketBean new_valid_bucket =  BeanTemplateUtils.clone(valid_bucket).with(DataBucketBean::_id, "id2").done();
				
		try {
			final DataBucketBean bucket = BeanTemplateUtils.clone(new_valid_bucket).with(DataBucketBean::full_name, null).done();
			final ManagementFuture<Supplier<Object>> result = _bucket_crud.storeObject(bucket);
			assertTrue("Got errors", !result.getManagementResults().get().isEmpty());
			
			result.get();
			fail("Should have thrown an exception");
		}
		catch (Exception e) {
			System.out.println("expected, err=" + e.getCause().getMessage());
			assertEquals(RuntimeException.class, e.getCause().getClass());
		}		
		assertEquals(1L, (long)_bucket_crud.countObjects().get());
		
		// 3) Zero length fields
		
		try {
			final DataBucketBean bucket = BeanTemplateUtils.clone(new_valid_bucket).with(DataBucketBean::full_name, "").done();
			final ManagementFuture<Supplier<Object>> result = _bucket_crud.storeObject(bucket);
			assertTrue("Got errors", !result.getManagementResults().get().isEmpty() && !result.getManagementResults().get().iterator().next().success());
			
			result.get();
			fail("Should have thrown an exception");
		}
		catch (Exception e) {
			System.out.println("expected, err=" + e.getCause().getMessage());
			assertEquals(RuntimeException.class, e.getCause().getClass());
		}		
		assertEquals(1L, (long)_bucket_crud.countObjects().get());
		
		// 4) Enrichment but no harvest
		
		try {
			final DataBucketBean bucket = BeanTemplateUtils.clone(new_valid_bucket)
					.with(DataBucketBean::master_enrichment_type, DataBucketBean.MasterEnrichmentType.batch)
					.with(DataBucketBean::batch_enrichment_topology, 
							BeanTemplateUtils.build(EnrichmentControlMetadataBean.class).done().get()).done();
			final ManagementFuture<Supplier<Object>> result = _bucket_crud.storeObject(bucket);
			assertTrue("Got errors", !result.getManagementResults().get().isEmpty() && !result.getManagementResults().get().iterator().next().success());			
			result.get();
			fail("Should have thrown an exception");
		}
		catch (Exception e) {
			System.out.println("expected, err=" + e.getCause().getMessage());
			assertEquals(RuntimeException.class, e.getCause().getClass());
		}				
		assertEquals(1L, (long)_bucket_crud.countObjects().get());
		
		// 5) Enrichment but no type
		
		try {
			final DataBucketBean bucket = BeanTemplateUtils.clone(new_valid_bucket)
					.with(DataBucketBean::harvest_technology_name_or_id, "harvest_tech")
					.with(DataBucketBean::harvest_configs, Arrays.asList(BeanTemplateUtils.build(HarvestControlMetadataBean.class).with(HarvestControlMetadataBean::enabled, true).done().get()))
					.with(DataBucketBean::batch_enrichment_topology, 
							BeanTemplateUtils.build(EnrichmentControlMetadataBean.class).done().get()).done();
			final ManagementFuture<Supplier<Object>> result = _bucket_crud.storeObject(bucket);
			assertTrue("Got errors", !result.getManagementResults().get().isEmpty() && !result.getManagementResults().get().iterator().next().success());
			
			result.get();
			fail("Should have thrown an exception");
		}
		catch (Exception e) {
			System.out.println("expected, err=" + e.getCause().getMessage());
			assertEquals(RuntimeException.class, e.getCause().getClass());
		}				
		assertEquals(1L, (long)_bucket_crud.countObjects().get());
		
		// 6) Wrong type of enrichment - streaming missing
		
		try {
			final DataBucketBean bucket = BeanTemplateUtils.clone(new_valid_bucket)
					.with(DataBucketBean::harvest_technology_name_or_id, "harvest_tech")
					.with(DataBucketBean::harvest_configs, Arrays.asList(BeanTemplateUtils.build(HarvestControlMetadataBean.class).with(HarvestControlMetadataBean::enabled, true).done().get()))
					.with(DataBucketBean::master_enrichment_type, DataBucketBean.MasterEnrichmentType.streaming_and_batch)
					.with(DataBucketBean::batch_enrichment_topology, 
							BeanTemplateUtils.build(EnrichmentControlMetadataBean.class).done().get()).done();
			final ManagementFuture<Supplier<Object>> result = _bucket_crud.storeObject(bucket);
			assertTrue("Got errors", !result.getManagementResults().get().isEmpty() && !result.getManagementResults().get().iterator().next().success());
			
			result.get();
			fail("Should have thrown an exception");
		}
		catch (Exception e) {
			System.out.println("expected, err=" + e.getCause().getMessage());
			assertEquals(RuntimeException.class, e.getCause().getClass());
		}				
		assertEquals(1L, (long)_bucket_crud.countObjects().get());			
		
		// 7) Wrong type of enrichment - batch missing
		
		try {
			final DataBucketBean bucket = BeanTemplateUtils.clone(new_valid_bucket)
					.with(DataBucketBean::harvest_technology_name_or_id, "harvest_tech")
					.with(DataBucketBean::harvest_configs, Arrays.asList(BeanTemplateUtils.build(HarvestControlMetadataBean.class).with(HarvestControlMetadataBean::enabled, true).done().get()))
					.with(DataBucketBean::master_enrichment_type, DataBucketBean.MasterEnrichmentType.streaming_and_batch)
					.with(DataBucketBean::streaming_enrichment_topology, 
							BeanTemplateUtils.build(EnrichmentControlMetadataBean.class).done().get()).done();
			final ManagementFuture<Supplier<Object>> result = _bucket_crud.storeObject(bucket);
			assertTrue("Got errors", !result.getManagementResults().get().isEmpty() && !result.getManagementResults().get().iterator().next().success());
			
			result.get();
			fail("Should have thrown an exception");
		}
		catch (Exception e) {
			System.out.println("expected, err=" + e.getCause().getMessage());
			assertEquals(RuntimeException.class, e.getCause().getClass());
		}				
		assertEquals(1L, (long)_bucket_crud.countObjects().get());			
		
		// 8) No harvest configs
		
		try {
			final DataBucketBean bucket = BeanTemplateUtils.clone(new_valid_bucket)
					.with(DataBucketBean::harvest_technology_name_or_id, "harvest_tech")
					.done();
			final ManagementFuture<Supplier<Object>> result = _bucket_crud.storeObject(bucket);
			assertTrue("Got errors", !result.getManagementResults().get().isEmpty() && !result.getManagementResults().get().iterator().next().success());
			
			result.get();
			fail("Should have thrown an exception");
		}
		catch (Exception e) {
			System.out.println("expected, err=" + e.getCause().getMessage());
			assertEquals(RuntimeException.class, e.getCause().getClass());
		}		
		assertEquals(1L, (long)_bucket_crud.countObjects().get());
				
		// 9) Harvest config with empty lists
		
		try {
			final DataBucketBean bucket = BeanTemplateUtils.clone(new_valid_bucket)
					.with(DataBucketBean::harvest_technology_name_or_id, "harvest_tech")
					.with(DataBucketBean::harvest_configs, Arrays.asList(
							BeanTemplateUtils.build(HarvestControlMetadataBean.class)
								.with(HarvestControlMetadataBean::enabled, true)
								.with(HarvestControlMetadataBean::library_ids_or_names, Arrays.asList("xxx", ""))
								.done().get()))
					.done();
			final ManagementFuture<Supplier<Object>> result = _bucket_crud.storeObject(bucket);
			assertTrue("Got errors", !result.getManagementResults().get().isEmpty() && !result.getManagementResults().get().iterator().next().success());
			
			result.get();
			fail("Should have thrown an exception");
		}
		catch (Exception e) {
			System.out.println("expected, err=" + e.getCause().getMessage());
			assertEquals(RuntimeException.class, e.getCause().getClass());
		}		
		assertEquals(1L, (long)_bucket_crud.countObjects().get());

		// 10) Missing batch config enabled:
		
		try {
			final DataBucketBean bucket = BeanTemplateUtils.clone(new_valid_bucket)
					.with(DataBucketBean::harvest_technology_name_or_id, "harvest_tech")
					.with(DataBucketBean::harvest_configs, Arrays.asList(BeanTemplateUtils.build(HarvestControlMetadataBean.class).with(HarvestControlMetadataBean::enabled, true).done().get()))
					.with(DataBucketBean::master_enrichment_type, DataBucketBean.MasterEnrichmentType.batch)
					.with(DataBucketBean::batch_enrichment_configs, 
							Arrays.asList(BeanTemplateUtils.build(EnrichmentControlMetadataBean.class).done().get()))
							.done();
			final ManagementFuture<Supplier<Object>> result = _bucket_crud.storeObject(bucket);
			assertTrue("Got errors", !result.getManagementResults().get().isEmpty() && !result.getManagementResults().get().iterator().next().success());
			
			result.get();
			fail("Should have thrown an exception");
		}
		catch (Exception e) {
			System.out.println("expected, err=" + e.getCause().getMessage());
			assertEquals(RuntimeException.class, e.getCause().getClass());
		}				
		assertEquals(1L, (long)_bucket_crud.countObjects().get());			
		
		
		// 11) Missing streaming config enabled:
		
		try {
			final DataBucketBean bucket = BeanTemplateUtils.clone(new_valid_bucket)
					.with(DataBucketBean::harvest_technology_name_or_id, "harvest_tech")
					.with(DataBucketBean::harvest_configs, Arrays.asList(BeanTemplateUtils.build(HarvestControlMetadataBean.class).with(HarvestControlMetadataBean::enabled, true).done().get()))
					.with(DataBucketBean::master_enrichment_type, DataBucketBean.MasterEnrichmentType.streaming)
					.with(DataBucketBean::streaming_enrichment_configs, 
							Arrays.asList(BeanTemplateUtils.build(EnrichmentControlMetadataBean.class).done().get()))
							.done();
			final ManagementFuture<Supplier<Object>> result = _bucket_crud.storeObject(bucket);
			assertTrue("Got errors", !result.getManagementResults().get().isEmpty() && !result.getManagementResults().get().iterator().next().success());
			
			result.get();
			fail("Should have thrown an exception");
		}
		catch (Exception e) {
			System.out.println("expected, err=" + e.getCause().getMessage());
			assertEquals(RuntimeException.class, e.getCause().getClass());
		}				
		assertEquals(1L, (long)_bucket_crud.countObjects().get());			
		
		
		// 12) Multi bucket!
		
		try {
			final DataBucketBean bucket = BeanTemplateUtils.clone(new_valid_bucket)
					.with(DataBucketBean::multi_bucket_children, new HashSet<String>(Arrays.asList("a", "b")))
					.with(DataBucketBean::harvest_technology_name_or_id, "harvest_tech")
					.with(DataBucketBean::harvest_configs, Arrays.asList(BeanTemplateUtils.build(HarvestControlMetadataBean.class).done().get()))
					.done();
			
			final ManagementFuture<Supplier<Object>> result = _bucket_crud.storeObject(bucket);
			assertTrue("Got errors", !result.getManagementResults().get().isEmpty() && !result.getManagementResults().get().iterator().next().success());
			
			result.get();
			fail("Should have thrown an exception");
		}
		catch (Exception e) {
			System.out.println("expected, err=" + e.getCause().getMessage());
			assertEquals(RuntimeException.class, e.getCause().getClass());
		}				
		assertEquals(1L, (long)_bucket_crud.countObjects().get());
	}

	///////////////////////////////////////////////////////////////////////////////////////////

	@Test 
	public void handleDeleteFileStillPresent() throws InterruptedException, ExecutionException {
		cleanDatabases();
				
		// 0) Start with a valid bucket:
		
		final DataBucketBean valid_bucket = 
				BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::_id, "id1")
				.with(DataBucketBean::full_name, "/name1/embedded/dir/")
				.with(DataBucketBean::display_name, "name1")
				.with(DataBucketBean::created, new Date())
				.with(DataBucketBean::modified, new Date())
				.with(DataBucketBean::owner_id, "owner1")
				.with(DataBucketBean::multi_node_enabled, true) 
				.with(DataBucketBean::access_rights, BeanTemplateUtils.build(AuthorizationBean.class).done().get())
				.with(DataBucketBean::harvest_technology_name_or_id, "harvest_tech")
				.with(DataBucketBean::harvest_configs, 
						Arrays.asList(BeanTemplateUtils.build(HarvestControlMetadataBean.class)
								.with(HarvestControlMetadataBean::enabled, true)
								.done().get()))
				.done().get();

		//(add the status object and try)
		final DataBucketStatusBean status = 
				BeanTemplateUtils.build(DataBucketStatusBean.class)
				.with(DataBucketStatusBean::_id, valid_bucket._id())
				.with(DataBucketStatusBean::bucket_path, valid_bucket.full_name())
				.with(DataBucketStatusBean::suspended, false)
				.done().get();
		
		assertEquals(0L, (long)_bucket_status_crud.countObjects().get());
		_bucket_status_crud.storeObject(status).get();
		assertEquals(1L, (long)_bucket_status_crud.countObjects().get());
		
		// Add a delete file:
		try {
			new File(System.getProperty("java.io.tmpdir") + valid_bucket.full_name()).mkdirs();
		}		
		catch (Exception e) {} // (fine, dir prob dones't delete)
		
		try {
			new File(System.getProperty("java.io.tmpdir") + valid_bucket.full_name() + ".DELETED").createNewFile();
		}		
		catch (Exception e) {} // (fine, dir prob dones't delete)

		assertTrue("file exists", new File(System.getProperty("java.io.tmpdir") + valid_bucket.full_name() + ".DELETED").exists());
		
		// OK now try inserting the bucket, should error:
		
		try {
			final ManagementFuture<Supplier<Object>> result = _bucket_crud.storeObject(valid_bucket);
			result.get();
			fail("Should have thrown an exception");
		}
		catch (Exception e) {
			System.out.println("expected, err=" + e.getCause().getMessage());
			assertEquals(RuntimeException.class, e.getCause().getClass());
		}				
		assertEquals(0L, (long)_bucket_crud.countObjects().get());			
		
	}
	
	///////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////
	
	@Test
	public void testAnotherSemiFailedBucketCreation_multiNode() throws Exception {
		cleanDatabases();

		// Setup: register a refuse-then-accept
		final String refusing_host1 = insertActor(TestActor_Refuser.class);
		final String refusing_host2 = insertActor(TestActor_Refuser.class);
		assertFalse("created actors on different hosts", refusing_host1.equals(refusing_host2));
		
		// 0) Start with a valid bucket:
		
		final DataBucketBean valid_bucket = 
				BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::_id, "id1")
				.with(DataBucketBean::full_name, "/name1/embedded/dir/")
				.with(DataBucketBean::display_name, "name1")
				.with(DataBucketBean::created, new Date())
				.with(DataBucketBean::modified, new Date())
				.with(DataBucketBean::owner_id, "owner1")
				.with(DataBucketBean::multi_node_enabled, false) 
				.with(DataBucketBean::access_rights, BeanTemplateUtils.build(AuthorizationBean.class).done().get())
				.with(DataBucketBean::harvest_technology_name_or_id, "harvest_tech")
				.with(DataBucketBean::harvest_configs, 
						Arrays.asList(BeanTemplateUtils.build(HarvestControlMetadataBean.class)
								.with(HarvestControlMetadataBean::enabled, true)
								.done().get()))
				.with(DataBucketBean::access_rights, BeanTemplateUtils.build(AuthorizationBean.class).done().get())
				.done().get();

		//(delete the file path)
		try {
			FileUtils.deleteDirectory(new File(System.getProperty("java.io.tmpdir") + valid_bucket.full_name()));
		}
		catch (Exception e) {} // (fine, dir prob dones't delete)
		assertFalse("The file path has been deleted", new File(System.getProperty("java.io.tmpdir") + valid_bucket.full_name() + "/managed_bucket").exists());
		
		//(add the status object and try)
		final DataBucketStatusBean status = 
				BeanTemplateUtils.build(DataBucketStatusBean.class)
				.with(DataBucketStatusBean::_id, valid_bucket._id())
				.with(DataBucketStatusBean::bucket_path, valid_bucket.full_name())
				.with(DataBucketStatusBean::suspended, false)
				.done().get();
		
		assertEquals(0L, (long)_bucket_status_crud.countObjects().get());
		_bucket_status_crud.storeObject(status).get();
		assertEquals(1L, (long)_bucket_status_crud.countObjects().get());

		assertEquals(0L, (long)_bucket_crud.countObjects().get());
		final ManagementFuture<Supplier<Object>> insert_future = _bucket_crud.storeObject(valid_bucket);
		final BasicMessageBean err_msg = insert_future.getManagementResults().get().iterator().next();
		assertEquals(false, err_msg.success());
		assertEquals(ErrorUtils.get(ManagementDbErrorUtils.NO_DATA_IMPORT_MANAGERS_STARTED_SUSPENDED, valid_bucket.full_name()), err_msg.message());
		assertEquals(valid_bucket._id(), insert_future.get().get());
		final DataBucketStatusBean status_after = _bucket_status_crud.getObjectById(valid_bucket._id()).get().get();
		assertEquals(0, status_after.node_affinity().size());
		assertEquals(true, status_after.suspended());
		assertTrue("The file path has been built", new File(System.getProperty("java.io.tmpdir") + valid_bucket.full_name() + "/managed_bucket").exists());
		assertEquals(1L, (long)_bucket_crud.countObjects().get());
	}
	
	@Test
	public void testAnotherSemiFailedBucketCreation_singleNode() throws Exception {
		cleanDatabases();

		// Setup: register a refuse-then-accept
		final String refusing_host1 = insertActor(TestActor_Accepter_Refuser.class);
		final String refusing_host2 = insertActor(TestActor_Accepter_Refuser.class);
		assertFalse("created actors on different hosts", refusing_host1.equals(refusing_host2));
		
		// 0) Start with a valid bucket:
		
		final DataBucketBean valid_bucket = 
				BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::_id, "id1")
				.with(DataBucketBean::full_name, "/name1/embedded/dir/")
				.with(DataBucketBean::display_name, "name1")
				.with(DataBucketBean::created, new Date())
				.with(DataBucketBean::modified, new Date())
				.with(DataBucketBean::owner_id, "owner1")
				.with(DataBucketBean::multi_node_enabled, true) 
				.with(DataBucketBean::access_rights, BeanTemplateUtils.build(AuthorizationBean.class).done().get())
				.with(DataBucketBean::harvest_technology_name_or_id, "harvest_tech")
				.with(DataBucketBean::harvest_configs, 
						Arrays.asList(BeanTemplateUtils.build(HarvestControlMetadataBean.class)
								.with(HarvestControlMetadataBean::enabled, true)
								.done().get()))
				.with(DataBucketBean::access_rights, BeanTemplateUtils.build(AuthorizationBean.class).done().get())
				.done().get();

		//(delete the file path)
		try {
			FileUtils.deleteDirectory(new File(System.getProperty("java.io.tmpdir") + valid_bucket.full_name()));
		}
		catch (Exception e) {} // (fine, dir prob dones't delete)
		assertFalse("The file path has been deleted", new File(System.getProperty("java.io.tmpdir") + valid_bucket.full_name() + "/managed_bucket").exists());
		
		//(add the status object and try)
		final DataBucketStatusBean status = 
				BeanTemplateUtils.build(DataBucketStatusBean.class)
				.with(DataBucketStatusBean::_id, valid_bucket._id())
				.with(DataBucketStatusBean::bucket_path, valid_bucket.full_name())
				.with(DataBucketStatusBean::suspended, false)
				.done().get();
		
		assertEquals(0L, (long)_bucket_status_crud.countObjects().get());
		_bucket_status_crud.storeObject(status).get();
		assertEquals(1L, (long)_bucket_status_crud.countObjects().get());

		assertEquals(0L, (long)_bucket_crud.countObjects().get());
		final ManagementFuture<Supplier<Object>> insert_future = _bucket_crud.storeObject(valid_bucket);
		final BasicMessageBean err_msg = insert_future.getManagementResults().get().iterator().next();
		assertEquals(false, err_msg.success());
		assertEquals(ErrorUtils.get(ManagementDbErrorUtils.NO_DATA_IMPORT_MANAGERS_STARTED_SUSPENDED, valid_bucket.full_name()), err_msg.message());
		assertEquals(valid_bucket._id(), insert_future.get().get());
		final DataBucketStatusBean status_after = _bucket_status_crud.getObjectById(valid_bucket._id()).get().get();
		assertEquals(0, status_after.node_affinity().size());
		assertEquals(true, status_after.suspended());
		assertTrue("The file path has been built", new File(System.getProperty("java.io.tmpdir") + valid_bucket.full_name() + "/managed_bucket").exists());
		assertEquals(1L, (long)_bucket_crud.countObjects().get());
	}
	
	///////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////
	
	@Test
	public void testSuccessfulBucketCreation_singleNode() throws Exception {
		cleanDatabases();

		// Setup: register an accepting actor to listen:
		final String accepting_host1 = insertActor(TestActor_Accepter.class);
		final String accepting_host2 = insertActor(TestActor_Accepter.class);
		assertFalse("created actors on different hosts", accepting_host1.equals(accepting_host2));
		
		// 0) Start with a valid bucket:
		
		final DataBucketBean valid_bucket = 
				BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::_id, "id1")
				.with(DataBucketBean::full_name, "/name1/embedded/dir/")
				.with(DataBucketBean::display_name, "name1")
				.with(DataBucketBean::created, new Date())
				.with(DataBucketBean::modified, new Date())
				.with(DataBucketBean::owner_id, "owner1")
				.with(DataBucketBean::multi_node_enabled, false) 
				.with(DataBucketBean::access_rights, BeanTemplateUtils.build(AuthorizationBean.class).done().get())
				.with(DataBucketBean::harvest_technology_name_or_id, "harvest_tech")
				.with(DataBucketBean::harvest_configs, 
						Arrays.asList(BeanTemplateUtils.build(HarvestControlMetadataBean.class)
								.with(HarvestControlMetadataBean::enabled, true)
								.done().get()))
				.done().get();

		//(delete the file path)
		try {
			FileUtils.deleteDirectory(new File(System.getProperty("java.io.tmpdir") + valid_bucket.full_name()));
		}
		catch (Exception e) {} // (fine, dir prob dones't delete)
		assertFalse("The file path has been deleted", new File(System.getProperty("java.io.tmpdir") + valid_bucket.full_name() + "/managed_bucket").exists());
		
		//(add the status object and try)
		final DataBucketStatusBean status = 
				BeanTemplateUtils.build(DataBucketStatusBean.class)
				.with(DataBucketStatusBean::_id, valid_bucket._id())
				.with(DataBucketStatusBean::bucket_path, valid_bucket.full_name())
				.with(DataBucketStatusBean::suspended, false)
				.done().get();
		
		assertEquals(0L, (long)_bucket_status_crud.countObjects().get());
		_bucket_status_crud.storeObject(status).get();
		assertEquals(1L, (long)_bucket_status_crud.countObjects().get());

		// Try again, assert - sort of works this time, creates the bucket but in suspended mode because there are no registered data import managers
		assertEquals(0L, (long)_bucket_crud.countObjects().get());
		final ManagementFuture<Supplier<Object>> insert_future = _bucket_crud.storeObject(valid_bucket);
		final BasicMessageBean err_msg = insert_future.getManagementResults().get().iterator().next();
		assertEquals(true, err_msg.success());
		assertEquals(valid_bucket._id(), insert_future.get().get());
		final DataBucketStatusBean status_after = _bucket_status_crud.getObjectById(valid_bucket._id()).get().get();
		assertEquals(1, status_after.node_affinity().size());
		assertTrue("Check the node affinity is correct: ", status_after.node_affinity().contains(accepting_host1) || status_after.node_affinity().contains(accepting_host2));
		assertEquals(false, status_after.suspended());
		assertTrue("The file path has been built", new File(System.getProperty("java.io.tmpdir") + valid_bucket.full_name() + "/managed_bucket").exists());
		assertEquals(1L, (long)_bucket_crud.countObjects().get());
	}

	@Test
	public void testSuccessfulBucketCreation_multiNode() throws Exception {
		cleanDatabases();

		// Setup: register an accepting actor to listen:
		final String accepting_host1 = insertActor(TestActor_Accepter.class);
		final String accepting_host2 = insertActor(TestActor_Accepter.class);
		assertFalse("created actors on different hosts", accepting_host1.equals(accepting_host2));
		
		// 0) Start with a valid bucket:
		
		final DataBucketBean valid_bucket = 
				BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::_id, "id1")
				.with(DataBucketBean::full_name, "/name1/embedded/dir/")
				.with(DataBucketBean::display_name, "name1")
				.with(DataBucketBean::created, new Date())
				.with(DataBucketBean::modified, new Date())
				.with(DataBucketBean::owner_id, "owner1")
				.with(DataBucketBean::multi_node_enabled, true) 
				.with(DataBucketBean::access_rights, BeanTemplateUtils.build(AuthorizationBean.class).done().get())
				.with(DataBucketBean::harvest_technology_name_or_id, "harvest_tech")
				.with(DataBucketBean::harvest_configs, 
						Arrays.asList(BeanTemplateUtils.build(HarvestControlMetadataBean.class)
								.with(HarvestControlMetadataBean::enabled, true)
								.done().get()))
				.done().get();

		//(delete the file path)
		try {
			FileUtils.deleteDirectory(new File(System.getProperty("java.io.tmpdir") + valid_bucket.full_name()));
		}
		catch (Exception e) {} // (fine, dir prob dones't delete)
		assertFalse("The file path has been deleted", new File(System.getProperty("java.io.tmpdir") + valid_bucket.full_name() + "/managed_bucket").exists());
		
		//(add the status object and try)
		final DataBucketStatusBean status = 
				BeanTemplateUtils.build(DataBucketStatusBean.class)
				.with(DataBucketStatusBean::_id, valid_bucket._id())
				.with(DataBucketStatusBean::bucket_path, valid_bucket.full_name())
				.with(DataBucketStatusBean::suspended, false)
				.done().get();
		
		assertEquals(0L, (long)_bucket_status_crud.countObjects().get());
		_bucket_status_crud.storeObject(status).get();
		assertEquals(1L, (long)_bucket_status_crud.countObjects().get());

		// Try again, assert - sort of works this time, creates the bucket but in suspended mode because there are no registered data import managers
		assertEquals(0L, (long)_bucket_crud.countObjects().get());
		final ManagementFuture<Supplier<Object>> insert_future = _bucket_crud.storeObject(valid_bucket);
		final BasicMessageBean err_msg = insert_future.getManagementResults().get().iterator().next();
		assertEquals(true, err_msg.success());
		assertEquals(valid_bucket._id(), insert_future.get().get());
		final DataBucketStatusBean status_after = _bucket_status_crud.getObjectById(valid_bucket._id()).get().get();
		assertEquals(2, status_after.node_affinity().size());
		assertTrue("Check the node affinity is correct: ", status_after.node_affinity().contains(accepting_host1) && status_after.node_affinity().contains(accepting_host2));
		assertEquals(false, status_after.suspended());
		assertTrue("The file path has been built", new File(System.getProperty("java.io.tmpdir") + valid_bucket.full_name() + "/managed_bucket").exists());
		assertEquals(1L, (long)_bucket_crud.countObjects().get());
	}

	///////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////
	
	@Test
	public void testUpdateValidation() throws Exception {
		
		// Insert a bucket:
		testSuccessfulBucketCreation_multiNode();		
		
		// Try to update it
		
		final DataBucketBean bucket = _bucket_crud.getObjectById("id1").get().get();
		
		final DataBucketBean mod_bucket1 = BeanTemplateUtils.clone(bucket)
											.with(DataBucketBean::full_name, "Something else")
											.with(DataBucketBean::owner_id, "Someone else")
											.done();
		
		// First attempt: will fail because not trying to overwrite:
		
		final ManagementFuture<Supplier<Object>> update_future = _bucket_crud.storeObject(mod_bucket1);
		
		try {
			update_future.get();
			fail("Should have thrown exception");
		}
		catch (Exception e) {
			assertTrue("Dup key error", e.getCause() instanceof MongoException);
		}
		
		// Second attempt: fail on validation
		final ManagementFuture<Supplier<Object>> update_future2 = _bucket_crud.storeObject(mod_bucket1, true);
		
		try {
			assertEquals(2, update_future2.getManagementResults().get().size()); // (2 errors)
			update_future.get();
			fail("Should have thrown exception");
		}
		catch (Exception e) {
			assertTrue("Validation error", e.getCause() instanceof RuntimeException);
		}
		
		// Third attempt, succeed with different update

		final DataBucketBean mod_bucket3 = BeanTemplateUtils.clone(bucket)
				.with(DataBucketBean::display_name, "Something else")
				.done();
		
		final ManagementFuture<Supplier<Object>> update_future3 = _bucket_crud.storeObject(mod_bucket3, true);
		assertEquals("id1", update_future3.get().get());

		final DataBucketBean bucket3 = _bucket_crud.getObjectById("id1").get().get();
		assertEquals("Something else", bucket3.display_name());
		
		//(just quickly check node affinity didn't change)
		final DataBucketStatusBean status_after = _bucket_status_crud.getObjectById("id1").get().get();
		assertEquals(2, status_after.node_affinity().size());
	}
}
