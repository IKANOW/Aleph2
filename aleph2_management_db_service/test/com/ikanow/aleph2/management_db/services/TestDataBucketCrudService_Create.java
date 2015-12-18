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
package com.ikanow.aleph2.management_db.services;

import static org.junit.Assert.*;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;

import com.google.common.collect.ImmutableMap;
import com.ikanow.aleph2.data_model.interfaces.data_services.IColumnarService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IDocumentService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.data_services.ITemporalService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.MockSecurityService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.MockServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean.MasterEnrichmentType;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.DataWarehouseSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.GeospatialSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.GraphSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.data_import.HarvestControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;
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
import com.ikanow.aleph2.management_db.utils.BucketValidationUtils;
import com.ikanow.aleph2.management_db.utils.ManagementDbErrorUtils;
import com.ikanow.aleph2.shared.crud.mongodb.services.MockMongoDbCrudServiceFactory;
import com.ikanow.aleph2.storage_service_hdfs.services.MockHdfsStorageService;
import com.mongodb.MongoException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;

import scala.Tuple2;

public class TestDataBucketCrudService_Create {

	public static final Logger _logger = LogManager.getLogger(TestDataBucketCrudService_Create.class);	
	
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
	
	@SuppressWarnings("deprecation")
	@Before
	public void setup() throws Exception {
		ModuleUtils.disableTestInjection();
		
		// Here's the setup that Guice normally gives you....
		final String tmpdir = System.getProperty("java.io.tmpdir") + File.separator;
		_globals = new GlobalPropertiesBean(tmpdir, tmpdir, tmpdir, tmpdir);
		_storage_service = new MockHdfsStorageService(_globals);
		_mock_service_context = new MockServiceContext();		
		_crud_factory = new MockMongoDbCrudServiceFactory();
		_underlying_db_service = new MockMongoDbManagementDbService(_crud_factory, new MongoDbManagementDbConfigBean(false), null, null, null, null);
		_core_distributed_services = new MockCoreDistributedServices();
		_mock_service_context.addGlobals(new GlobalPropertiesBean(null, null, null, null));
		_mock_service_context.addService(IManagementDbService.class, Optional.empty(), _underlying_db_service);
		_mock_service_context.addService(ICoreDistributedServices.class, Optional.empty(), _core_distributed_services);
		_mock_service_context.addService(IStorageService.class, Optional.empty(),_storage_service);
		_mock_service_context.addService(ISecurityService.class, Optional.empty(), new MockSecurityService());
		_db_actor_context = new ManagementDbActorContext(_mock_service_context, true);

		_bucket_crud = new DataBucketCrudService(_mock_service_context, _db_actor_context);
		_bucket_status_crud = new DataBucketStatusCrudService(_mock_service_context, _db_actor_context);
		_shared_library_crud = new SharedLibraryCrudService(_mock_service_context);
		_core_db_service = new CoreManagementDbService(_mock_service_context, _bucket_crud, _bucket_status_crud, _shared_library_crud, _db_actor_context);
		
		_mock_service_context.addService(IManagementDbService.class, IManagementDbService.CORE_MANAGEMENT_DB, _core_db_service);		
		
		_bucket_crud.initialize();
		_bucket_status_crud.initialize();
		_underlying_bucket_crud = _bucket_crud._underlying_data_bucket_db.get();
		_underlying_bucket_status_crud = _bucket_crud._underlying_data_bucket_status_db.get();
		_bucket_action_retry_store = _bucket_crud._bucket_action_retry_store.get();
	}	
	
	@Test
	public void test_Setup() {
		if (File.separator.equals("\\")) { // windows mode!
			assertTrue("WINDOWS MODE: hadoop home needs to be set (use -Dhadoop.home.dir={HADOOP_HOME} in JAVA_OPTS)", null != System.getProperty("hadoop.home.dir"));
			assertTrue("WINDOWS MODE: hadoop home needs to exist: " + System.getProperty("hadoop.home.dir"), null != System.getProperty("hadoop.home.dir"));
		}		
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
	
	public String insertAnalyticsActor(Class<? extends UntypedActor> actor_clazz) throws Exception {
		String uuid = UuidUtils.get().getRandomUuid();
		ManagementDbActorContext.get().getDistributedServices()
			.getCuratorFramework().create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
			.forPath(ActorUtils.BUCKET_ANALYTICS_ZOOKEEPER + "/" + uuid);
		
		ActorRef handler = ManagementDbActorContext.get().getActorSystem().actorOf(Props.create(actor_clazz, uuid), uuid);
		ManagementDbActorContext.get().getAnalyticsMessageBus().subscribe(handler, ActorUtils.BUCKET_ANALYTICS_ZOOKEEPER);

		return uuid;
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
	public void test_ValidateSchemas() {
		// 0) Start with a valid bucket:
		
		final DataBucketBean valid_bucket = 
				BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::_id, "id1")
				.with(DataBucketBean::full_name, "/name1/embedded/dir")
				.with(DataBucketBean::display_name, "name1")
				.with(DataBucketBean::created, new Date())
				.with(DataBucketBean::modified, new Date())
				.with(DataBucketBean::owner_id, "owner1")
				.done().get();				
		
		final DataSchemaBean enabled_schema = BeanTemplateUtils.build(DataSchemaBean.class)
												.with(DataSchemaBean::columnar_schema, 
														BeanTemplateUtils.build(DataSchemaBean.ColumnarSchemaBean.class).with(DataSchemaBean.ColumnarSchemaBean::enabled, true).done().get())
												.with(DataSchemaBean::document_schema, 
														BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class).with(DataSchemaBean.DocumentSchemaBean::enabled, true).done().get())
												.with(DataSchemaBean::search_index_schema, 
														BeanTemplateUtils.build(DataSchemaBean.SearchIndexSchemaBean.class).with(DataSchemaBean.SearchIndexSchemaBean::enabled, true).done().get())
												.with(DataSchemaBean::storage_schema, 
														BeanTemplateUtils.build(DataSchemaBean.StorageSchemaBean.class).with(DataSchemaBean.StorageSchemaBean::enabled, true).done().get())
												.with(DataSchemaBean::temporal_schema, 
														BeanTemplateUtils.build(DataSchemaBean.TemporalSchemaBean.class).with(DataSchemaBean.TemporalSchemaBean::enabled, true).done().get())
												.done().get();
		
		final DataBucketBean bucket_with_schema = BeanTemplateUtils.clone(valid_bucket)
													.with(DataBucketBean::data_schema, enabled_schema).done();
		
		// Test validation runs:

		MockServiceContext test_context = new MockServiceContext();
		
		IColumnarService service1 = Mockito.mock(IColumnarService.class);
		Mockito.when(service1.validateSchema(Matchers.any(), Matchers.any())).thenReturn(Tuples._2T("t1", Arrays.asList(new BasicMessageBean(null, true, null, null, null, null, null))));
		test_context.addService(IColumnarService.class, Optional.empty(), service1);

		IDocumentService service2 = Mockito.mock(IDocumentService.class);
		Mockito.when(service2.validateSchema(Matchers.any(), Matchers.any())).thenReturn(Tuples._2T("t2", Arrays.asList(new BasicMessageBean(null, true, null, null, null, null, null))));
		test_context.addService(IDocumentService.class, Optional.empty(), service2);

		ISearchIndexService service3 = Mockito.mock(ISearchIndexService.class);
		Mockito.when(service3.validateSchema(Matchers.any(), Matchers.any())).thenReturn(Tuples._2T("t3", Arrays.asList(new BasicMessageBean(null, true, null, null, null, null, null))));
		test_context.addService(ISearchIndexService.class, Optional.empty(), service3);

		IStorageService service4 = Mockito.mock(IStorageService.class);
		Mockito.when(service4.validateSchema(Matchers.any(), Matchers.any())).thenReturn(Tuples._2T("t4", Arrays.asList(new BasicMessageBean(null, true, null, null, null, null, null))));
		test_context.addService(IStorageService.class, Optional.empty(), service4);

		ITemporalService service5 = Mockito.mock(ITemporalService.class);
		Mockito.when(service5.validateSchema(Matchers.any(), Matchers.any())).thenReturn(Tuples._2T("t5", Arrays.asList(new BasicMessageBean(null, true, null, null, null, null, null))));
		test_context.addService(ITemporalService.class, Optional.empty(), service5);

		final Tuple2<Map<String, String>, List<BasicMessageBean>> results1 = BucketValidationUtils.validateSchema(bucket_with_schema, test_context);
		assertEquals(5, results1._2().size());
		assertTrue("All returned success==true", results1._2().stream().allMatch(m -> m.success()));
		assertEquals(5, results1._1().size());
		assertEquals(ImmutableMap.<String, String>builder()
				.put("columnar_schema", "t1").put("document_schema", "t2").put("search_index_schema", "t3").put("storage_schema", "t4").put("temporal_schema", "t5")
				.build()
				,
				results1._1());
		
		// Not present tests:
		
		final DataSchemaBean enabled_by_default_schema = BeanTemplateUtils.build(DataSchemaBean.class)
				.with(DataSchemaBean::columnar_schema, 
						BeanTemplateUtils.build(DataSchemaBean.ColumnarSchemaBean.class).done().get())
				.with(DataSchemaBean::document_schema, 
						BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class).done().get())
				.with(DataSchemaBean::search_index_schema, 
						BeanTemplateUtils.build(DataSchemaBean.SearchIndexSchemaBean.class).done().get())
				.with(DataSchemaBean::storage_schema, 
						BeanTemplateUtils.build(DataSchemaBean.StorageSchemaBean.class).done().get())
				.with(DataSchemaBean::temporal_schema, 
						BeanTemplateUtils.build(DataSchemaBean.TemporalSchemaBean.class).done().get())
				.with(DataSchemaBean::data_warehouse_schema, 
						BeanTemplateUtils.build(DataWarehouseSchemaBean.class).done().get())
				.with(DataSchemaBean::graph_schema, 
						BeanTemplateUtils.build(GraphSchemaBean.class).done().get())
				.with(DataSchemaBean::geospatial_schema, 
						BeanTemplateUtils.build(GeospatialSchemaBean.class).done().get())
				.done().get();
		
		final DataBucketBean bucket_with_other_schema = BeanTemplateUtils.clone(valid_bucket)
				.with(DataBucketBean::data_schema, enabled_by_default_schema).done();
		
		MockServiceContext test_context2 = new MockServiceContext();
		
		final List<BasicMessageBean> results2 = BucketValidationUtils.validateSchema(bucket_with_other_schema, test_context2)._2();
		assertEquals(8, results2.size());
		
		assertTrue("All returned success==false", results2.stream().allMatch(m -> !m.success()));		
		
		// Failure - but ignored because enabled turned off
		
		final DataSchemaBean not_enabled_schema = BeanTemplateUtils.build(DataSchemaBean.class)
				.with(DataSchemaBean::columnar_schema, 
						BeanTemplateUtils.build(DataSchemaBean.ColumnarSchemaBean.class).with(DataSchemaBean.ColumnarSchemaBean::enabled, false).done().get())
				.with(DataSchemaBean::document_schema, 
						BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class).with(DataSchemaBean.DocumentSchemaBean::enabled, false).done().get())
				.with(DataSchemaBean::search_index_schema, 
						BeanTemplateUtils.build(DataSchemaBean.SearchIndexSchemaBean.class).with(DataSchemaBean.SearchIndexSchemaBean::enabled, false).done().get())
				.with(DataSchemaBean::storage_schema, 
						BeanTemplateUtils.build(DataSchemaBean.StorageSchemaBean.class).with(DataSchemaBean.StorageSchemaBean::enabled, false).done().get())
				.with(DataSchemaBean::temporal_schema, 
						BeanTemplateUtils.build(DataSchemaBean.TemporalSchemaBean.class).with(DataSchemaBean.TemporalSchemaBean::enabled, false).done().get())
				.done().get();
		
		final DataBucketBean bucket_with_disabled_schema = BeanTemplateUtils.clone(valid_bucket)
				.with(DataBucketBean::data_schema, not_enabled_schema).done();		
		
		final List<BasicMessageBean> results3 = BucketValidationUtils.validateSchema(bucket_with_disabled_schema, test_context2)._2();
		assertEquals(0, results3.size());
	}
	
	@Test
	public void test_ValidateTimes_pass() throws Exception {
		cleanDatabases();

		// 0) Start with a valid bucket:
		
		final DataBucketBean valid_bucket = 
				BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::_id, "id1")
				.with(DataBucketBean::full_name, "/name1/embedded/dir")
				.with(DataBucketBean::display_name, "name1")
				.with(DataBucketBean::created, new Date())
				.with(DataBucketBean::modified, new Date())
				.with(DataBucketBean::owner_id, "owner1")
				.done().get();

		
		// 1) add all errors:
		{
			final DataBucketBean valid_times = BeanTemplateUtils.clone(valid_bucket)
													.with(DataBucketBean::poll_frequency, "every 1 hour")
													.with(DataBucketBean::data_schema,
														BeanTemplateUtils.build(DataSchemaBean.class)
															.with(DataSchemaBean::temporal_schema, 
																BeanTemplateUtils.build(DataSchemaBean.TemporalSchemaBean.class)
																	.with(DataSchemaBean.TemporalSchemaBean::grouping_time_period, "month")
																	.with(DataSchemaBean.TemporalSchemaBean::cold_age_max, "30d")
																	.with(DataSchemaBean.TemporalSchemaBean::hot_age_max, "4 weeks")
																	.with(DataSchemaBean.TemporalSchemaBean::exist_age_max, "1 month")
																	.done().get()
															)
															.with(DataSchemaBean::storage_schema, 
																BeanTemplateUtils.build(DataSchemaBean.StorageSchemaBean.class)
																	.with(DataSchemaBean.StorageSchemaBean::json, 
																			BeanTemplateUtils.build(DataSchemaBean.StorageSchemaBean.StorageSubSchemaBean.class)
																				.with(DataSchemaBean.StorageSchemaBean.StorageSubSchemaBean::grouping_time_period, "day")
																				.with(DataSchemaBean.StorageSchemaBean.StorageSubSchemaBean::exist_age_max, "100s")
																			.done().get())
																	.with(DataSchemaBean.StorageSchemaBean::raw, 
																			BeanTemplateUtils.build(DataSchemaBean.StorageSchemaBean.StorageSubSchemaBean.class)
																				.with(DataSchemaBean.StorageSchemaBean.StorageSubSchemaBean::grouping_time_period, "year")
																				.with(DataSchemaBean.StorageSchemaBean.StorageSubSchemaBean::exist_age_max, "10d")
																			.done().get())
																	.with(DataSchemaBean.StorageSchemaBean::processed, 
																			BeanTemplateUtils.build(DataSchemaBean.StorageSchemaBean.StorageSubSchemaBean.class)
																				.with(DataSchemaBean.StorageSchemaBean.StorageSubSchemaBean::grouping_time_period, "hour")
																				.with(DataSchemaBean.StorageSchemaBean.StorageSubSchemaBean::exist_age_max, "next year")
																			.done().get())
																	.done().get()
															)
															.done().get()
													)												
													.done();
			
			//(delete the file path)
			try {
				FileUtils.deleteDirectory(new File(System.getProperty("java.io.tmpdir") + File.separator + "data" + File.separator + valid_bucket.full_name()));
			}
			catch (Exception e) {} // (fine, dir prob dones't delete)
			assertFalse("The file path has been deleted", new File(System.getProperty("java.io.tmpdir") + File.separator + "data" + File.separator + valid_bucket.full_name() + "/managed_bucket").exists());
			
			try {
				final ManagementFuture<Supplier<Object>> result = _bucket_crud.storeObject(valid_times);
				
				assertEquals(1, result.getManagementResults().get().size());			
				result.get();
				fail("Should have thrown an exception");
			}
			catch (Exception e) {
				System.out.println("expected, err=" + e.getCause().getMessage());
				assertEquals(RuntimeException.class, e.getCause().getClass());
			}
		}
	}	
	
	@Test
	public void test_ValidateTimes_fail() throws Exception {
		cleanDatabases();

		// 0) Start with a valid bucket:
		
		final DataBucketBean valid_bucket = 
				BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::_id, "id1")
				.with(DataBucketBean::full_name, "/name1/embedded/dir")
				.with(DataBucketBean::display_name, "name1")
				.with(DataBucketBean::created, new Date())
				.with(DataBucketBean::modified, new Date())
				.with(DataBucketBean::owner_id, "owner1")
				.done().get();

		
		// 1) add all errors:
		{
			final DataBucketBean invalid_times = BeanTemplateUtils.clone(valid_bucket)
													.with(DataBucketBean::poll_frequency, "apple")
													.with(DataBucketBean::data_schema,
														BeanTemplateUtils.build(DataSchemaBean.class)
															.with(DataSchemaBean::temporal_schema, 
																BeanTemplateUtils.build(DataSchemaBean.TemporalSchemaBean.class)
																	.with(DataSchemaBean.TemporalSchemaBean::grouping_time_period, "orange")
																	.with(DataSchemaBean.TemporalSchemaBean::cold_age_max, "pear")
																	.with(DataSchemaBean.TemporalSchemaBean::hot_age_max, "mango")
																	.with(DataSchemaBean.TemporalSchemaBean::exist_age_max, "banana!")
																	.done().get()
															)
															.with(DataSchemaBean::storage_schema, 
																BeanTemplateUtils.build(DataSchemaBean.StorageSchemaBean.class)
																	.with(DataSchemaBean.StorageSchemaBean::json, 
																			BeanTemplateUtils.build(DataSchemaBean.StorageSchemaBean.StorageSubSchemaBean.class)
																				.with(DataSchemaBean.StorageSchemaBean.StorageSubSchemaBean::grouping_time_period, "tomato")
																				.with(DataSchemaBean.StorageSchemaBean.StorageSubSchemaBean::exist_age_max, "kiwi")
																			.done().get())
																	.with(DataSchemaBean.StorageSchemaBean::raw, 
																			BeanTemplateUtils.build(DataSchemaBean.StorageSchemaBean.StorageSubSchemaBean.class)
																				.with(DataSchemaBean.StorageSchemaBean.StorageSubSchemaBean::grouping_time_period, "pomegranate")
																				.with(DataSchemaBean.StorageSchemaBean.StorageSubSchemaBean::exist_age_max, "pineapple")
																			.done().get())
																	.with(DataSchemaBean.StorageSchemaBean::processed, 
																			BeanTemplateUtils.build(DataSchemaBean.StorageSchemaBean.StorageSubSchemaBean.class)
																				.with(DataSchemaBean.StorageSchemaBean.StorageSubSchemaBean::grouping_time_period, "lychee")
																				.with(DataSchemaBean.StorageSchemaBean.StorageSubSchemaBean::exist_age_max, "grapefruit")
																			.done().get())
																	.done().get()
															)
															.done().get()
													)												
													.done();
			
			//(delete the file path)
			try {
				FileUtils.deleteDirectory(new File(System.getProperty("java.io.tmpdir") + File.separator + "data" + File.separator + valid_bucket.full_name()));
			}
			catch (Exception e) {} // (fine, dir prob dones't delete)
			assertFalse("The file path has been deleted", new File(System.getProperty("java.io.tmpdir") + File.separator + "data" + File.separator + valid_bucket.full_name() + "/managed_bucket").exists());
			
			final ManagementFuture<Supplier<Object>> result = _bucket_crud.storeObject(invalid_times);
			try {
				assertEquals(12, result.getManagementResults().get().size());
				result.get();
				fail("Should have thrown an exception");
			}
			catch (Exception e) {
				System.out.println("expected, err=" + e.getCause().getMessage());
				assertEquals(RuntimeException.class, e.getCause().getClass());
			}
		}		
	}	
	
	@Test
	public void test_ValidateName() throws Exception {
		cleanDatabases();

		// 0) Start with a valid bucket:
		
		final DataBucketBean valid_bucket = 
				BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::_id, "id1")
				.with(DataBucketBean::full_name, "/name1/embedded/dir")
				.with(DataBucketBean::display_name, "name1")
				.with(DataBucketBean::created, new Date())
				.with(DataBucketBean::modified, new Date())
				.with(DataBucketBean::owner_id, "owner1")
				.done().get();

		//(delete the file path)
		try {
			FileUtils.deleteDirectory(new File(System.getProperty("java.io.tmpdir") + File.separator + "data" + File.separator + valid_bucket.full_name()));
		}
		catch (Exception e) {} // (fine, dir prob dones't delete)
		assertFalse("The file path has been deleted", new File(System.getProperty("java.io.tmpdir") + File.separator + "data" + File.separator + valid_bucket.full_name() + "/managed_bucket").exists());

		// Try out the various ways in which it should be broken

		{
			final List<String> invalid_names = Arrays.asList("name1/embedded/dir", "/name1/embedded/dir/", "/name1/embedded//dir", 
															"/name1/../dir", "/name1/./dir", "/name1/embedded/dir/.", "/name1/embedded/dir/..",
															"/name1/embedded/ dir", "/name1/embedded/:dir", "/name1/embedded/;dir", "/name1/embedded/,dir"
															);
			for (String invalid_name: invalid_names) {
				final ManagementFuture<Supplier<Object>> result = _bucket_crud.storeObject(BeanTemplateUtils.clone(valid_bucket).with(DataBucketBean::full_name, invalid_name).done());
				try {
					assertEquals(1, result.getManagementResults().get().size());
					assertEquals(ErrorUtils.get(ManagementDbErrorUtils.BUCKET_FULL_NAME_FORMAT_ERROR, invalid_name), result.getManagementResults().get().iterator().next().message());
					result.get();
					fail("Should have thrown an exception");
				}
				catch (Exception e) {
					System.out.println("expected, err=" + e.getCause().getMessage());
					assertEquals(RuntimeException.class, e.getCause().getClass());
				}
			}
		}
	}	
	
	@Test
	public void test_ValidateInsert() throws Exception {
		cleanDatabases();

		// 0) Start with a valid bucket:
		
		final DataBucketBean valid_bucket = 
				BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::_id, "id1")
				.with(DataBucketBean::full_name, "/name1/embedded/dir")
				.with(DataBucketBean::display_name, "name1")
				.with(DataBucketBean::created, new Date())
				.with(DataBucketBean::modified, new Date())
				.with(DataBucketBean::owner_id, "owner1")
				.done().get();

		//(delete the file path)
		try {
			FileUtils.deleteDirectory(new File(System.getProperty("java.io.tmpdir") + File.separator + "data" + File.separator + valid_bucket.full_name()));
		}
		catch (Exception e) {} // (fine, dir prob dones't delete)
		assertFalse("The file path has been deleted", new File(System.getProperty("java.io.tmpdir") + File.separator + "data" + File.separator + valid_bucket.full_name() + "/managed_bucket").exists());
		
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
			// (note the replace first, checks bucket full name has been normalized)
		assertEquals(valid_bucket._id(), insert_future.get().get());
		final DataBucketStatusBean status_after = _bucket_status_crud.getObjectById(valid_bucket._id()).get().get();
		assertEquals(0, status_after.node_affinity().size());
		assertEquals(true, status_after.suspended());
		assertTrue("The file path has been built", new File(System.getProperty("java.io.tmpdir") + File.separator + "data" + File.separator + valid_bucket.full_name() + "/managed_bucket").exists());
		assertEquals(1L, (long)_bucket_crud.countObjects().get());
		assertEquals(valid_bucket.full_name(), status_after.bucket_path()); // (check has been normalized)
		
		//////////////////////////
		
		// Validation _errors_
		
		// 2) Missing field
		
		final DataBucketBean new_valid_bucket =  BeanTemplateUtils.clone(valid_bucket).with(DataBucketBean::_id, "id2").done();
				
		try {
			final DataBucketBean bucket = BeanTemplateUtils.clone(new_valid_bucket).with(DataBucketBean::full_name, null).done();
			final ManagementFuture<Supplier<Object>> result = _bucket_crud.storeObject(bucket);
			assertTrue("Should have errors", !result.getManagementResults().get().isEmpty());
			
			System.out.println("Mgmt side channels = " + result.getManagementResults().get().stream().map(m->m.message()).collect(Collectors.joining(" ; ")));
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
			
			System.out.println("Mgmt side channels = " + result.getManagementResults().get().stream().map(m->m.message()).collect(Collectors.joining(" ; ")));
			result.get();
			fail("Should have thrown an exception");
		}
		catch (Exception e) {
			System.out.println("expected, err=" + e.getCause().getMessage());
			assertEquals(RuntimeException.class, e.getCause().getClass());
		}		
		assertEquals(1L, (long)_bucket_crud.countObjects().get());
		
		// EVERYTHING FROM HERE IS ALSO TESTED IN THE STATIC VERSION BELOW
		
		// 4) Enrichment but no harvest (THIS IS NOW FINE)
		
		try {
			final DataBucketBean bucket = BeanTemplateUtils.clone(new_valid_bucket)
					.with(DataBucketBean::full_name, "/validation/test4")
					.with(DataBucketBean::master_enrichment_type, DataBucketBean.MasterEnrichmentType.batch)
					.with(DataBucketBean::batch_enrichment_topology, 
							BeanTemplateUtils.build(EnrichmentControlMetadataBean.class).done().get()).done();
			final ManagementFuture<Supplier<Object>> result = _bucket_crud.storeObject(bucket);
			assertFalse("Not Got errors", !result.getManagementResults().get().isEmpty() && !result.getManagementResults().get().iterator().next().success());
			
			System.out.println("Mgmt side channels = " + result.getManagementResults().get().stream().map(m->m.message() + ":" + m.success()).collect(Collectors.joining(" ; ")));
			result.get();
		}
		catch (Exception e) {
			System.out.println("expected, err=" + e.getCause().getMessage());
			assertEquals(RuntimeException.class, e.getCause().getClass());
		}				
		assertEquals(1L, (long)_bucket_crud.countObjects().get());
		
		// 5) Enrichment but no type
		
		try {
			final DataBucketBean bucket = BeanTemplateUtils.clone(new_valid_bucket)
					.with(DataBucketBean::full_name, "/validation/test5")
					.with(DataBucketBean::harvest_technology_name_or_id, "harvest_tech")
					.with(DataBucketBean::harvest_configs, Arrays.asList(BeanTemplateUtils.build(HarvestControlMetadataBean.class).with(HarvestControlMetadataBean::enabled, true).done().get()))
					.with(DataBucketBean::batch_enrichment_topology, 
							BeanTemplateUtils.build(EnrichmentControlMetadataBean.class).done().get()).done();
			final ManagementFuture<Supplier<Object>> result = _bucket_crud.storeObject(bucket);
			assertTrue("Got errors", !result.getManagementResults().get().isEmpty() && !result.getManagementResults().get().iterator().next().success());
			
			System.out.println("Mgmt side channels = " + result.getManagementResults().get().stream().map(m->m.message()).collect(Collectors.joining(" ; ")));
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
					.with(DataBucketBean::full_name, "/validation/test6")
					.with(DataBucketBean::harvest_configs, Arrays.asList(BeanTemplateUtils.build(HarvestControlMetadataBean.class).with(HarvestControlMetadataBean::enabled, true).done().get()))
					.with(DataBucketBean::master_enrichment_type, DataBucketBean.MasterEnrichmentType.streaming)
					.with(DataBucketBean::batch_enrichment_topology, 
							BeanTemplateUtils.build(EnrichmentControlMetadataBean.class).done().get()).done();
			final ManagementFuture<Supplier<Object>> result = _bucket_crud.storeObject(bucket);
			assertTrue("Got errors", !result.getManagementResults().get().isEmpty() && !result.getManagementResults().get().iterator().next().success());
			
			System.out.println("Mgmt side channels = " + result.getManagementResults().get().stream().map(m->m.message()).collect(Collectors.joining(" ; ")));
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
					.with(DataBucketBean::full_name, "/validation/test7")
					.with(DataBucketBean::harvest_technology_name_or_id, "harvest_tech")
					.with(DataBucketBean::harvest_configs, Arrays.asList(BeanTemplateUtils.build(HarvestControlMetadataBean.class).with(HarvestControlMetadataBean::enabled, true).done().get()))
					.with(DataBucketBean::master_enrichment_type, DataBucketBean.MasterEnrichmentType.batch)
					.with(DataBucketBean::streaming_enrichment_topology, 
							BeanTemplateUtils.build(EnrichmentControlMetadataBean.class).done().get()).done();
			final ManagementFuture<Supplier<Object>> result = _bucket_crud.storeObject(bucket);
			assertTrue("Got errors", !result.getManagementResults().get().isEmpty() && !result.getManagementResults().get().iterator().next().success());

			System.out.println("Mgmt side channels = " + result.getManagementResults().get().stream().map(m->m.message()).collect(Collectors.joining(" ; ")));
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
					.with(DataBucketBean::full_name, "/validation/test8")
					.with(DataBucketBean::harvest_technology_name_or_id, "harvest_tech")
					.done();
			final ManagementFuture<Supplier<Object>> result = _bucket_crud.storeObject(bucket);
			assertTrue("Got errors", !result.getManagementResults().get().isEmpty() && !result.getManagementResults().get().iterator().next().success());
			
			System.out.println("Mgmt side channels = " + result.getManagementResults().get().stream().map(m->m.message()).collect(Collectors.joining(" ; ")));
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
					.with(DataBucketBean::full_name, "/validation/test9")
					.with(DataBucketBean::harvest_technology_name_or_id, "harvest_tech")
					.with(DataBucketBean::harvest_configs, Arrays.asList(
							BeanTemplateUtils.build(HarvestControlMetadataBean.class)
								.with(HarvestControlMetadataBean::enabled, true)
								.with(HarvestControlMetadataBean::library_names_or_ids, Arrays.asList("xxx", ""))
								.done().get()))
					.done();
			final ManagementFuture<Supplier<Object>> result = _bucket_crud.storeObject(bucket);
			assertTrue("Got errors", !result.getManagementResults().get().isEmpty() && !result.getManagementResults().get().iterator().next().success());
			
			System.out.println("Mgmt side channels = " + result.getManagementResults().get().stream().map(m->m.message()).collect(Collectors.joining(" ; ")));
			result.get();
			fail("Should have thrown an exception");
		}
		catch (Exception e) {
			System.out.println("expected, err=" + e.getCause().getMessage());
			assertEquals(RuntimeException.class, e.getCause().getClass());
		}		
		assertEquals(1L, (long)_bucket_crud.countObjects().get());

		//9a: check it's fine if entry point or module_name_or_id is inserted
		{
			final DataBucketBean bucket_9a = BeanTemplateUtils.clone(new_valid_bucket)
					.with(DataBucketBean::full_name, "/validation/test9a")
					.with(DataBucketBean::harvest_technology_name_or_id, "harvest_tech")
					.with(DataBucketBean::harvest_configs, Arrays.asList(
							BeanTemplateUtils.build(HarvestControlMetadataBean.class)
								.with(HarvestControlMetadataBean::enabled, true)
								.with(HarvestControlMetadataBean::entry_point, "test")
								.done().get()))
					.done();
			
			assertTrue("Bucket validation failed: " + BucketValidationUtils.staticValidation(bucket_9a, false).stream().map(m -> m.message()).collect(Collectors.toList()), 
					BucketValidationUtils.staticValidation(bucket_9a, false).isEmpty());
		}
		//9b: ditto but module_name_or_id
		{
			final DataBucketBean bucket_9b = BeanTemplateUtils.clone(new_valid_bucket)
					.with(DataBucketBean::full_name, "/validation/test9b")
					.with(DataBucketBean::harvest_technology_name_or_id, "harvest_tech")
					.with(DataBucketBean::harvest_configs, Arrays.asList(
							BeanTemplateUtils.build(HarvestControlMetadataBean.class)
								.with(HarvestControlMetadataBean::enabled, true)
								.with(HarvestControlMetadataBean::module_name_or_id, "test")
								.with(HarvestControlMetadataBean::library_names_or_ids, Arrays.asList("xxx", ""))
								.done().get()))
					.done();
			
			assertTrue("Bucket validation failed: " + BucketValidationUtils.staticValidation(bucket_9b, false).stream().map(m -> m.message()).collect(Collectors.toList()), 
					BucketValidationUtils.staticValidation(bucket_9b, false).isEmpty());			
		}		
		
		// 10) Missing batch config enabled:
		
		try {
			final DataBucketBean bucket = BeanTemplateUtils.clone(new_valid_bucket)
					.with(DataBucketBean::full_name, "/validation/test10b")
					.with(DataBucketBean::harvest_technology_name_or_id, "harvest_tech")
					.with(DataBucketBean::harvest_configs, Arrays.asList(BeanTemplateUtils.build(HarvestControlMetadataBean.class).with(HarvestControlMetadataBean::enabled, true).done().get()))
					.with(DataBucketBean::master_enrichment_type, DataBucketBean.MasterEnrichmentType.batch)
					.with(DataBucketBean::batch_enrichment_configs, 
							Arrays.asList(BeanTemplateUtils.build(EnrichmentControlMetadataBean.class).done().get()))
							.done();
			final ManagementFuture<Supplier<Object>> result = _bucket_crud.storeObject(bucket);
			assertTrue("Got errors", !result.getManagementResults().get().isEmpty() && !result.getManagementResults().get().iterator().next().success());
			
			System.out.println("Mgmt side channels = " + result.getManagementResults().get().stream().map(m->m.message()).collect(Collectors.joining(" ; ")));
			result.get();
			fail("Should have thrown an exception");
		}
		catch (Exception e) {
			System.out.println("expected, err=" + e.getCause().getMessage());
			assertEquals(RuntimeException.class, e.getCause().getClass());
		}				
		assertEquals(1L, (long)_bucket_crud.countObjects().get());			
		
		//10a: check it's fine if entry point or module_name_or_id is inserted
		{
			final DataBucketBean bucket_10a = BeanTemplateUtils.clone(new_valid_bucket)
					.with(DataBucketBean::full_name, "/validation/test10")
					.with(DataBucketBean::harvest_technology_name_or_id, "harvest_tech")
					.with(DataBucketBean::harvest_configs, Arrays.asList(BeanTemplateUtils.build(HarvestControlMetadataBean.class).with(HarvestControlMetadataBean::enabled, true).done().get()))
					.with(DataBucketBean::master_enrichment_type, DataBucketBean.MasterEnrichmentType.batch)
					.with(DataBucketBean::batch_enrichment_configs, 
							Arrays.asList(BeanTemplateUtils.build(EnrichmentControlMetadataBean.class)
									.with(EnrichmentControlMetadataBean::entry_point, "test")
									.done().get()))
							.done();
			
			assertTrue("Bucket validation failed: " + BucketValidationUtils.staticValidation(bucket_10a, false).stream().map(m -> m.message()).collect(Collectors.toList()), 
					BucketValidationUtils.staticValidation(bucket_10a, false).isEmpty());
		}
		//10b: ditto but module_name_or_id
		{
			final DataBucketBean bucket_10b = BeanTemplateUtils.clone(new_valid_bucket)
					.with(DataBucketBean::full_name, "/validation/test10")
					.with(DataBucketBean::harvest_technology_name_or_id, "harvest_tech")
					.with(DataBucketBean::harvest_configs, Arrays.asList(BeanTemplateUtils.build(HarvestControlMetadataBean.class).with(HarvestControlMetadataBean::enabled, true).done().get()))
					.with(DataBucketBean::master_enrichment_type, DataBucketBean.MasterEnrichmentType.batch)
					.with(DataBucketBean::batch_enrichment_configs, 
							Arrays.asList(BeanTemplateUtils.build(EnrichmentControlMetadataBean.class)
									.with(EnrichmentControlMetadataBean::module_name_or_id, "test")
									.done().get()))
							.done();
			
			assertTrue("Bucket validation failed: " + BucketValidationUtils.staticValidation(bucket_10b, false).stream().map(m -> m.message()).collect(Collectors.toList()), 
					BucketValidationUtils.staticValidation(bucket_10b, false).isEmpty());			
		}
		
		// 11) Missing streaming config enabled:
		
		try {
			final DataBucketBean bucket = BeanTemplateUtils.clone(new_valid_bucket)
					.with(DataBucketBean::full_name, "/validation/test11")
					.with(DataBucketBean::harvest_technology_name_or_id, "harvest_tech")
					.with(DataBucketBean::harvest_configs, Arrays.asList(BeanTemplateUtils.build(HarvestControlMetadataBean.class).with(HarvestControlMetadataBean::enabled, true).done().get()))
					.with(DataBucketBean::master_enrichment_type, DataBucketBean.MasterEnrichmentType.streaming)
					.with(DataBucketBean::streaming_enrichment_configs, 
							Arrays.asList(BeanTemplateUtils.build(EnrichmentControlMetadataBean.class).done().get()))
							.done();
			final ManagementFuture<Supplier<Object>> result = _bucket_crud.storeObject(bucket);
			assertTrue("Got errors", !result.getManagementResults().get().isEmpty() && !result.getManagementResults().get().iterator().next().success());
			
			System.out.println("Mgmt side channels = " + result.getManagementResults().get().stream().map(m->m.message()).collect(Collectors.joining(" ; ")));
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
					.with(DataBucketBean::full_name, "/validation/test12")
					.with(DataBucketBean::multi_bucket_children, new HashSet<String>(Arrays.asList("a", "b")))
					.with(DataBucketBean::harvest_technology_name_or_id, "harvest_tech")
					.with(DataBucketBean::harvest_configs, Arrays.asList(BeanTemplateUtils.build(HarvestControlMetadataBean.class).done().get()))
					.done();
			
			final ManagementFuture<Supplier<Object>> result = _bucket_crud.storeObject(bucket);
			assertTrue("Got errors", !result.getManagementResults().get().isEmpty() && !result.getManagementResults().get().iterator().next().success());
			
			System.out.println("Mgmt side channels = " + result.getManagementResults().get().stream().map(m->m.message()).collect(Collectors.joining(" ; ")));
			result.get();
			fail("Should have thrown an exception");
		}
		catch (Exception e) {
			System.out.println("expected, err=" + e.getCause().getMessage());
			assertEquals(RuntimeException.class, e.getCause().getClass());
		}				
		assertEquals(1L, (long)_bucket_crud.countObjects().get());
	}

	/** (More accurate test of validation using a simpler static method called by the non static one)
	 * @throws Exception
	 */
	@Test
	public void test_staticValidateInsert() throws Exception {
		cleanDatabases();

		// 0) Start with a valid bucket:
		
		final DataBucketBean valid_bucket = 
				BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::_id, "id1")
				.with(DataBucketBean::full_name, "/name1/embedded/dir")
				.with(DataBucketBean::display_name, "name1")
				.with(DataBucketBean::created, new Date())
				.with(DataBucketBean::modified, new Date())
				.with(DataBucketBean::owner_id, "owner1")
				.done().get();

		// 1) Check needs status object to be present
		
		{
			final List<BasicMessageBean> errs = BucketValidationUtils.staticValidation(valid_bucket, false);
			System.out.println("validation errs = " + errs.stream().map(m->m.message()).collect(Collectors.joining(";")));
			assertEquals(0, errs.size());
		}
		
		//////////////////////////
		
		// Validation _errors_
		
		// MOSTLY DUPS OF THE NON STATIC VERSION
		
		final DataBucketBean new_valid_bucket =  BeanTemplateUtils.clone(valid_bucket).with(DataBucketBean::_id, "id2").done();
		
		// 2) Missing field
		
		// (TESTED IN NON STATIC CODE - ABOVE)
		
		// (2- not allowed buckets that start with "aleph2_")
		{
			final DataBucketBean bucket = BeanTemplateUtils.clone(new_valid_bucket)
					.with(DataBucketBean::full_name, "/aleph2_something/blah")
					.done();
			
			final List<BasicMessageBean> errs = BucketValidationUtils.staticValidation(bucket, false);
			System.out.println("validation errs = " + errs.stream().map(m->m.message()).collect(Collectors.joining(";")));
			assertEquals(1, errs.size());			
		}
		//(check is allowed if explicitly specified) 
		{
			final DataBucketBean bucket = BeanTemplateUtils.clone(new_valid_bucket)
					.with(DataBucketBean::full_name, "/aleph2_something/blah")
					.done();
			
			final List<BasicMessageBean> errs = BucketValidationUtils.staticValidation(bucket, true);
			System.out.println("validation errs = " + errs.stream().map(m->m.message()).collect(Collectors.joining(";")));
			assertEquals(0, errs.size());			
		}
		
		
		// 3) Zero length fields
		
		// (TESTED IN NON STATIC CODE - ABOVE)
		
		// 4-) Not currently supported combined master batch+streaming enrichment (need to tidy up logic and I suppose decide if there are really any use cases... can always use 2xjob analytic thread versions if really really needed?)

		{
			final DataBucketBean bucket = BeanTemplateUtils.clone(new_valid_bucket)
					.with(DataBucketBean::full_name, "/validation/test4_")
					.with(DataBucketBean::master_enrichment_type, DataBucketBean.MasterEnrichmentType.streaming_and_batch)
					.with(DataBucketBean::streaming_enrichment_topology, 
							BeanTemplateUtils.build(EnrichmentControlMetadataBean.class).done().get())				
					.with(DataBucketBean::batch_enrichment_topology, 
							BeanTemplateUtils.build(EnrichmentControlMetadataBean.class).done().get())
					.done();
			
			final List<BasicMessageBean> errs = BucketValidationUtils.staticValidation(bucket, false);
			System.out.println("validation errs = " + errs.stream().map(m->m.message()).collect(Collectors.joining(";")));
			assertEquals(1, errs.size());			
		}
		
		// 4) Enrichment but no harvest (THIS IS NOW FINE)
		
		{
			final DataBucketBean bucket = BeanTemplateUtils.clone(new_valid_bucket)
					.with(DataBucketBean::full_name, "/validation/test4")
					.with(DataBucketBean::master_enrichment_type, DataBucketBean.MasterEnrichmentType.batch)
					.with(DataBucketBean::batch_enrichment_topology, 
							BeanTemplateUtils.build(EnrichmentControlMetadataBean.class).done().get()).done();
			
			final List<BasicMessageBean> errs = BucketValidationUtils.staticValidation(bucket, false);
			System.out.println("validation errs = " + errs.stream().map(m->m.message()).collect(Collectors.joining(";")));
			assertEquals(0, errs.size());
		}
		
		// 5) Enrichment but no type
		
		{
			final DataBucketBean bucket = BeanTemplateUtils.clone(new_valid_bucket)
					.with(DataBucketBean::full_name, "/validation/test5")
					.with(DataBucketBean::harvest_technology_name_or_id, "harvest_tech")
					.with(DataBucketBean::harvest_configs, Arrays.asList(BeanTemplateUtils.build(HarvestControlMetadataBean.class).with(HarvestControlMetadataBean::enabled, true).done().get()))
					.with(DataBucketBean::batch_enrichment_topology, 
							BeanTemplateUtils.build(EnrichmentControlMetadataBean.class).done().get()).done();
			
			final List<BasicMessageBean> errs = BucketValidationUtils.staticValidation(bucket, false);
			System.out.println("validation errs = " + errs.stream().map(m->m.message()).collect(Collectors.joining(";")));
			assertEquals(1, errs.size());
		}
		
		// 6) Wrong type of enrichment - streaming missing
		
		{
			final DataBucketBean bucket = BeanTemplateUtils.clone(new_valid_bucket)
					.with(DataBucketBean::harvest_technology_name_or_id, "harvest_tech")
					.with(DataBucketBean::full_name, "/validation/test6")
					.with(DataBucketBean::harvest_configs, Arrays.asList(BeanTemplateUtils.build(HarvestControlMetadataBean.class).with(HarvestControlMetadataBean::enabled, true).done().get()))
					.with(DataBucketBean::master_enrichment_type, DataBucketBean.MasterEnrichmentType.streaming)
					.with(DataBucketBean::batch_enrichment_topology, 
							BeanTemplateUtils.build(EnrichmentControlMetadataBean.class).done().get()).done();
			
			final List<BasicMessageBean> errs = BucketValidationUtils.staticValidation(bucket, false);
			System.out.println("validation errs = " + errs.stream().map(m->m.message()).collect(Collectors.joining(";")));
			assertEquals(1, errs.size());
		}
		
		// 7) Wrong type of enrichment - batch missing
		
		{
			final DataBucketBean bucket = BeanTemplateUtils.clone(new_valid_bucket)
					.with(DataBucketBean::full_name, "/validation/test7")
					.with(DataBucketBean::harvest_technology_name_or_id, "harvest_tech")
					.with(DataBucketBean::harvest_configs, Arrays.asList(BeanTemplateUtils.build(HarvestControlMetadataBean.class).with(HarvestControlMetadataBean::enabled, true).done().get()))
					.with(DataBucketBean::master_enrichment_type, DataBucketBean.MasterEnrichmentType.batch)
					.with(DataBucketBean::streaming_enrichment_topology, 
							BeanTemplateUtils.build(EnrichmentControlMetadataBean.class).done().get()).done();
			
			final List<BasicMessageBean> errs = BucketValidationUtils.staticValidation(bucket, false);
			System.out.println("validation errs = " + errs.stream().map(m->m.message()).collect(Collectors.joining(";")));
			assertEquals(1, errs.size());
		}
		
		// 8) No harvest configs
		
		{
			final DataBucketBean bucket = BeanTemplateUtils.clone(new_valid_bucket)
					.with(DataBucketBean::full_name, "/validation/test8")
					.with(DataBucketBean::harvest_technology_name_or_id, "harvest_tech")
					.done();
			
			final List<BasicMessageBean> errs = BucketValidationUtils.staticValidation(bucket, false);
			System.out.println("validation errs = " + errs.stream().map(m->m.message()).collect(Collectors.joining(";")));
			assertEquals(1, errs.size());
		}
				
		// 9) Harvest config with empty lists
		
		{
			final DataBucketBean bucket = BeanTemplateUtils.clone(new_valid_bucket)
					.with(DataBucketBean::full_name, "/validation/test9")
					.with(DataBucketBean::harvest_technology_name_or_id, "harvest_tech")
					.with(DataBucketBean::harvest_configs, Arrays.asList(
							BeanTemplateUtils.build(HarvestControlMetadataBean.class)
								.with(HarvestControlMetadataBean::enabled, true)
								.with(HarvestControlMetadataBean::library_names_or_ids, Arrays.asList("xxx", ""))
								.done().get()))
					.done();
			
			final List<BasicMessageBean> errs = BucketValidationUtils.staticValidation(bucket, false);
			System.out.println("validation errs = " + errs.stream().map(m->m.message()).collect(Collectors.joining(";")));
			assertEquals(1, errs.size());
		}

		// 10) Missing batch config enabled:
		
		{
			final DataBucketBean bucket = BeanTemplateUtils.clone(new_valid_bucket)
					.with(DataBucketBean::full_name, "/validation/test10")
					.with(DataBucketBean::harvest_technology_name_or_id, "harvest_tech")
					.with(DataBucketBean::harvest_configs, Arrays.asList(BeanTemplateUtils.build(HarvestControlMetadataBean.class).with(HarvestControlMetadataBean::enabled, true).done().get()))
					.with(DataBucketBean::master_enrichment_type, DataBucketBean.MasterEnrichmentType.batch)
					.with(DataBucketBean::batch_enrichment_configs, 
							Arrays.asList(BeanTemplateUtils.build(EnrichmentControlMetadataBean.class).done().get()))
							.done();
			
			final List<BasicMessageBean> errs = BucketValidationUtils.staticValidation(bucket, false);
			System.out.println("validation errs = " + errs.stream().map(m->m.message()).collect(Collectors.joining(";")));
			assertEquals(1, errs.size());
		}
		
		
		// 11) Missing streaming config enabled:
		
		{
			final DataBucketBean bucket = BeanTemplateUtils.clone(new_valid_bucket)
					.with(DataBucketBean::full_name, "/validation/test11")
					.with(DataBucketBean::harvest_technology_name_or_id, "harvest_tech")
					.with(DataBucketBean::harvest_configs, Arrays.asList(BeanTemplateUtils.build(HarvestControlMetadataBean.class).with(HarvestControlMetadataBean::enabled, true).done().get()))
					.with(DataBucketBean::master_enrichment_type, DataBucketBean.MasterEnrichmentType.streaming)
					.with(DataBucketBean::streaming_enrichment_configs, 
							Arrays.asList(BeanTemplateUtils.build(EnrichmentControlMetadataBean.class).done().get()))
							.done();
			final ManagementFuture<Supplier<Object>> result = _bucket_crud.storeObject(bucket);
			assertTrue("Got errors", !result.getManagementResults().get().isEmpty() && !result.getManagementResults().get().iterator().next().success());
			
			
			final List<BasicMessageBean> errs = BucketValidationUtils.staticValidation(bucket, false);
			System.out.println("validation errs = " + errs.stream().map(m->m.message()).collect(Collectors.joining(";")));
			assertEquals(1, errs.size());
		}
				
		// 12) Multi bucket!
		
		{
			final DataBucketBean bucket = BeanTemplateUtils.clone(new_valid_bucket)
					.with(DataBucketBean::full_name, "/validation/test12")
					.with(DataBucketBean::multi_bucket_children, new HashSet<String>(Arrays.asList("a", "b")))
					.with(DataBucketBean::harvest_technology_name_or_id, "harvest_tech")
					.with(DataBucketBean::harvest_configs, Arrays.asList(BeanTemplateUtils.build(HarvestControlMetadataBean.class).done().get()))
					.done();
			
			
			final List<BasicMessageBean> errs = BucketValidationUtils.staticValidation(bucket, false);
			System.out.println("validation errs = " + errs.stream().map(m->m.message()).collect(Collectors.joining(";")));
			assertEquals(1, errs.size());
		}
	}

	///////////////////////////////////////////////////////////////////////////////////////////

	@Test 
	public void handleDeleteFileStillPresent() throws InterruptedException, ExecutionException {
		cleanDatabases();
				
		// 0) Start with a valid bucket:
		
		final DataBucketBean valid_bucket = 
				BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::_id, "id1")
				.with(DataBucketBean::full_name, "/name1/embedded/dir")
				.with(DataBucketBean::display_name, "name1")
				.with(DataBucketBean::created, new Date())
				.with(DataBucketBean::modified, new Date())
				.with(DataBucketBean::owner_id, "owner1")
				.with(DataBucketBean::multi_node_enabled, true) 
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
			new File(System.getProperty("java.io.tmpdir") + File.separator + "data" + File.separator + valid_bucket.full_name()).mkdirs();
		}		
		catch (Exception e) {} // (fine, dir prob dones't delete)
		
		try {
			new File(System.getProperty("java.io.tmpdir") + File.separator + "data" + File.separator + valid_bucket.full_name() + File.separator + "managed_bucket" + File.separator + ".DELETED").createNewFile();
		}		
		catch (Exception e) {} // (fine, dir prob dones't delete)

		assertTrue("file exists", new File(System.getProperty("java.io.tmpdir") + File.separator + "data" + File.separator + valid_bucket.full_name() + File.separator + "managed_bucket" + File.separator + ".DELETED").exists());
		
		// OK now try inserting the bucket, should error:
		
		try {
			final ManagementFuture<Supplier<Object>> result = _bucket_crud.storeObject(valid_bucket);
			result.get();
			fail("Should have thrown an exception: " + result.getManagementResults().get().stream().map(m -> m.message()).collect(Collectors.joining(",")));
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
	public void test_AnotherSemiFailedBucketCreation_multiNode() throws Exception {
		cleanDatabases();

		// Setup: register a refuse-then-accept
		final String refusing_host1 = insertActor(TestActor_Refuser.class);
		final String refusing_host2 = insertActor(TestActor_Refuser.class);
		assertFalse("created actors on different hosts", refusing_host1.equals(refusing_host2));
		
		// 0) Start with a valid bucket:
		
		final DataBucketBean valid_bucket = 
				BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::_id, "id1")
				.with(DataBucketBean::full_name, "/name1/embedded/dir")
				.with(DataBucketBean::display_name, "name1")
				.with(DataBucketBean::created, new Date())
				.with(DataBucketBean::modified, new Date())
				.with(DataBucketBean::owner_id, "owner1")
				.with(DataBucketBean::multi_node_enabled, false) 
				.with(DataBucketBean::harvest_technology_name_or_id, "harvest_tech")
				.with(DataBucketBean::harvest_configs, 
						Arrays.asList(BeanTemplateUtils.build(HarvestControlMetadataBean.class)
								.with(HarvestControlMetadataBean::enabled, true)
								.done().get()))
				.done().get();

		//(delete the file path)
		try {
			FileUtils.deleteDirectory(new File(System.getProperty("java.io.tmpdir") + File.separator + "data" + File.separator + valid_bucket.full_name()));
		}
		catch (Exception e) {} // (fine, dir prob dones't delete)
		assertFalse("The file path has been deleted", new File(System.getProperty("java.io.tmpdir") + File.separator + "data" + File.separator + valid_bucket.full_name() + "/managed_bucket").exists());
		
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
		assertTrue("The file path has been built", new File(System.getProperty("java.io.tmpdir") + File.separator + "data" + File.separator + valid_bucket.full_name() + "/managed_bucket").exists());
		assertEquals(1L, (long)_bucket_crud.countObjects().get());
	}
	
	@Test
	public void test_AnotherSemiFailedBucketCreation_singleNode() throws Exception {
		cleanDatabases();

		// Setup: register a refuse-then-accept
		final String refusing_host1 = insertActor(TestActor_Accepter_Refuser.class);
		final String refusing_host2 = insertActor(TestActor_Accepter_Refuser.class);
		assertFalse("created actors on different hosts", refusing_host1.equals(refusing_host2));
		
		// 0) Start with a valid bucket:
		
		final DataBucketBean valid_bucket = 
				BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::_id, "id1")
				.with(DataBucketBean::full_name, "/name1/embedded/dir")
				.with(DataBucketBean::display_name, "name1")
				.with(DataBucketBean::created, new Date())
				.with(DataBucketBean::modified, new Date())
				.with(DataBucketBean::owner_id, "owner1")
				.with(DataBucketBean::multi_node_enabled, true) 
				.with(DataBucketBean::harvest_technology_name_or_id, "harvest_tech")
				.with(DataBucketBean::harvest_configs, 
						Arrays.asList(BeanTemplateUtils.build(HarvestControlMetadataBean.class)
								.with(HarvestControlMetadataBean::enabled, true)
								.done().get()))
				.done().get();

		//(delete the file path)
		try {
			FileUtils.deleteDirectory(new File(System.getProperty("java.io.tmpdir") + File.separator + "data" + File.separator + valid_bucket.full_name()));
		}
		catch (Exception e) {} // (fine, dir prob dones't delete)
		assertFalse("The file path has been deleted", new File(System.getProperty("java.io.tmpdir") + File.separator + "data" + File.separator + valid_bucket.full_name() + "/managed_bucket").exists());
		
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
		assertTrue("The file path has been built", new File(System.getProperty("java.io.tmpdir") + "/data/" + valid_bucket.full_name() + "/managed_bucket").exists());
		assertEquals(1L, (long)_bucket_crud.countObjects().get());
	}
	
	///////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////
	
	@Test
	public void test_SuccessfulBucketCreation_singleNode() throws Exception {
		cleanDatabases();

		// Setup: register an accepting actor to listen:
		final String accepting_host1 = insertActor(TestActor_Accepter.class);
		final String accepting_host2 = insertActor(TestActor_Accepter.class);
		assertFalse("created actors on different hosts", accepting_host1.equals(accepting_host2));
		
		// 0) Start with a valid bucket:
		
		final DataBucketBean valid_bucket = 
				BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::_id, "id1")
				.with(DataBucketBean::full_name, "/name1/embedded/dir")
				.with(DataBucketBean::display_name, "name1")
				.with(DataBucketBean::created, new Date())
				.with(DataBucketBean::modified, new Date())
				.with(DataBucketBean::owner_id, "owner1")
				.with(DataBucketBean::multi_node_enabled, false) 
				.with(DataBucketBean::master_enrichment_type, MasterEnrichmentType.none)
				.with(DataBucketBean::harvest_technology_name_or_id, "harvest_tech")
				.with(DataBucketBean::harvest_configs, 
						Arrays.asList(BeanTemplateUtils.build(HarvestControlMetadataBean.class)
								.with(HarvestControlMetadataBean::enabled, true)
								.done().get()))
				.done().get();

		//(delete the file path)
		try {
			FileUtils.deleteDirectory(new File(System.getProperty("java.io.tmpdir") + File.separator + "data" + File.separator + valid_bucket.full_name()));
		}
		catch (Exception e) {} // (fine, dir prob dones't delete)
		assertFalse("The file path has been deleted", new File(System.getProperty("java.io.tmpdir") + File.separator + "data" + File.separator + valid_bucket.full_name() + "/managed_bucket").exists());
		
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
		assertTrue("The file path has been built", new File(System.getProperty("java.io.tmpdir") + File.separator + "data" + File.separator + valid_bucket.full_name() + "/managed_bucket").exists());
		assertEquals(1L, (long)_bucket_crud.countObjects().get());
		
		// Check the "Confirmed" bucket fields match the bucket now
		assertEquals(false, status_after.confirmed_suspended());
		assertEquals(false, status_after.confirmed_multi_node_enabled());
		assertEquals(MasterEnrichmentType.none, status_after.confirmed_master_enrichment_type());
		
		// Since it worked, let's quickly try adding again with same full name but different id and check it fails...
		
		final DataBucketBean dup = BeanTemplateUtils.clone(valid_bucket).with(DataBucketBean::_id, "different_id").done();
		//(add the status object and try)
		final DataBucketStatusBean status2 = 
				BeanTemplateUtils.build(DataBucketStatusBean.class)
				.with(DataBucketStatusBean::_id, dup._id())
				.with(DataBucketStatusBean::bucket_path, dup.full_name())
				.with(DataBucketStatusBean::suspended, false)
				.done().get();
		
		assertEquals(1L, (long)_bucket_status_crud.countObjects().get());
		_bucket_status_crud.storeObject(status2).get();
		assertEquals(2L, (long)_bucket_status_crud.countObjects().get());
		assertEquals(1L, (long)_bucket_crud.countObjects().get());
		final ManagementFuture<Supplier<Object>> insert_future2 = _bucket_crud.storeObject(valid_bucket);
		try {
			insert_future2.get();
			fail("Should have thrown an exception");
		}
		catch (Exception e) {
			assertEquals(1, insert_future2.getManagementResults().get().size());
			System.out.println("Dup error = " + insert_future2.getManagementResults().get().iterator().next().message());
		}
		assertEquals(1L, (long)_bucket_crud.countObjects().get());
}
	
	@Test
	public void test_SuccessfulBucketCreation_singleNode_PollFrequency() throws Exception {
		cleanDatabases();

		// Setup: register an accepting actor to listen:
		final String accepting_host1 = insertActor(TestActor_Accepter.class);
		final String accepting_host2 = insertActor(TestActor_Accepter.class);
		assertFalse("created actors on different hosts", accepting_host1.equals(accepting_host2));
		
		// 0) Start with a valid bucket:
		
		final DataBucketBean valid_bucket = 
				BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::_id, "id1")
				.with(DataBucketBean::full_name, "/name1/embedded/dir")
				.with(DataBucketBean::display_name, "name1")
				.with(DataBucketBean::created, new Date())
				.with(DataBucketBean::modified, new Date())
				.with(DataBucketBean::owner_id, "owner1")
				.with(DataBucketBean::multi_node_enabled, false) 
				.with(DataBucketBean::master_enrichment_type, MasterEnrichmentType.none)
				.with(DataBucketBean::harvest_technology_name_or_id, "harvest_tech")
				.with(DataBucketBean::poll_frequency, "in 5 minutes")
				.with(DataBucketBean::harvest_configs, 
						Arrays.asList(BeanTemplateUtils.build(HarvestControlMetadataBean.class)
								.with(HarvestControlMetadataBean::enabled, true)
								.done().get()))
				.done().get();

		//(delete the file path)
		try {
			FileUtils.deleteDirectory(new File(System.getProperty("java.io.tmpdir") + File.separator + "data" + File.separator + valid_bucket.full_name()));
		}
		catch (Exception e) {} // (fine, dir prob dones't delete)
		assertFalse("The file path has been deleted", new File(System.getProperty("java.io.tmpdir") + File.separator + "data" + File.separator + valid_bucket.full_name() + "/managed_bucket").exists());
		
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
		assertTrue("The file path has been built", new File(System.getProperty("java.io.tmpdir") + File.separator + "data" + File.separator + valid_bucket.full_name() + "/managed_bucket").exists());
		assertEquals(1L, (long)_bucket_crud.countObjects().get());
		
		// Check the "Confirmed" bucket fields match the bucket now
		assertEquals(false, status_after.confirmed_suspended());
		assertEquals(false, status_after.confirmed_multi_node_enabled());
		assertEquals(MasterEnrichmentType.none, status_after.confirmed_master_enrichment_type());
		
		//finally check that next_poll_date was set
		final DataBucketStatusBean valid_bucket_status_retrieved = _bucket_status_crud.getObjectById(valid_bucket._id()).get().get();
//		final DataBucketBean valid_bucket_retrieved = _bucket_crud.getObjectById(valid_bucket._id()).get().get();
//		assertTrue("Next poll date should not be set in the object we submitted", valid_bucket.next_poll_date() == null);
		assertTrue("Next poll date should have been set during store", valid_bucket_status_retrieved.next_poll_date() != null);
	}

	@Test
	public void test_SuccessfulBucketCreation_multiNode() throws Exception {
		cleanDatabases();

		// Setup: register an accepting actor to listen:
		final String accepting_host1 = insertActor(TestActor_Accepter.class);
		final String accepting_host2 = insertActor(TestActor_Accepter.class);
		assertFalse("created actors on different hosts", accepting_host1.equals(accepting_host2));
		
		// 0) Start with a valid bucket:
		
		final DataBucketBean valid_bucket = 
				BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::_id, "id1")
				.with(DataBucketBean::full_name, "/name1/embedded/dir")
				.with(DataBucketBean::display_name, "name1")
				.with(DataBucketBean::created, new Date())
				.with(DataBucketBean::modified, new Date())
				.with(DataBucketBean::owner_id, "owner1")
				.with(DataBucketBean::multi_node_enabled, true) 
				.with(DataBucketBean::master_enrichment_type, MasterEnrichmentType.none)
				.with(DataBucketBean::harvest_technology_name_or_id, "harvest_tech")
				.with(DataBucketBean::harvest_configs, 
						Arrays.asList(BeanTemplateUtils.build(HarvestControlMetadataBean.class)
								.with(HarvestControlMetadataBean::enabled, true)
								.done().get()))
				.done().get();

		//(delete the file path)
		try {
			FileUtils.deleteDirectory(new File(System.getProperty("java.io.tmpdir") + File.separator + "data" + File.separator + valid_bucket.full_name()));
		}
		catch (Exception e) {} // (fine, dir prob dones't delete)
		assertFalse("The file path has been deleted", new File(System.getProperty("java.io.tmpdir") + File.separator + "data" + File.separator + valid_bucket.full_name() + "/managed_bucket").exists());
		
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
		assertEquals("Store should succeed: " + err_msg.message(), true, err_msg.success());
		assertEquals(valid_bucket._id(), insert_future.get().get());
		final DataBucketStatusBean status_after = _bucket_status_crud.getObjectById(valid_bucket._id()).get().get();
		assertEquals(2, status_after.node_affinity().size());
		assertTrue("Check the node affinity is correct: ", status_after.node_affinity().contains(accepting_host1) && status_after.node_affinity().contains(accepting_host2));
		assertEquals(false, status_after.suspended());
		assertTrue("The file path has been built", new File(System.getProperty("java.io.tmpdir") + "/data/" +  valid_bucket.full_name() + "/managed_bucket").exists());
		assertEquals(1L, (long)_bucket_crud.countObjects().get());
		
		// Check the "Confirmed" bucket fields match the bucket now
		assertEquals(false, status_after.confirmed_suspended());
		assertEquals(true, status_after.confirmed_multi_node_enabled());
		assertEquals(MasterEnrichmentType.none, status_after.confirmed_master_enrichment_type());		
	}

	@Test
	public void test_SuccessfulBucketCreation_multiNode_streaming() throws Exception {
		cleanDatabases();

		// Setup: register an accepting actor to listen:
		final String streaming_host = insertAnalyticsActor(TestActor_Accepter.class);
		final String accepting_host1 = insertActor(TestActor_Accepter.class);
		final String accepting_host2 = insertActor(TestActor_Accepter.class);
		assertFalse("created actors on different hosts", accepting_host1.equals(accepting_host2));
		
		// 0) Start with a valid bucket:
		
		final DataBucketBean valid_bucket = 
				BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::_id, "id1")
				.with(DataBucketBean::full_name, "/name1/embedded/dir")
				.with(DataBucketBean::display_name, "name1")
				.with(DataBucketBean::created, new Date())
				.with(DataBucketBean::modified, new Date())
				.with(DataBucketBean::owner_id, "owner1")
				.with(DataBucketBean::multi_node_enabled, true) 
				.with(DataBucketBean::master_enrichment_type, MasterEnrichmentType.streaming)
				.with(DataBucketBean::streaming_enrichment_topology, BeanTemplateUtils.build(EnrichmentControlMetadataBean.class).done().get())
				.with(DataBucketBean::harvest_technology_name_or_id, "harvest_tech")
				.with(DataBucketBean::harvest_configs, 
						Arrays.asList(BeanTemplateUtils.build(HarvestControlMetadataBean.class)
								.with(HarvestControlMetadataBean::enabled, true)
								.done().get()))
				.done().get();

		//(delete the file path)
		try {
			FileUtils.deleteDirectory(new File(System.getProperty("java.io.tmpdir") + File.separator + "data" + File.separator + valid_bucket.full_name()));
		}
		catch (Exception e) {} // (fine, dir prob dones't delete)
		assertFalse("The file path has been deleted", new File(System.getProperty("java.io.tmpdir") + File.separator + "data" + File.separator + valid_bucket.full_name() + "/managed_bucket").exists());
		
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

		// Try again, assert - works this time
		assertEquals(0L, (long)_bucket_crud.countObjects().get());
		final ManagementFuture<Supplier<Object>> insert_future = _bucket_crud.storeObject(valid_bucket);
		assertEquals("Wrong number of replies: " +
					insert_future.getManagementResults().join().stream().map(b->b.message()).collect(Collectors.joining(";")), 
						3, insert_future.getManagementResults().get().size());
		final java.util.Iterator<BasicMessageBean> it = insert_future.getManagementResults().get().iterator();
		final BasicMessageBean streaming_msg = it.next();
		assertEquals(true, streaming_msg.success());
		assertEquals(streaming_msg.source(), streaming_host);
		assertEquals(streaming_msg.command(), ActorUtils.BUCKET_ANALYTICS_ZOOKEEPER);
		final BasicMessageBean err_msg1 = it.next();
		assertEquals(true, err_msg1.success());
		final BasicMessageBean err_msg2 = it.next();
		assertEquals(true, err_msg2.success());
		assertEquals(valid_bucket._id(), insert_future.get().get());
		final DataBucketStatusBean status_after = _bucket_status_crud.getObjectById(valid_bucket._id()).get().get();
		assertEquals(2, status_after.node_affinity().size());
		assertTrue("Check the node affinity is correct: ", status_after.node_affinity().contains(accepting_host1) && status_after.node_affinity().contains(accepting_host2));
		assertTrue("Check the node affinity is correct: ", !status_after.node_affinity().contains(streaming_host));
		assertEquals(false, status_after.suspended());
		assertTrue("The file path has been built", new File(System.getProperty("java.io.tmpdir") + File.separator + "data" + File.separator + valid_bucket.full_name() + "/managed_bucket").exists());
		assertEquals(1L, (long)_bucket_crud.countObjects().get());
	}

	///////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////
	
	@Test
	public void test_UpdateValidation() throws Exception {
		
		// Insert a bucket:
		test_SuccessfulBucketCreation_multiNode();		

		// (add analytics bucket to handle batch enrichment changes)
		insertAnalyticsActor(TestActor_Accepter.class);
		
		// Try to update it
		
		final DataBucketBean bucket = _bucket_crud.getObjectById("id1").get().get();
		
		//(while we're here, just check that the data schema map is written in)
		assertEquals(Collections.emptyMap(), bucket.data_locations());
		
		final DataBucketBean mod_bucket1 = BeanTemplateUtils.clone(bucket)
											.with(DataBucketBean::full_name, "/Something/else")
											.with(DataBucketBean::owner_id, "Someone else")
											.done();
		
		// First attempt: will fail because not trying to overwrite:
		{
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
		}
		
		// Third attempt, succeed with different update
		final DataBucketBean mod_bucket3 = BeanTemplateUtils.clone(bucket)
				.with(DataBucketBean::master_enrichment_type, MasterEnrichmentType.batch)
				.with(DataBucketBean::batch_enrichment_configs, Arrays.asList())
				.with(DataBucketBean::display_name, "Something else")
				.done();

		// Actually first time, fail because changing enrichment type and bucket active
		{
			final ManagementFuture<Supplier<Object>> update_future2 = _bucket_crud.storeObject(mod_bucket3, true);
			
			try {
				assertEquals("Should get 1 error: " + update_future2.getManagementResults().get().stream().map(m -> m.message()).collect(Collectors.joining()), 
						1, update_future2.getManagementResults().get().size()); // (1 error)
				update_future2.get();
				fail("Should have thrown exception");
			}
			catch (Exception e) {
				assertTrue("Validation error: " + e + " / " + e.getCause(), e.getCause() instanceof RuntimeException);
				assertTrue("compains about master_enrichment_type: " + e.getMessage() + ": " + 
						update_future2.getManagementResults().get().stream().map(b->b.message()).collect(Collectors.joining(";"))
						, 
						update_future2.getManagementResults().get().iterator().next().message().contains("master_enrichment_type"));
			}			
		}
		
		// THen suspend and succeed
		{
			assertEquals(true, _underlying_bucket_status_crud.updateObjectById("id1", 
					CrudUtils.update(DataBucketStatusBean.class).set("suspended", true)
																.set("confirmed_suspended", true)
					).get());
			
			final ManagementFuture<Supplier<Object>> update_future3 = _bucket_crud.storeObject(mod_bucket3, true);
			
			try {
				assertEquals("id1", update_future3.get().get());
		
				final DataBucketBean bucket3 = _bucket_crud.getObjectById("id1").get().get();
				assertEquals("Something else", bucket3.display_name());
				
				//(wait for completion)
				update_future3.getManagementResults().get();
				
				//(just quickly check node affinity didn't change)
				final DataBucketStatusBean status_after = _bucket_status_crud.getObjectById("id1").get().get();
				assertEquals(2, status_after.node_affinity().size());
				
				// Check the "Confirmed" bucket fields match the bucket now (only confirmed_suspended is set)
				assertEquals(true, status_after.confirmed_suspended());
				assertEquals(true, status_after.confirmed_multi_node_enabled());
				assertEquals(MasterEnrichmentType.batch, status_after.confirmed_master_enrichment_type());		
			}
			catch (Exception e) {
				update_future3.getManagementResults().get().stream().map(msg -> msg.command()).collect(Collectors.joining());
				throw e; // erorr out
			}
		}		
		// Check that will set the affinity if it's null though:
		{
			// (manually remove)
			assertTrue("Updated", _underlying_bucket_status_crud.updateObjectById("id1", CrudUtils.update(DataBucketStatusBean.class).set("node_affinity", Arrays.asList())).get());		
			final DataBucketStatusBean status_after2 = _bucket_status_crud.getObjectById("id1").get().get();
			assertEquals("Really updated!", 0, status_after2.node_affinity().size());
			
			final ManagementFuture<Supplier<Object>> update_future4 = _bucket_crud.storeObject(mod_bucket3, true);
			
			assertEquals("id1", update_future4.get().get());
	
			final DataBucketBean bucket4 = _bucket_crud.getObjectById("id1").get().get();
			assertEquals("Something else", bucket4.display_name());
			
			//(wait for completion)
			update_future4.getManagementResults().get();			
			
			//(Check that node affinity was set)
			update_future4.getManagementResults().get(); // (wait for management results - until then node affinity may not be set)
			final DataBucketStatusBean status_after3 = _bucket_status_crud.getObjectById("id1").get().get();
			assertEquals(2, status_after3.node_affinity().size());
			
			// Check the "Confirmed" bucket fields match the bucket now (only confirmed_suspended is set)
			assertEquals(true, status_after3.confirmed_suspended());
			assertEquals(true, status_after3.confirmed_multi_node_enabled());
			assertEquals(MasterEnrichmentType.batch, status_after3.confirmed_master_enrichment_type());					
		}		
		
		// OK check that if moving to single node then it resets the affinity
		{
			final DataBucketBean mod_bucket4 = BeanTemplateUtils.clone(bucket)
					.with(DataBucketBean::display_name, "Something else")
					.with(DataBucketBean::master_enrichment_type, MasterEnrichmentType.batch)
					.with(DataBucketBean::batch_enrichment_configs, Arrays.asList())
					.with(DataBucketBean::multi_node_enabled, false)
					.done();
			
			//ACTIVE: FAIL
			{
				assertEquals(true, _underlying_bucket_status_crud.updateObjectById("id1", 
						CrudUtils.update(DataBucketStatusBean.class).set("suspended", false)
																	.set("confirmed_suspended", false)
						).get());
				
				final ManagementFuture<Supplier<Object>> update_future2 = _bucket_crud.storeObject(mod_bucket4, true);
				
				try {
					assertEquals("Should get 1 error: " + update_future2.getManagementResults().get().stream().map(m -> m.message()).collect(Collectors.joining()), 
							1, update_future2.getManagementResults().get().size()); // (1 error)
					update_future2.get();
					fail("Should have thrown exception");
				}
				catch (Exception e) {
					assertTrue("Validation error", e.getCause() instanceof RuntimeException);
					assertTrue("compains about multi_node_enabled: " + e.getMessage(), 
							update_future2.getManagementResults().get().iterator().next().message().contains("multi_node_enabled"));
				}			
			}
			//SUSPENDED: SUCCESS
			{
				assertEquals(true, _underlying_bucket_status_crud.updateObjectById("id1", 
						CrudUtils.update(DataBucketStatusBean.class).set("suspended", true)
																	.set("confirmed_suspended", true)
						).get());
				
				final ManagementFuture<Supplier<Object>> update_future5 = _bucket_crud.storeObject(mod_bucket4, true);
				
				assertEquals("id1", update_future5.get().get());
		
				final DataBucketBean bucket5 = _bucket_crud.getObjectById("id1").get().get();
				assertEquals("Something else", bucket5.display_name());
				
				//(wait for completion)
				update_future5.getManagementResults().get();			
								
				//(Check that node affinity was set to 1)
				update_future5.getManagementResults().get(); // (wait for management results - until then node affinity may not be set)
				final DataBucketStatusBean status_after4 = _bucket_status_crud.getObjectById("id1").get().get();
				assertEquals(1, status_after4.node_affinity().size());
				
				// Check the "Confirmed" bucket fields match the bucket now (only confirmed_suspended is set)
				assertEquals(true, status_after4.confirmed_suspended());
				assertEquals(false, status_after4.confirmed_multi_node_enabled());
				assertEquals(MasterEnrichmentType.batch, status_after4.confirmed_master_enrichment_type());					
				
			}		
		}
		// And check that moves back to 2 when set back to multi node
		final DataBucketBean mod_bucket4 = BeanTemplateUtils.clone(bucket)
				.with(DataBucketBean::display_name, "Something else")
				.with(DataBucketBean::multi_node_enabled, true)
				.done();
		
		{
			final ManagementFuture<Supplier<Object>> update_future5 = _bucket_crud.storeObject(mod_bucket4, true);
			
			assertEquals("id1", update_future5.get().get());
	
			final DataBucketBean bucket5 = _bucket_crud.getObjectById("id1").get().get();
			assertEquals("Something else", bucket5.display_name());
			
			//(Check that node affinity was set to 1)
			update_future5.getManagementResults().get(); // (wait for management results - until then node affinity may not be set)
			final DataBucketStatusBean status_after4 = _bucket_status_crud.getObjectById("id1").get().get();
			assertEquals(2, status_after4.node_affinity().size());
		}		
		// Convert to lock_to_nodes: false and check that the node affinity is not updated
		{
			CompletableFuture<Boolean> updated = _underlying_bucket_status_crud.updateObjectById("id1", 
				CrudUtils.update(DataBucketStatusBean.class)
					.set(DataBucketStatusBean::suspended, false)
					.set(DataBucketStatusBean::confirmed_suspended, false)
				);
			assertTrue(updated.join());
			final DataBucketStatusBean status_after4a = _bucket_status_crud.getObjectById("id1").get().get();
			assertEquals(false, status_after4a.suspended());
			assertEquals(false, status_after4a.confirmed_suspended());
			
			final DataBucketBean mod_bucket5 = BeanTemplateUtils.clone(mod_bucket4)
					.with(DataBucketBean::lock_to_nodes, false)
					.done();
			
			final ManagementFuture<Supplier<Object>> update_future5 = _bucket_crud.storeObject(mod_bucket5, true);
			
			assertEquals("id1", update_future5.get().get());
	
			final DataBucketBean bucket5 = _bucket_crud.getObjectById("id1").get().get();
			assertEquals("Something else", bucket5.display_name());
			
			//(Check that node affinity was not set)
			update_future5.getManagementResults().get(); // (wait for management results - until then node affinity may not be set)
			final DataBucketStatusBean status_after4 = _bucket_status_crud.getObjectById("id1").get().get();
			assertEquals(2, status_after4.node_affinity().size());
		}
		// Now suspend the bucket and then rerun, check removes the node affinity
		// (note there is logic in the DIM that prevents you from doing this unless the harvest tech allows you to)
		{
			CompletableFuture<Boolean> updated = _underlying_bucket_status_crud.updateObjectById("id1", 
				CrudUtils.update(DataBucketStatusBean.class)
					.set(DataBucketStatusBean::suspended, true)
					.set(DataBucketStatusBean::confirmed_suspended, true)
					);
			assertTrue(updated.join());
			final DataBucketStatusBean status_after4a = _bucket_status_crud.getObjectById("id1").get().get();
			assertEquals(true, status_after4a.suspended());
			assertEquals(true, status_after4a.confirmed_suspended());
			
			final DataBucketBean mod_bucket5 = BeanTemplateUtils.clone(mod_bucket4)
					.with(DataBucketBean::lock_to_nodes, false)
					.done();
			
			final ManagementFuture<Supplier<Object>> update_future5 = _bucket_crud.storeObject(mod_bucket5, true);
			
			assertEquals("id1", update_future5.get().get());
	
			final DataBucketBean bucket5 = _bucket_crud.getObjectById("id1").get().get();
			assertEquals("Something else", bucket5.display_name());
			
			//(Check that node affinity was not set)
			update_future5.getManagementResults().get(); // (wait for management results - until then node affinity may not be set)
			final DataBucketStatusBean status_after4 = _bucket_status_crud.getObjectById("id1").get().get();
			assertEquals(null, status_after4.node_affinity());
		}
		// Check again (code coverage)
		{
			final DataBucketBean mod_bucket5 = BeanTemplateUtils.clone(mod_bucket4)
					.with(DataBucketBean::lock_to_nodes, false)
					.done();
			
			final ManagementFuture<Supplier<Object>> update_future5 = _bucket_crud.storeObject(mod_bucket5, true);
			
			assertEquals("id1", update_future5.get().get());
	
			final DataBucketBean bucket5 = _bucket_crud.getObjectById("id1").get().get();
			assertEquals("Something else", bucket5.display_name());
			
			//(Check that node affinity was not set)
			update_future5.getManagementResults().get(); // (wait for management results - until then node affinity may not be set)
			final DataBucketStatusBean status_after4 = _bucket_status_crud.getObjectById("id1").get().get();
			assertEquals(null, status_after4.node_affinity());			
		}
	}
}
