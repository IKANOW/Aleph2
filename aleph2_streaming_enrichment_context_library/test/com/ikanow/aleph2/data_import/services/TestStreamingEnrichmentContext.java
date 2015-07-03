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
package com.ikanow.aleph2.data_import.services;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Arrays;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import org.junit.Before;
import org.junit.Test;

import scala.Tuple2;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ContextUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class TestStreamingEnrichmentContext {

	protected Injector _app_injector;
	
	@Before
	public void injectModules() throws Exception {
		final Config config = ConfigFactory.parseFile(new File("./example_config_files/context_local_test.properties"));
		
		try {
			_app_injector = ModuleUtils.createInjector(Arrays.asList(), Optional.of(config));
		}
		catch (Exception e) {
			try {
				e.printStackTrace();
			}
			catch (Exception ee) {
				System.out.println(ErrorUtils.getLongForm("{0}", e));
			}
		}
	}
	
	@Test
	public void basicContextCreation() {
		try {
			assertTrue("Injector created", _app_injector != null);
		
			final StreamingEnrichmentContext test_context = _app_injector.getInstance(StreamingEnrichmentContext.class);
			
			assertTrue("StreamingEnrichmentContext created", test_context != null);
			
			assertTrue("StreamingEnrichmentContext dependencies", test_context._core_management_db != null);
			assertTrue("StreamingEnrichmentContext dependencies", test_context._distributed_services != null);
			assertTrue("StreamingEnrichmentContext dependencies", test_context._index_service != null);
			assertTrue("StreamingEnrichmentContext dependencies", test_context._globals != null);
			assertTrue("StreamingEnrichmentContext dependencies", test_context._service_context != null);
			
			// Check if started in "technology" (internal mode)
			assertEquals(test_context._state_name, StreamingEnrichmentContext.State.IN_TECHNOLOGY);
			
			// Check that multiple calls to create harvester result in different contexts but with the same injection:
			final StreamingEnrichmentContext test_context2 = _app_injector.getInstance(StreamingEnrichmentContext.class);
			assertTrue("StreamingEnrichmentContext created", test_context2 != null);
			assertTrue("StreamingEnrichmentContexts different", test_context2 != test_context);
			assertEquals(test_context._service_context, test_context2._service_context);
			
		}
		catch (Exception e) {
			System.out.println(ErrorUtils.getLongForm("{1}: {0}", e, e.getClass()));
			fail("Threw exception");
		}
	}
	
	@Test
	public void testExternalContextCreation() throws InstantiationException, IllegalAccessException, ClassNotFoundException, InterruptedException, ExecutionException {
		try {
			final StreamingEnrichmentContext test_context = _app_injector.getInstance(StreamingEnrichmentContext.class);
	
			final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class)
													.with(DataBucketBean::_id, "test")
													.with(DataBucketBean::modified, new Date())
													.with("data_schema", BeanTemplateUtils.build(DataSchemaBean.class)
															.with("search_index_schema", BeanTemplateUtils.build(DataSchemaBean.SearchIndexSchemaBean.class)
																	.done().get())
															.done().get())
													.done().get();
			
			// Empty service set:
			final String signature = test_context.getEnrichmentContextSignature(Optional.of(test_bucket), Optional.empty());
			
			final String expected_sig = "com.ikanow.aleph2.data_import.services.StreamingEnrichmentContext:{\"3fdb4bfa-2024-11e5-b5f7-727283247c7f\":\"test\",\"MongoDbManagementDbService\":{\"mongodb_connection\":\"localhost:9999\"},\"globals\":{\"local_cached_jar_dir\":\"file://temp/\"},\"service\":{\"CoreDistributedServices\":{\"interface\":\"com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices\",\"service\":\"com.ikanow.aleph2.distributed_services.services.MockCoreDistributedServices\"},\"CoreManagementDbService\":{\"interface\":\"com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService\",\"service\":\"com.ikanow.aleph2.management_db.services.CoreManagementDbService\"},\"ManagementDbService\":{\"interface\":\"com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService\",\"service\":\"com.ikanow.aleph2.management_db.mongodb.services.MockMongoDbManagementDbService\"},\"SearchIndexService\":{\"interface\":\"com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService\",\"service\":\"com.ikanow.aleph2.search_service.elasticsearch.services.MockElasticsearchIndexService\"},\"StorageService\":{\"interface\":\"com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService\",\"service\":\"com.ikanow.aleph2.storage_service_hdfs.services.MockHdfsStorageService\"}}}";			
			assertEquals(expected_sig, signature);

			// Check can't call multiple times
			
			// Additionals service set:

			try {
				test_context.getEnrichmentContextSignature(Optional.of(test_bucket),
												Optional.of(
														ImmutableSet.<Tuple2<Class<? extends IUnderlyingService>, Optional<String>>>builder()
															.add(Tuples._2T(IStorageService.class, Optional.empty()))
															.add(Tuples._2T(IManagementDbService.class, Optional.of("test")))
															.build()																
														)
												);
				fail("Should have errored");
			}
			catch (Exception e) {}
			// Create another injector:
			final StreamingEnrichmentContext test_context2 = _app_injector.getInstance(StreamingEnrichmentContext.class);
			final String signature2 = test_context2.getEnrichmentContextSignature(Optional.of(test_bucket),
					Optional.of(
							ImmutableSet.<Tuple2<Class<? extends IUnderlyingService>, Optional<String>>>builder()
								.add(Tuples._2T(IStorageService.class, Optional.empty()))
								.add(Tuples._2T(IManagementDbService.class, Optional.of("test")))
								.build()																
							)
					);
			
			
			final String expected_sig2 = "com.ikanow.aleph2.data_import.services.StreamingEnrichmentContext:{\"3fdb4bfa-2024-11e5-b5f7-727283247c7f\":\"test\",\"MongoDbManagementDbService\":{\"mongodb_connection\":\"localhost:9999\"},\"globals\":{\"local_cached_jar_dir\":\"file://temp/\"},\"service\":{\"CoreDistributedServices\":{\"interface\":\"com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices\",\"service\":\"com.ikanow.aleph2.distributed_services.services.MockCoreDistributedServices\"},\"CoreManagementDbService\":{\"interface\":\"com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService\",\"service\":\"com.ikanow.aleph2.management_db.services.CoreManagementDbService\"},\"ManagementDbService\":{\"interface\":\"com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService\",\"service\":\"com.ikanow.aleph2.management_db.mongodb.services.MockMongoDbManagementDbService\"},\"SearchIndexService\":{\"interface\":\"com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService\",\"service\":\"com.ikanow.aleph2.search_service.elasticsearch.services.MockElasticsearchIndexService\"},\"StorageService\":{\"interface\":\"com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService\",\"service\":\"com.ikanow.aleph2.storage_service_hdfs.services.MockHdfsStorageService\"},\"test\":{\"interface\":\"com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService\",\"service\":\"com.ikanow.aleph2.management_db.mongodb.services.MockMongoDbManagementDbService\"}}}"; 
			assertEquals(expected_sig2, signature2);
			
			// First fail because bucket not present
			
			try {
				@SuppressWarnings("unused")
				IEnrichmentModuleContext test_external1 = ContextUtils.getEnrichmentContext(signature);
				fail("Should have thrown exception");
			}
			catch (Exception e) {
				assertEquals("java.lang.RuntimeException: Unable to locate bucket: test", e.getMessage());
			}
			
			// Then insert bucket and hence succeed
	
			@SuppressWarnings("unchecked")
			final ICrudService<DataBucketBean> raw_mock_db =
					test_context._core_management_db.getDataBucketStore().getUnderlyingPlatformDriver(ICrudService.class, Optional.empty()).get();
			raw_mock_db.storeObject(test_bucket).get();
			
			assertEquals(1L, (long)raw_mock_db.countObjects().get());
			
			final IEnrichmentModuleContext test_external1a = ContextUtils.getEnrichmentContext(signature);		
			
			assertTrue("external context non null", test_external1a != null);
			
			assertTrue("external context of correct type", test_external1a instanceof StreamingEnrichmentContext);
			
			final StreamingEnrichmentContext test_external1b = (StreamingEnrichmentContext)test_external1a;
			
			assertTrue("StreamingEnrichmentContext dependencies", test_external1b._core_management_db != null);
			assertTrue("StreamingEnrichmentContext dependencies", test_external1b._distributed_services != null);
			assertTrue("StreamingEnrichmentContext dependencies", test_context._index_service != null);
			assertTrue("StreamingEnrichmentContext dependencies", test_external1b._globals != null);
			assertTrue("StreamingEnrichmentContext dependencies", test_external1b._service_context != null);
			
			assertEquals("test", test_external1b._mutable_state.bucket.get()._id());
			
			// Finally, check I can see my extended services: 
			
			final IEnrichmentModuleContext test_external2a = ContextUtils.getEnrichmentContext(signature2);		
			
			assertTrue("external context non null", test_external2a != null);
			
			assertTrue("external context of correct type", test_external2a instanceof StreamingEnrichmentContext);
			
			final StreamingEnrichmentContext test_external2b = (StreamingEnrichmentContext)test_external2a;
			
			assertTrue("StreamingEnrichmentContext dependencies", test_external2b._core_management_db != null);
			assertTrue("StreamingEnrichmentContext dependencies", test_external2b._distributed_services != null);
			assertTrue("StreamingEnrichmentContext dependencies", test_context._index_service != null);
			assertTrue("StreamingEnrichmentContext dependencies", test_external2b._globals != null);
			assertTrue("StreamingEnrichmentContext dependencies", test_external2b._service_context != null);
			
			assertEquals("test", test_external2b._mutable_state.bucket.get()._id());
			
			assertTrue("I can see my additonal services", null != test_external2b._service_context.getService(IStorageService.class, Optional.empty()));
			assertTrue("I can see my additonal services", null != test_external2b._service_context.getService(IManagementDbService.class, Optional.of("test")));
			
		}
		catch (Exception e) {
			System.out.println(ErrorUtils.getLongForm("{1}: {0}", e, e.getClass()));
			fail("Threw exception");
		}
	}
	
	//TODO
//	@Test
//	public void testFileLocations() throws InstantiationException, IllegalAccessException, ClassNotFoundException, InterruptedException, ExecutionException {
//		try {
//			final HarvestContext test_context = _app_injector.getInstance(HarvestContext.class);
//
//			final List<String> lib_paths = test_context.getHarvestContextLibraries(Optional.empty());
//
//			//(this doesn't work very well when run in test mode because it's all being found from file)
//			assertTrue("Finds some libraries", !lib_paths.isEmpty());
//			lib_paths.stream().forEach(lib -> assertTrue("No external libraries: " + lib, lib.contains("aleph2")));
//			
//			// Now get the various shared libs
//
//			final HarvestControlMetadataBean harvest_module1 = BeanTemplateUtils.build(HarvestControlMetadataBean.class)
//															.with(HarvestControlMetadataBean::library_ids_or_names, Arrays.asList("id1", "name2"))
//															.done().get();
//			
//			final HarvestControlMetadataBean harvest_module2 = BeanTemplateUtils.build(HarvestControlMetadataBean.class)
//					.with(HarvestControlMetadataBean::library_ids_or_names, Arrays.asList("id1", "name3", "test_harvest_tech_id"))
//															.done().get();
//												
//			
//			final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class)
//					.with(DataBucketBean::_id, "test")
//					.with(DataBucketBean::harvest_technology_name_or_id, "test_harvest_tech_id")
//					.with(DataBucketBean::harvest_configs, Arrays.asList(harvest_module1, harvest_module2))
//					.done().get();
//
//			final SharedLibraryBean htlib1 = BeanTemplateUtils.build(SharedLibraryBean.class)
//												.with(SharedLibraryBean::_id, "test_harvest_tech_id")
//												.with(SharedLibraryBean::path_name, "test_harvest_tech_name")
//												.done().get();
//			
//			final SharedLibraryBean htmod1 = BeanTemplateUtils.build(SharedLibraryBean.class)
//					.with(SharedLibraryBean::_id, "id1")
//					.with(SharedLibraryBean::path_name, "name1")
//					.done().get();
//			
//			final SharedLibraryBean htmod2 = BeanTemplateUtils.build(SharedLibraryBean.class)
//					.with(SharedLibraryBean::_id, "id2")
//					.with(SharedLibraryBean::path_name, "name2")
//					.done().get();
//			
//			final SharedLibraryBean htmod3 = BeanTemplateUtils.build(SharedLibraryBean.class)
//					.with(SharedLibraryBean::_id, "id3")
//					.with(SharedLibraryBean::path_name, "name3")
//					.done().get();
//			
//			test_context._service_context.getService(IManagementDbService.class, Optional.empty()).get()
//								.getSharedLibraryStore().storeObjects(Arrays.asList(htlib1, htmod1, htmod2, htmod3)).get();
//			
//			Map<String, String> mods = test_context.getHarvestLibraries(Optional.of(test_bucket)).get();
//			assertTrue("name1", mods.containsKey("name1") && mods.get("name1").endsWith("id1.cache.jar"));
//			assertTrue("name2", mods.containsKey("name2") && mods.get("name2").endsWith("id2.cache.jar"));
//			assertTrue("name3", mods.containsKey("name3") && mods.get("name3").endsWith("id3.cache.jar"));
//			assertTrue("test_harvest_tech_name", mods.containsKey("test_harvest_tech_name") && mods.get("test_harvest_tech_name").endsWith("test_harvest_tech_id.cache.jar"));
//		}
//		catch (Exception e) {
//			try {
//				e.printStackTrace();
//			}
//			catch (Exception ee) {
//				System.out.println(ErrorUtils.getLongForm("{1}: {0}", e, e.getClass()));
//			}
//			fail("Threw exception");
//		}
//		
//	}
//	
}
