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
package com.ikanow.aleph2.data_import.services;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import scala.Tuple2;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_import.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.HarvestControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean.MasterEnrichmentType;
import com.ikanow.aleph2.data_model.objects.shared.AssetStateDirectoryBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.BucketUtils;
import com.ikanow.aleph2.data_model.utils.ContextUtils;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import fj.data.Either;

public class TestHarvestContext {
	private static final Logger _logger = LogManager.getLogger();	

	protected Injector _app_injector;
	
	@Before
	public void injectModules() throws Exception {
		_logger.info("running injectModules");
		
		final Config config = ConfigFactory.parseFile(new File("./example_config_files/harvest_local_test.properties"));
		
		try {
			_app_injector = ModuleUtils.createTestInjector(Arrays.asList(), Optional.of(config));
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
	public void test_basicContextCreation() {
		_logger.info("running test_basicContextCreation");
		
		try {
			assertTrue("Injector created", _app_injector != null);
		
			final HarvestContext test_context = _app_injector.getInstance(HarvestContext.class);
			
			assertTrue("HarvestContext created", test_context != null);
			
			assertTrue("Harvest Context dependencies", test_context._core_management_db != null);
			assertTrue("Harvest Context dependencies", test_context._distributed_services != null);
			assertTrue("Harvest Context dependencies", test_context._logging_service != null);
			assertTrue("Harvest Context dependencies", test_context._globals != null);
			assertTrue("Harvest Context dependencies", test_context._service_context != null);
			
			// Check if started in "technology" (internal mode)
			assertEquals(test_context._state_name, HarvestContext.State.IN_TECHNOLOGY);
			
			// Check can request the harvest context libs without a bucket being set
			// (without exceptioning)
			test_context.getHarvestContextLibraries(Optional.empty());
			
			// Check that multiple calls to create harvester result in different contexts but with the same injection:
			final HarvestContext test_context2 = _app_injector.getInstance(HarvestContext.class);
			assertTrue("HarvestContext created", test_context2 != null);
			assertTrue("HarvestContexts different", test_context2 != test_context);
			assertEquals(test_context._service_context, test_context2._service_context);
			
			// Check calls that require that bucket/endpoint be set
			final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::_id, "test")
					.with(DataBucketBean::full_name, "/test/basicContextCreation")
					.with(DataBucketBean::modified, new Date())
					.with("data_schema", BeanTemplateUtils.build(DataSchemaBean.class)
							.with("search_index_schema", BeanTemplateUtils.build(DataSchemaBean.SearchIndexSchemaBean.class)
									.done().get())
							.done().get())
					.done().get();
			
			test_context.setBucket(test_bucket);
			assertEquals(test_bucket, test_context.getBucket().get());
			
		}
		catch (Exception e) {
			System.out.println(ErrorUtils.getLongForm("{1}: {0}", e, e.getClass()));
			fail("Threw exception");
		}
	}
		
	@Test
	public void test_externalContextCreation() throws InstantiationException, IllegalAccessException, ClassNotFoundException, InterruptedException, ExecutionException {
		_logger.info("running test_externalContextCreation");
		
		try {
			assertTrue("Config contains application name: " + ModuleUtils.getStaticConfig().root().toString(), ModuleUtils.getStaticConfig().root().toString().contains("application_name"));
			assertTrue("Config contains v1_enabled: " + ModuleUtils.getStaticConfig().root().toString(), ModuleUtils.getStaticConfig().root().toString().contains("v1_enabled"));
			
			final HarvestContext test_context = _app_injector.getInstance(HarvestContext.class);
	
			final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class)
													.with(DataBucketBean::_id, "test")
													.done().get();
			
			final SharedLibraryBean library = BeanTemplateUtils.build(SharedLibraryBean.class)
													.with(SharedLibraryBean::path_name, "/test/lib")
													.done().get();
			
			test_context.setTechnologyConfig(library);
			
			// Empty service set:
			final String signature = test_context.getHarvestContextSignature(Optional.of(test_bucket), Optional.empty());
			
			final String expected_sig = "com.ikanow.aleph2.data_import.services.HarvestContext:{\"030e2b82-0285-11e5-a322-1697f925ec7b\":\"{\\\"_id\\\":\\\"test\\\"}\",\"030e2b82-0285-11e5-a322-1697f925ec7c\":\"{\\\"path_name\\\":\\\"/test/lib\\\"}\",\"CoreDistributedServices\":{},\"MongoDbManagementDbService\":{\"mongodb_connection\":\"localhost:9999\"},\"globals\":{\"local_cached_jar_dir\":\"file://temp/\"},\"service\":{\"CoreDistributedServices\":{\"interface\":\"com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices\",\"service\":\"com.ikanow.aleph2.distributed_services.services.MockCoreDistributedServices\"},\"CoreManagementDbService\":{\"interface\":\"com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService\",\"service\":\"com.ikanow.aleph2.management_db.services.CoreManagementDbService\"},\"LoggingService\":{\"interface\":\"com.ikanow.aleph2.data_model.interfaces.shared_services.ILoggingService\",\"service\":\"com.ikanow.aleph2.logging.service.LoggingService\"},\"ManagementDbService\":{\"interface\":\"com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService\",\"service\":\"com.ikanow.aleph2.management_db.mongodb.services.MockMongoDbManagementDbService\"},\"SecurityService\":{\"interface\":\"com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService\",\"service\":\"com.ikanow.aleph2.data_model.interfaces.shared_services.MockSecurityService\"},\"StorageService\":{\"interface\":\"com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService\",\"service\":\"com.ikanow.aleph2.storage_service_hdfs.services.MockHdfsStorageService\"}}}"; 
			assertEquals(expected_sig, signature);
			
			// Additionals service set:

			final HarvestContext test_context2 = _app_injector.getInstance(HarvestContext.class);
			test_context2.setTechnologyConfig(library);
			final SharedLibraryBean module_library = BeanTemplateUtils.build(SharedLibraryBean.class)
					.with(SharedLibraryBean::_id, "_test_module")
					.with(SharedLibraryBean::path_name, "/test/module")
					.done().get();
			test_context2.setLibraryConfigs(ImmutableMap.<String, SharedLibraryBean>builder().put("/test/module", module_library).build());
			
			final String signature2 = test_context2.getHarvestContextSignature(Optional.of(test_bucket),
												Optional.of(
														ImmutableSet.<Tuple2<Class<? extends IUnderlyingService>, Optional<String>>>builder()
															.add(Tuples._2T(IStorageService.class, Optional.empty()))
															.add(Tuples._2T(IManagementDbService.class, Optional.of("test")))
															.build()																
														)
												);
			
			final String expected_sig2 = "com.ikanow.aleph2.data_import.services.HarvestContext:{\"030e2b82-0285-11e5-a322-1697f925ec7b\":\"{\\\"_id\\\":\\\"test\\\"}\",\"030e2b82-0285-11e5-a322-1697f925ec7c\":\"{\\\"path_name\\\":\\\"/test/lib\\\"}\",\"030e2b82-0285-11e5-a322-1697f925ec7d\":\"{\\\"libs\\\":[{\\\"_id\\\":\\\"_test_module\\\",\\\"path_name\\\":\\\"/test/module\\\"}]}\",\"CoreDistributedServices\":{},\"MongoDbManagementDbService\":{\"mongodb_connection\":\"localhost:9999\"},\"globals\":{\"local_cached_jar_dir\":\"file://temp/\"},\"service\":{\"CoreDistributedServices\":{\"interface\":\"com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices\",\"service\":\"com.ikanow.aleph2.distributed_services.services.MockCoreDistributedServices\"},\"CoreManagementDbService\":{\"interface\":\"com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService\",\"service\":\"com.ikanow.aleph2.management_db.services.CoreManagementDbService\"},\"LoggingService\":{\"interface\":\"com.ikanow.aleph2.data_model.interfaces.shared_services.ILoggingService\",\"service\":\"com.ikanow.aleph2.logging.service.LoggingService\"},\"ManagementDbService\":{\"interface\":\"com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService\",\"service\":\"com.ikanow.aleph2.management_db.mongodb.services.MockMongoDbManagementDbService\"},\"SecurityService\":{\"interface\":\"com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService\",\"service\":\"com.ikanow.aleph2.data_model.interfaces.shared_services.MockSecurityService\"},\"StorageService\":{\"interface\":\"com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService\",\"service\":\"com.ikanow.aleph2.storage_service_hdfs.services.MockHdfsStorageService\"},\"test\":{\"interface\":\"com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService\",\"service\":\"com.ikanow.aleph2.management_db.mongodb.services.MockMongoDbManagementDbService\"}}}"; 
			assertEquals(expected_sig2, signature2);
			
			// First fail because bucket not present
						
			final IHarvestContext test_external1a = ContextUtils.getHarvestContext(signature);		
			
			assertTrue("external context non null", test_external1a != null);
			
			assertTrue("external context of correct type", test_external1a instanceof HarvestContext);
			
			final HarvestContext test_external1b = (HarvestContext)test_external1a;
			
			assertTrue("Harvest Context dependencies", test_external1b._core_management_db != null);
			assertTrue("Harvest Context dependencies", test_external1b._logging_service != null);
			assertTrue("Harvest Context dependencies", test_external1b._distributed_services != null);
			assertTrue("Harvest Context dependencies", test_external1b._globals != null);
			assertTrue("Harvest Context dependencies", test_external1b._service_context != null);
			
			assertEquals("test", test_external1b._mutable_state.bucket.get()._id());
			
			// Check that it gets cloned
			
			final IHarvestContext test_external1a_1 = ContextUtils.getHarvestContext(signature);		
			
			assertTrue("external context non null", test_external1a_1 != null);
			
			assertTrue("external context of correct type", test_external1a_1 instanceof HarvestContext);
			
			final HarvestContext test_external1b_1 = (HarvestContext)test_external1a_1;
			
			assertEquals(test_external1b_1._distributed_services, test_external1b._distributed_services);
			
			// Finally, check I can see my extended services: 
			
			final IHarvestContext test_external2a = ContextUtils.getHarvestContext(signature2);		
			
			assertTrue("external context non null", test_external2a != null);
			
			assertTrue("external context of correct type", test_external2a instanceof HarvestContext);
			
			final HarvestContext test_external2b = (HarvestContext)test_external2a;
			
			assertTrue("Harvest Context dependencies", test_external2b._core_management_db != null);
			assertTrue("Harvest Context dependencies", test_external2b._logging_service != null);
			assertTrue("Harvest Context dependencies", test_external2b._distributed_services != null);
			assertTrue("Harvest Context dependencies", test_external2b._globals != null);
			assertTrue("Harvest Context dependencies", test_external2b._service_context != null);
			
			assertEquals("test", test_external2b._mutable_state.bucket.get()._id());
			
			assertEquals("/test/lib", test_external2b._mutable_state.technology_config.get().path_name());			
			assertEquals("/test/lib", test_external2b.getTechnologyLibraryConfig().path_name());
			
			assertTrue("I can see my additonal services", null != test_external2b._service_context.getService(IStorageService.class, Optional.empty()));
			assertTrue("I can see my additonal services", null != test_external2b._service_context.getService(IManagementDbService.class, Optional.of("test")));
			
			//Check some "won't work in module" calls:
			try {
				test_external2b.getHarvestContextSignature(null, null);
				fail("Should have errored");
			}
			catch (Exception e) {
				assertEquals(ErrorUtils.TECHNOLOGY_NOT_MODULE, e.getMessage());
			}
			try {
				test_external2b.getUnderlyingArtefacts();
				fail("Should have errored");
			}
			catch (Exception e) {
				assertEquals(ErrorUtils.TECHNOLOGY_NOT_MODULE, e.getMessage());
			}			
		}
		catch (Exception e) {
			System.out.println(ErrorUtils.getLongForm("{1}: {0}", e, e.getClass()));
			fail("Threw exception");
		}
	}
	
	@Test
	public void test_fileLocations() throws InstantiationException, IllegalAccessException, ClassNotFoundException, InterruptedException, ExecutionException {
		_logger.info("running test_fileLocations");
		
		try {
			final HarvestContext test_context = _app_injector.getInstance(HarvestContext.class);
			
			//All we can do here is test the trivial eclipse specific path:
			try { 
				File f = new File(test_context._globals.local_root_dir() + "/lib/aleph2_test_file_locations.jar");
				FileUtils.forceMkdir(f.getParentFile());
				FileUtils.touch(f);
			}
			catch (Exception e) {} // probably already exists:

			final DataBucketBean test_empty_bucket = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::_id, "test")
					.with(DataBucketBean::harvest_technology_name_or_id, "test_harvest_tech_id")
					.done().get();

			final SharedLibraryBean htlib1 = BeanTemplateUtils.build(SharedLibraryBean.class)
												.with(SharedLibraryBean::_id, "test_harvest_tech_id")
												.with(SharedLibraryBean::path_name, "test_harvest_tech_name.jar")
												.done().get();
			
			test_context.setBucket(test_empty_bucket);
			test_context.setTechnologyConfig(htlib1);

			final List<String> lib_paths = test_context.getHarvestContextLibraries(Optional.empty());

			//(this doesn't work very well when run in test mode because it's all being found from file)
			assertTrue("Finds some libraries", !lib_paths.isEmpty());
			lib_paths.stream().forEach(lib -> assertTrue("No external libraries: " + lib, lib.contains("aleph2")));
			
			assertTrue("Can find the test JAR or the data model: " +
							lib_paths.stream().collect(Collectors.joining(";")),
						lib_paths.stream().anyMatch(lib -> lib.contains("aleph2_test_file_locations"))
						||
						lib_paths.stream().anyMatch(lib -> lib.contains("aleph2_data_model"))
					);
			
			// Now get the various shared libs

			final HarvestControlMetadataBean harvest_module1 = BeanTemplateUtils.build(HarvestControlMetadataBean.class)
															.with(HarvestControlMetadataBean::library_names_or_ids, Arrays.asList("id1", "name2.zip"))
															.done().get();
			
			final HarvestControlMetadataBean harvest_module2 = BeanTemplateUtils.build(HarvestControlMetadataBean.class)
					.with(HarvestControlMetadataBean::library_names_or_ids, Arrays.asList("id1", "name3.test", "test_harvest_tech_id"))
															.done().get();
												
			final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::_id, "test")
					.with(DataBucketBean::harvest_technology_name_or_id, "test_harvest_tech_id")
					.with(DataBucketBean::harvest_configs, Arrays.asList(harvest_module1, harvest_module2))
					.done().get();
						
			final SharedLibraryBean htmod1 = BeanTemplateUtils.build(SharedLibraryBean.class)
					.with(SharedLibraryBean::_id, "id1")
					.with(SharedLibraryBean::path_name, "name1.jar")
					.done().get();
			
			final SharedLibraryBean htmod2 = BeanTemplateUtils.build(SharedLibraryBean.class)
					.with(SharedLibraryBean::_id, "id2")
					.with(SharedLibraryBean::path_name, "name2.zip")
					.done().get();
			
			final SharedLibraryBean htmod3 = BeanTemplateUtils.build(SharedLibraryBean.class)
					.with(SharedLibraryBean::_id, "id3")
					.with(SharedLibraryBean::path_name, "name3.test")
					.done().get();
			
			test_context._service_context.getService(IManagementDbService.class, Optional.empty()).get()
								.getSharedLibraryStore().storeObjects(Arrays.asList(htlib1, htmod1, htmod2, htmod3)).get();
			
			Map<String, String> mods = test_context.getHarvestLibraries(Optional.of(test_bucket)).get();
			assertTrue("name1:" + mods, mods.containsKey("name1.jar") && mods.get("name1.jar").endsWith("id1.cache.jar"));
			assertTrue("name2:" + mods, mods.containsKey("name2.zip") && mods.get("name2.zip").endsWith("id2.cache.zip"));
			assertTrue("name3:" + mods, mods.containsKey("name3.test") && mods.get("name3.test").endsWith("id3.cache.misc.test"));
			assertTrue("test_harvest_tech_name:" + mods, mods.containsKey("test_harvest_tech_name.jar") && mods.get("test_harvest_tech_name.jar").endsWith("test_harvest_tech_id.cache.jar"));
		}
		catch (Exception e) {
			try {
				e.printStackTrace();
			}
			catch (Exception ee) {
				System.out.println(ErrorUtils.getLongForm("{1}: {0}", e, e.getClass()));
			}
			fail("Threw exception");
		}
		
	}
	
	@Test
	public void test_produceConsume() throws JsonProcessingException, IOException, InterruptedException {
		_logger.info("running test_produceConsume");
		
		final DataBucketBean bucket = BeanTemplateUtils.build(DataBucketBean.class)
																.with(DataBucketBean::full_name, "/TEST/HARVEST/CONTEXT")
																.with(DataBucketBean::master_enrichment_type, MasterEnrichmentType.streaming)
														.done().get();
		final ObjectMapper mapper = BeanTemplateUtils.configureMapper(Optional.empty());
		
		final HarvestContext test_context = _app_injector.getInstance(HarvestContext.class);
		
		assertEquals(Optional.empty(), test_context.getBucket());
		String message1 = "{\"key\":\"val\"}";
		String message2 = "{\"key\":\"val2\"}";
		String message3 = "{\"key\":\"val3\"}";
		String message4 = "{\"key\":\"val4\"}";
		Map<String, Object> msg3 = ImmutableMap.<String, Object>builder().put("key", "val3").build();
		Map<String, Object> msg4 = ImmutableMap.<String, Object>builder().put("key", "val4").build();
		//currently mock cds produce does nothing
		try {
			test_context.sendObjectToStreamingPipeline(Optional.empty(), Either.left(mapper.readTree(message1)));
			fail("Should fail, bucket not set and not specified");
		}
		catch (Exception e) {}
		test_context.setBucket(bucket);
		assertEquals(bucket, test_context.getBucket().get());
		
		Iterator<String> iter = test_context._distributed_services.consumeAs(BucketUtils.getUniqueSignature("/TEST/HARVEST/CONTEXT", Optional.empty()), Optional.empty(), Optional.empty());
		
		test_context.sendObjectToStreamingPipeline(Optional.empty(), Either.left(mapper.readTree(message1)));
		test_context.sendObjectToStreamingPipeline(Optional.of(bucket), Either.left(mapper.readTree(message2)));
		test_context.sendObjectToStreamingPipeline(Optional.empty(), Either.right(msg3));
		test_context.sendObjectToStreamingPipeline(Optional.of(bucket), Either.right(msg4));
		Thread.sleep(5000); //wait a few seconds for producers to dump batch
		final HashSet<String> mutable_set = new HashSet<>(Arrays.asList(message1, message2, message3, message4));
		
		//nothing will be in consume
		
		long count = 0;
		while ( iter.hasNext() ) {
			String msg = iter.next();
			assertTrue("Sent this message: " + msg, mutable_set.remove(msg));
			count++;
		}
		assertEquals(4,count);
	}
		
	public static class TestBean {}
	
	@Test
	public void test_objectStateRetrieval() throws InterruptedException, ExecutionException {
		_logger.info("running test_objectStateRetrieval");
		
		final HarvestContext test_context = _app_injector.getInstance(HarvestContext.class);
		final DataBucketBean bucket = BeanTemplateUtils.build(DataBucketBean.class).with("full_name", "TEST_HARVEST_CONTEXT").done().get();

		final SharedLibraryBean lib_bean = BeanTemplateUtils.build(SharedLibraryBean.class).with("path_name", "TEST_HARVEST_CONTEXT").done().get();
		test_context.setTechnologyConfig(lib_bean);
		
		ICrudService<AssetStateDirectoryBean> dir_a = test_context._core_management_db.getStateDirectory(Optional.empty(), Optional.of(AssetStateDirectoryBean.StateDirectoryType.analytic_thread));
		ICrudService<AssetStateDirectoryBean> dir_e = test_context._core_management_db.getStateDirectory(Optional.empty(), Optional.of(AssetStateDirectoryBean.StateDirectoryType.enrichment));
		ICrudService<AssetStateDirectoryBean> dir_h = test_context._core_management_db.getStateDirectory(Optional.empty(), Optional.of(AssetStateDirectoryBean.StateDirectoryType.harvest));
		ICrudService<AssetStateDirectoryBean> dir_s = test_context._core_management_db.getStateDirectory(Optional.empty(), Optional.of(AssetStateDirectoryBean.StateDirectoryType.library));

		dir_a.deleteDatastore().get();
		dir_e.deleteDatastore().get();
		dir_h.deleteDatastore().get();
		dir_s.deleteDatastore().get();
		assertEquals(0, dir_a.countObjects().get().intValue());
		assertEquals(0, dir_e.countObjects().get().intValue());
		assertEquals(0, dir_h.countObjects().get().intValue());
		assertEquals(0, dir_s.countObjects().get().intValue());
		
		@SuppressWarnings("unused")
		ICrudService<TestBean> s1 = test_context.getGlobalHarvestTechnologyObjectStore(TestBean.class, Optional.of("test"));
		assertEquals(0, dir_a.countObjects().get().intValue());
		assertEquals(0, dir_e.countObjects().get().intValue());
		assertEquals(0, dir_h.countObjects().get().intValue());
		assertEquals(1, dir_s.countObjects().get().intValue());

		@SuppressWarnings("unused")
		ICrudService<TestBean> e1 = test_context.getBucketObjectStore(TestBean.class, Optional.of(bucket), Optional.of("test"), Optional.of(AssetStateDirectoryBean.StateDirectoryType.enrichment));
		assertEquals(0, dir_a.countObjects().get().intValue());
		assertEquals(1, dir_e.countObjects().get().intValue());
		assertEquals(0, dir_h.countObjects().get().intValue());
		assertEquals(1, dir_s.countObjects().get().intValue());
		
		test_context.setBucket(bucket);
		
		@SuppressWarnings("unused")
		ICrudService<TestBean> a1 = test_context.getBucketObjectStore(TestBean.class, Optional.empty(), Optional.of("test"), Optional.of(AssetStateDirectoryBean.StateDirectoryType.analytic_thread));
		assertEquals(1, dir_a.countObjects().get().intValue());
		assertEquals(1, dir_e.countObjects().get().intValue());
		assertEquals(0, dir_h.countObjects().get().intValue());
		assertEquals(1, dir_s.countObjects().get().intValue());
		
		@SuppressWarnings("unused")
		ICrudService<TestBean> h1 = test_context.getBucketObjectStore(TestBean.class, Optional.empty(), Optional.of("test"), Optional.of(AssetStateDirectoryBean.StateDirectoryType.harvest));
		assertEquals(1, dir_a.countObjects().get().intValue());
		assertEquals(1, dir_e.countObjects().get().intValue());
		assertEquals(1, dir_h.countObjects().get().intValue());
		assertEquals(1, dir_s.countObjects().get().intValue());		

		ICrudService<AssetStateDirectoryBean> dir_h_2 = test_context.getBucketObjectStore(AssetStateDirectoryBean.class, Optional.empty(), Optional.empty(), Optional.empty());
		assertEquals(1, dir_h_2.countObjects().get().intValue());
		
		@SuppressWarnings("unused")
		ICrudService<TestBean> h2 = test_context.getBucketObjectStore(TestBean.class, Optional.empty(), Optional.of("test_2"), Optional.empty());
		assertEquals(1, dir_a.countObjects().get().intValue());
		assertEquals(1, dir_e.countObjects().get().intValue());
		assertEquals(2, dir_h.countObjects().get().intValue());
		assertEquals(1, dir_s.countObjects().get().intValue());		
		assertEquals(2, dir_h_2.countObjects().get().intValue());
		
		@SuppressWarnings("unused")
		ICrudService<TestBean> s2 = test_context.getBucketObjectStore(TestBean.class, Optional.empty(), Optional.of("test_2"), Optional.of(AssetStateDirectoryBean.StateDirectoryType.library));
		assertEquals(1, dir_a.countObjects().get().intValue());
		assertEquals(1, dir_e.countObjects().get().intValue());
		assertEquals(2, dir_h.countObjects().get().intValue());
		assertEquals(2, dir_s.countObjects().get().intValue());
		
	}
	
	@Test
	public void test_misc() {
		_logger.info("running test_misc");
		try {
			assertTrue("Injector created", _app_injector != null);		
			final HarvestContext test_context = _app_injector.getInstance(HarvestContext.class);
			assertEquals(Optional.empty(), test_context.getUnderlyingPlatformDriver(String.class, Optional.empty()));
		}
		catch (Exception e) {
			try {
				e.printStackTrace();
			}
			catch (Exception ee) {
				System.out.println(ErrorUtils.getLongForm("{1}: {0}", e, e.getClass()));
			}
			fail("Threw exception");
		}
	}

	@Test
	public void test_getUnderlyingArtefacts() {
		_logger.info("running test_getUnderlyingArtefacts");
		
		final HarvestContext test_context = _app_injector.getInstance(HarvestContext.class);
		
		// (interlude: check errors if called before getSignature
		try {
			test_context.getUnderlyingArtefacts();
			fail("Should have errored");
		}
		catch (Exception e) {
			assertEquals(ErrorUtils.SERVICE_RESTRICTIONS, e.getMessage());
		}
		
		final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class)
												.with(DataBucketBean::_id, "test")
												.with(DataBucketBean::full_name, "/test/get_underlying/artefacts")
												.with(DataBucketBean::modified, new Date())
												.with("data_schema", BeanTemplateUtils.build(DataSchemaBean.class)
														.with("search_index_schema", BeanTemplateUtils.build(DataSchemaBean.SearchIndexSchemaBean.class)
																.done().get())
														.done().get())
												.done().get();
		
		final SharedLibraryBean library = BeanTemplateUtils.build(SharedLibraryBean.class)
				.with(SharedLibraryBean::path_name, "/test/lib")
				.done().get();
		test_context.setTechnologyConfig(library);		
		
		// Empty service set:
		test_context.getHarvestContextSignature(Optional.of(test_bucket), Optional.empty());		
		final Collection<Object> res1 = test_context.getUnderlyingArtefacts();
		final String exp1 = "class com.ikanow.aleph2.core.shared.utils.SharedErrorUtils:class com.ikanow.aleph2.data_import.services.HarvestContext:class com.ikanow.aleph2.data_model.utils.ModuleUtils$ServiceContext:class com.ikanow.aleph2.distributed_services.services.MockCoreDistributedServices:class com.ikanow.aleph2.logging.service.LoggingService:class com.ikanow.aleph2.management_db.mongodb.services.MockMongoDbManagementDbService:class com.ikanow.aleph2.management_db.services.CoreManagementDbService:class com.ikanow.aleph2.shared.crud.mongodb.services.MockMongoDbCrudServiceFactory:class com.ikanow.aleph2.storage_service_hdfs.services.MockHdfsStorageService";
		assertEquals(exp1, res1.stream().map(o -> o.getClass().toString()).sorted().collect(Collectors.joining(":")));
	}
	
}
