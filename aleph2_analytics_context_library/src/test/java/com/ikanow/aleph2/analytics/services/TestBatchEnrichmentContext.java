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
package com.ikanow.aleph2.analytics.services;

import static org.junit.Assert.*;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.analytics.services.AnalyticsContext;
import com.ikanow.aleph2.analytics.services.BatchEnrichmentContext;
import com.ikanow.aleph2.analytics.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_import.AnnotationBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean.MasterEnrichmentType;
import com.ikanow.aleph2.data_model.objects.shared.AssetStateDirectoryBean;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ContextUtils;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class TestBatchEnrichmentContext {
	static final Logger _logger = LogManager.getLogger(); 

	protected ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());
	protected Injector _app_injector;
	
	// All the services
	@Inject IServiceContext _service_context;
	IManagementDbService _core_management_db;
	ICoreDistributedServices _distributed_services;
	ISearchIndexService _index_service;
	GlobalPropertiesBean _globals;	
	
	AnalyticsContext _mock_analytics_context;
	
	
	@Before
	public void injectModules() throws Exception {
		_logger.info("run injectModules");
		
		final Config config = ConfigFactory.parseFile(new File("./src/test/resources/context_local_test.properties"));
		
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
		
		
		_app_injector.injectMembers(this);
		_mock_analytics_context = new AnalyticsContext(_service_context);
		_core_management_db = _service_context.getCoreManagementDbService();
		_distributed_services = _service_context.getService(ICoreDistributedServices.class, Optional.empty()).get();
		_index_service = _service_context.getSearchIndexService().get();
		_globals = _service_context.getGlobalProperties();
	}
	
	private Tuple2<AnalyticsContext, BatchEnrichmentContext> getContextPair() {
		final AnalyticsContext analytics_context = new AnalyticsContext(_service_context);
		final BatchEnrichmentContext stream_context = new BatchEnrichmentContext(analytics_context);
		return Tuples._2T(analytics_context, stream_context);
	}
	
	@SuppressWarnings("unused")
	@Test
	public void test_basicContextCreation() throws Exception {
		_logger.info("run test_basicContextCreation");
		try {
			assertTrue("Injector created", _app_injector != null);
		
			Tuple2<AnalyticsContext, BatchEnrichmentContext> context_pair = getContextPair();
			final BatchEnrichmentContext test_context = context_pair._2();
						
			assertTrue("Find service", test_context.getServiceContext().getService(ISearchIndexService.class, Optional.empty()).isPresent());
			
			// Check that multiple calls to create harvester result in different contexts but with the same injection:
			Tuple2<AnalyticsContext, BatchEnrichmentContext> context_pair2 = getContextPair();
			final BatchEnrichmentContext test_context2 = context_pair2._2();
			assertTrue("BatchEnrichmentContext created", test_context2 != null);
			assertTrue("BatchEnrichmentContexts different", test_context2 != test_context);
			assertEquals(test_context.getServiceContext(), test_context.getServiceContext());
			
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
			
			final AnalyticThreadJobBean.AnalyticThreadJobOutputBean analytic_output1 =  
					BeanTemplateUtils.build(AnalyticThreadJobBean.AnalyticThreadJobOutputBean.class)
						.with(AnalyticThreadJobBean.AnalyticThreadJobOutputBean::transient_type, MasterEnrichmentType.streaming)
						.with(AnalyticThreadJobBean.AnalyticThreadJobOutputBean::is_transient, false)
					.done().get();		

			final AnalyticThreadJobBean.AnalyticThreadJobOutputBean analytic_output2 =  
					BeanTemplateUtils.build(AnalyticThreadJobBean.AnalyticThreadJobOutputBean.class)
						.with(AnalyticThreadJobBean.AnalyticThreadJobOutputBean::transient_type, MasterEnrichmentType.streaming)
						.with(AnalyticThreadJobBean.AnalyticThreadJobOutputBean::is_transient, true)
					.done().get();		

			final AnalyticThreadJobBean.AnalyticThreadJobOutputBean analytic_output3 =  
					BeanTemplateUtils.build(AnalyticThreadJobBean.AnalyticThreadJobOutputBean.class)
						.with(AnalyticThreadJobBean.AnalyticThreadJobOutputBean::transient_type, MasterEnrichmentType.batch)
						.with(AnalyticThreadJobBean.AnalyticThreadJobOutputBean::is_transient, true)
					.done().get();		
			
			final AnalyticThreadJobBean analytic_job1 = BeanTemplateUtils.build(AnalyticThreadJobBean.class)
					.with(AnalyticThreadJobBean::output, analytic_output1)
					.with(AnalyticThreadJobBean::name, "analytic_output1")
					.done().get();
			final AnalyticThreadJobBean analytic_job2 = BeanTemplateUtils.build(AnalyticThreadJobBean.class)
					.with(AnalyticThreadJobBean::output, analytic_output2)
					.with(AnalyticThreadJobBean::name, "analytic_output2")
					.done().get();
			final AnalyticThreadJobBean analytic_job3 = BeanTemplateUtils.build(AnalyticThreadJobBean.class)
					.with(AnalyticThreadJobBean::output, analytic_output3)
					.with(AnalyticThreadJobBean::name, "analytic_output3")
					.done().get();
						
			final SharedLibraryBean mod_library = BeanTemplateUtils.build(SharedLibraryBean.class)
					.with(SharedLibraryBean::_id, "_test_module")
					.with(SharedLibraryBean::path_name, "/test/module")
					.done().get();
			final SharedLibraryBean tech_library = BeanTemplateUtils.build(SharedLibraryBean.class)
					.with(SharedLibraryBean::path_name, "/test/tech")
					.done().get();
			
			context_pair._1().resetLibraryConfigs(							
					ImmutableMap.<String, SharedLibraryBean>builder()
						.put(mod_library.path_name(), mod_library)
						.put(mod_library._id(), mod_library)
						.build());
			context_pair._1().setTechnologyConfig(tech_library);
			context_pair._1().setBucket(test_bucket);			
			
			//TODO: ALEPH-12: replace this topology testing code:
//			context_pair._2().setUserTopology(new IEnrichmentStreamingTopology() {
//
//				@Override
//				public Tuple2<Object, Map<String, String>> getTopologyAndConfiguration(
//						DataBucketBean bucket, IEnrichmentModuleContext context) {
//					return null;
//				}
//
//				@Override
//				public <O> JsonNode rebuildObject(
//						O raw_outgoing_object,
//						Function<O, LinkedHashMap<String, Object>> generic_outgoing_object_builder) {
//					return null;
//				}				
//			});
//			
//			// Test the 3 different cases for user topologies
//			
//			// 1) Transient job => gets user topology
//			
//			{
//				context_pair._2().setJob(analytic_job1);			
//				final Object endpoint_1 = test_context.getTopologyStorageEndpoint(Object.class, Optional.empty());
//				assertTrue("This endpoint should be OutputBolt: " + endpoint_1.getClass(), endpoint_1 instanceof OutputBolt);
//				
//				// (check it's serializable - ie that this doesn't exception)
//				ObjectOutputStream out_bolt1 = new ObjectOutputStream(new ByteArrayOutputStream());
//				out_bolt1.writeObject(endpoint_1);
//				out_bolt1.close(); 
//			}
//			
//			// 2) Non-transient, output topic exists (ie output is streaming)
//			{
//				context_pair._2().setJob(analytic_job2);			
//				final Object endpoint_2 = test_context.getTopologyStorageEndpoint(Object.class, Optional.empty());
//				assertTrue("This endpoint should be KafkaBolt: " + endpoint_2.getClass(), endpoint_2 instanceof TransientStreamingOutputBolt);
//				
//				// (check it's serializable - ie that this doesn't exception)
//				ObjectOutputStream out_bolt2 = new ObjectOutputStream(new ByteArrayOutputStream());
//				out_bolt2.writeObject(endpoint_2);
//				out_bolt2.close(); 
//			}
//			// 3) Non-transient, batch output
//			
//			context_pair._2().setJob(analytic_job3);			
//			try {
//				test_context.getTopologyStorageEndpoint(Object.class, Optional.empty());
//				fail("Should have errored");
//			}
//			catch(Exception e) {
//			}
		}
		catch (Exception e) {
			System.out.println(ErrorUtils.getLongForm("{1}: {0}", e, e.getClass()));
			throw e;
		}
	}
	
	@Test
	public void test_ExternalContextCreation() throws InstantiationException, IllegalAccessException, ClassNotFoundException, InterruptedException, ExecutionException {
		_logger.info("run test_ExternalContextCreation");
		try {
			assertTrue("Config contains application name: " + ModuleUtils.getStaticConfig().root().toString(), ModuleUtils.getStaticConfig().root().toString().contains("application_name"));
			assertTrue("Config contains v1_enabled: " + ModuleUtils.getStaticConfig().root().toString(), ModuleUtils.getStaticConfig().root().toString().contains("v1_enabled"));
			
			Tuple2<AnalyticsContext, BatchEnrichmentContext> context_pair = getContextPair();
			final BatchEnrichmentContext test_context = context_pair._2();

			final AnalyticThreadJobBean analytic_job1 = BeanTemplateUtils.build(AnalyticThreadJobBean.class)
					.with(AnalyticThreadJobBean::name, "analytic_job1")
					.done().get();

			final AnalyticThreadBean analytic_thread = BeanTemplateUtils.build(AnalyticThreadBean.class)
															.with(AnalyticThreadBean::jobs, Arrays.asList(analytic_job1))
														.done().get();
			
			final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class)
													.with(DataBucketBean::_id, "test")
													.with(DataBucketBean::full_name, "/test/external-context/creation")
													.with(DataBucketBean::modified, new Date(1436194933000L))
													.with(DataBucketBean::analytic_thread, analytic_thread)
													.with("data_schema", BeanTemplateUtils.build(DataSchemaBean.class)
															.with("search_index_schema", BeanTemplateUtils.build(DataSchemaBean.SearchIndexSchemaBean.class)
																	.done().get())
															.done().get())
													.done().get();						
			
			final SharedLibraryBean mod_library = BeanTemplateUtils.build(SharedLibraryBean.class)
					.with(SharedLibraryBean::_id, "_test_module")
					.with(SharedLibraryBean::path_name, "/test/module")
					.done().get();
			final SharedLibraryBean tech_library = BeanTemplateUtils.build(SharedLibraryBean.class)
					.with(SharedLibraryBean::path_name, "/test/tech")
					.done().get();

			context_pair._1().resetLibraryConfigs(							
					ImmutableMap.<String, SharedLibraryBean>builder()
						.put(mod_library.path_name(), mod_library)
						.put(mod_library._id(), mod_library)
						.build());
			context_pair._1().setTechnologyConfig(tech_library);
			context_pair._1().setBucket(test_bucket);
			context_pair._2().setJob(analytic_job1);
			
			// Empty service set:
			final String signature = test_context.getEnrichmentContextSignature(Optional.of(test_bucket), Optional.empty());
						
			final String expected_sig = "com.ikanow.aleph2.analytics.services.BatchEnrichmentContext:analytic_job1:com.ikanow.aleph2.analytics.services.AnalyticsContext:{\"3fdb4bfa-2024-11e5-b5f7-727283247c7e\":\"{\\\"_id\\\":\\\"test\\\",\\\"modified\\\":1436194933000,\\\"full_name\\\":\\\"/test/external-context/creation\\\",\\\"analytic_thread\\\":{\\\"jobs\\\":[{\\\"name\\\":\\\"analytic_job1\\\"}]},\\\"data_schema\\\":{\\\"search_index_schema\\\":{}}}\",\"3fdb4bfa-2024-11e5-b5f7-727283247c7f\":\"{\\\"path_name\\\":\\\"/test/tech\\\"}\",\"3fdb4bfa-2024-11e5-b5f7-727283247cff\":\"{\\\"libs\\\":[{\\\"_id\\\":\\\"_test_module\\\",\\\"path_name\\\":\\\"/test/module\\\"}]}\",\"CoreDistributedServices\":{},\"MongoDbManagementDbService\":{},\"service\":{\"CoreDistributedServices\":{\"interface\":\"com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices\",\"service\":\"com.ikanow.aleph2.distributed_services.services.NoCoreDistributedServices\"},\"CoreManagementDbService\":{\"interface\":\"com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService\",\"service\":\"com.ikanow.aleph2.management_db.mongodb.services.MockMongoDbManagementDbService\"},\"LoggingService\":{\"interface\":\"com.ikanow.aleph2.data_model.interfaces.shared_services.ILoggingService\",\"service\":\"com.ikanow.aleph2.logging.service.LoggingService\"},\"ManagementDbService\":{\"interface\":\"com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService\",\"service\":\"com.ikanow.aleph2.management_db.mongodb.services.MockMongoDbManagementDbService\"},\"SearchIndexService\":{\"interface\":\"com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService\",\"service\":\"com.ikanow.aleph2.search_service.elasticsearch.services.MockElasticsearchIndexService\"},\"SecurityService\":{\"interface\":\"com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService\",\"service\":\"com.ikanow.aleph2.security.service.NoSecurityService\"},\"StorageService\":{\"interface\":\"com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService\",\"service\":\"com.ikanow.aleph2.storage_service_hdfs.services.MockHdfsStorageService\"}}}";			
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
			Tuple2<AnalyticsContext, BatchEnrichmentContext> context_pair2 = getContextPair();
			final BatchEnrichmentContext test_context2 = context_pair2._2();

			context_pair2._1().resetLibraryConfigs(							
					ImmutableMap.<String, SharedLibraryBean>builder()
						.put(mod_library.path_name(), mod_library)
						.put(mod_library._id(), mod_library)
						.build());
			context_pair2._1().setTechnologyConfig(tech_library);
			context_pair2._1().setBucket(test_bucket);
			context_pair2._1().resetJob(analytic_job1);
			context_pair2._2().setJob(analytic_job1);

			final String signature2 = test_context2.getEnrichmentContextSignature(Optional.of(test_bucket),
					Optional.of(
							ImmutableSet.<Tuple2<Class<? extends IUnderlyingService>, Optional<String>>>builder()
								.add(Tuples._2T(IStorageService.class, Optional.empty()))
								.add(Tuples._2T(IManagementDbService.class, Optional.of("test")))
								.build()																
							)
					);
			
			
			final String expected_sig2 = "com.ikanow.aleph2.analytics.services.BatchEnrichmentContext:analytic_job1:com.ikanow.aleph2.analytics.services.AnalyticsContext:{\"3fdb4bfa-2024-11e5-b5f7-727283247c7e\":\"{\\\"_id\\\":\\\"test\\\",\\\"modified\\\":1436194933000,\\\"full_name\\\":\\\"/test/external-context/creation\\\",\\\"analytic_thread\\\":{\\\"jobs\\\":[{\\\"name\\\":\\\"analytic_job1\\\"}]},\\\"data_schema\\\":{\\\"search_index_schema\\\":{}}}\",\"3fdb4bfa-2024-11e5-b5f7-727283247c7f\":\"{\\\"path_name\\\":\\\"/test/tech\\\"}\",\"3fdb4bfa-2024-11e5-b5f7-727283247cff\":\"{\\\"libs\\\":[{\\\"_id\\\":\\\"_test_module\\\",\\\"path_name\\\":\\\"/test/module\\\"}]}\",\"3fdb4bfa-2024-11e5-b5f7-7272832480f0\":\"analytic_job1\",\"CoreDistributedServices\":{},\"MongoDbManagementDbService\":{},\"service\":{\"CoreDistributedServices\":{\"interface\":\"com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices\",\"service\":\"com.ikanow.aleph2.distributed_services.services.NoCoreDistributedServices\"},\"CoreManagementDbService\":{\"interface\":\"com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService\",\"service\":\"com.ikanow.aleph2.management_db.mongodb.services.MockMongoDbManagementDbService\"},\"LoggingService\":{\"interface\":\"com.ikanow.aleph2.data_model.interfaces.shared_services.ILoggingService\",\"service\":\"com.ikanow.aleph2.logging.service.LoggingService\"},\"ManagementDbService\":{\"interface\":\"com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService\",\"service\":\"com.ikanow.aleph2.management_db.mongodb.services.MockMongoDbManagementDbService\"},\"SearchIndexService\":{\"interface\":\"com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService\",\"service\":\"com.ikanow.aleph2.search_service.elasticsearch.services.MockElasticsearchIndexService\"},\"SecurityService\":{\"interface\":\"com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService\",\"service\":\"com.ikanow.aleph2.security.service.NoSecurityService\"},\"StorageService\":{\"interface\":\"com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService\",\"service\":\"com.ikanow.aleph2.storage_service_hdfs.services.MockHdfsStorageService\"},\"test\":{\"interface\":\"com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService\",\"service\":\"com.ikanow.aleph2.management_db.mongodb.services.MockMongoDbManagementDbService\"}}}"; 
			assertEquals(expected_sig2, signature2);
			
			final IEnrichmentModuleContext test_external1a = ContextUtils.getEnrichmentContext(signature);		
			
			assertTrue("external context non null", test_external1a != null);
			
			assertTrue("external context of correct type", test_external1a instanceof BatchEnrichmentContext);
			
			assertTrue("BatchEnrichmentContext dependencies", test_external1a.getServiceContext().getCoreManagementDbService() != null);
			
			// Ceck I can see my extended services: 
			
			final IEnrichmentModuleContext test_external2a = ContextUtils.getEnrichmentContext(signature2);		
			
			assertTrue("external context non null", test_external2a != null);
			
			assertTrue("external context of correct type", test_external2a instanceof BatchEnrichmentContext);
			
			final BatchEnrichmentContext test_external2b = (BatchEnrichmentContext)test_external2a;
			
			assertFalse("Module not located", test_external2b.getModuleConfig().isPresent());
			test_external2b.setJob(BeanTemplateUtils.clone(analytic_job1).with(AnalyticThreadJobBean::module_name_or_id, "_test_module").done());
			assertTrue("Module located", test_external2b.getModuleConfig().isPresent());
			assertEquals("/test/module", test_external2b.getModuleConfig().get().path_name());			
			
			assertTrue("I can see my additonal services", null != test_external2b.getServiceContext().getService(IStorageService.class, Optional.empty()));
			assertTrue("I can see my additonal services", null != test_external2b.getServiceContext().getService(IManagementDbService.class, Optional.of("test")));
						
			//Check some "won't work in module" calls:
			test_external2b.setJob(analytic_job1);
			assertFalse("Module not located", test_external2b.getModuleConfig().isPresent());
			try {
				test_external2b.getEnrichmentContextSignature(null, null);
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
			try {
				test_external2b.getTopologyEntryPoints(String.class, Optional.empty());
				fail("Should have errored");
			}
			catch (Exception e) {
				assertEquals(ErrorUtils.get(ErrorUtils.NOT_YET_IMPLEMENTED, "batch_topology"), e.getMessage());
			}
			try {
				test_external2b.getTopologyStorageEndpoint(String.class, Optional.empty());
				fail("Should have errored");
			}
			catch (Exception e) {
				assertEquals(ErrorUtils.get(ErrorUtils.NOT_YET_IMPLEMENTED, "batch_topology"), e.getMessage());
			}
			try {
				test_external2b.getTopologyErrorEndpoint(null, null);
				fail("Should have errored");
			}
			catch (Exception e) {
				assertEquals(ErrorUtils.get(ErrorUtils.NOT_YET_IMPLEMENTED, "batch_topology"), e.getMessage());
			}
			
		}
		catch (Exception e) {
			try {//(if it's a guice error then this will error out, but you can still use the long form error below for diagnosis)
				e.printStackTrace();
			}
			catch (Throwable t) {}
			
			System.out.println(ErrorUtils.getLongForm("{1}: {0}", e, e.getClass()));
			fail("Threw exception");
		}
	}

	@Test
	public void test_getUnderlyingArtefacts() {
		_logger.info("run test_getUnderlyingArtefacts");
		
		Tuple2<AnalyticsContext, BatchEnrichmentContext> context_pair = getContextPair();
		final BatchEnrichmentContext test_context = context_pair._2();
		
		// (interlude: check errors if called before getSignature
		try {
			test_context.getUnderlyingArtefacts();
			fail("Should have errored");
		}
		catch (Exception e) {
			assertEquals(com.ikanow.aleph2.analytics.utils.ErrorUtils.SERVICE_RESTRICTIONS, e.getMessage());		
		}
		
		final AnalyticThreadJobBean analytic_job1 = BeanTemplateUtils.build(AnalyticThreadJobBean.class)
				.with(AnalyticThreadJobBean::name, "analytic_job1")
				.done().get();
		
		final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class)
												.with(DataBucketBean::_id, "test")
												.with(DataBucketBean::full_name, "/test/get_underlying/artefacts")
												.with(DataBucketBean::modified, new Date())
												.with("data_schema", BeanTemplateUtils.build(DataSchemaBean.class)
														.with("search_index_schema", BeanTemplateUtils.build(DataSchemaBean.SearchIndexSchemaBean.class)
																.done().get())
														.done().get())
												.done().get();
		
		final SharedLibraryBean mod_library = BeanTemplateUtils.build(SharedLibraryBean.class)
				.with(SharedLibraryBean::_id, "_test_module")
				.with(SharedLibraryBean::path_name, "/test/module")
				.done().get();
		final SharedLibraryBean tech_library = BeanTemplateUtils.build(SharedLibraryBean.class)
				.with(SharedLibraryBean::path_name, "/test/tech")
				.done().get();
		
		context_pair._1().resetLibraryConfigs(							
				ImmutableMap.<String, SharedLibraryBean>builder()
					.put(mod_library.path_name(), mod_library)
					.put(mod_library._id(), mod_library)
					.build());
		context_pair._1().setTechnologyConfig(tech_library);
		context_pair._2().setJob(analytic_job1);
		
		// Empty service set:
		test_context.getEnrichmentContextSignature(Optional.of(test_bucket), Optional.empty());		
		final Collection<Object> res1 = test_context.getUnderlyingArtefacts();
		final String exp1 = "class com.ikanow.aleph2.analytics.services.AnalyticsContext:class com.ikanow.aleph2.analytics.services.BatchEnrichmentContext:class com.ikanow.aleph2.core.shared.utils.SharedErrorUtils:class com.ikanow.aleph2.data_model.utils.ModuleUtils$ServiceContext:class com.ikanow.aleph2.logging.service.LoggingService:class com.ikanow.aleph2.management_db.mongodb.services.MockMongoDbManagementDbService:class com.ikanow.aleph2.search_service.elasticsearch.services.MockElasticsearchIndexService:class com.ikanow.aleph2.security.service.NoSecurityService:class com.ikanow.aleph2.shared.crud.elasticsearch.services.MockElasticsearchCrudServiceFactory:class com.ikanow.aleph2.shared.crud.mongodb.services.MockMongoDbCrudServiceFactory:class com.ikanow.aleph2.storage_service_hdfs.services.MockHdfsStorageService";
		assertEquals(exp1, res1.stream().map(o -> o.getClass().toString()).sorted().collect(Collectors.joining(":")));
		
		// Check can retrieve the analytics context:
		
		Optional<IAnalyticsContext> analytics_context = test_context.getUnderlyingPlatformDriver(IAnalyticsContext.class, Optional.empty());
		assertTrue("Found analytics context", analytics_context.isPresent());
		assertTrue("Analytics context is of correct type: " + analytics_context.get(), analytics_context.get() instanceof AnalyticsContext);
	}
	
	@Test
	public void test_misc() {
		_logger.info("run test_misc");
		
		assertTrue("Injector created", _app_injector != null);
		
		Tuple2<AnalyticsContext, BatchEnrichmentContext> context_pair = getContextPair();
		final BatchEnrichmentContext test_context = context_pair._2();

		assertEquals(Optional.empty(), test_context.getUnderlyingPlatformDriver(String.class, Optional.empty()));
		assertEquals(Optional.empty(), test_context.getBucket());
		
		try {
			test_context.emergencyQuarantineBucket(null, null);
			fail("Should have thrown exception");
		}
		catch (Exception e) {
			assertEquals(ErrorUtils.get(ErrorUtils.NOT_YET_IMPLEMENTED, "emergencyQuarantineBucket"), e.getMessage());
		}
		try {
			test_context.emergencyDisableBucket(null);
			fail("Should have thrown exception");
		}
		catch (Exception e) {
			assertEquals(ErrorUtils.get(ErrorUtils.NOT_YET_IMPLEMENTED, "emergencyDisableBucket"), e.getMessage());
		}
		// (This has now been implemented, though not ready to test yet)
//		try {
//			test_context.getBucketStatus(null);
//			fail("Should have thrown exception");
//		}
//		catch (Exception e) {
//			assertEquals(ErrorUtils.NOT_YET_IMPLEMENTED, e.getMessage());
//		}
		assertEquals(0L, test_context.getNextUnusedId());
		assertEquals(0L, test_context.getNextUnusedId());
		
		try {
			test_context.storeErroredObject(0L, null);
			fail("Should have thrown exception");
		}
		catch (Exception e) {
			assertEquals(ErrorUtils.get(ErrorUtils.NOT_YET_IMPLEMENTED, "storeErroredObject"), e.getMessage());
		}
		try {
			test_context.getTopologyErrorEndpoint(null, null);
			fail("Should have errored");
		}
		catch (Exception e) {
			assertEquals(ErrorUtils.get(ErrorUtils.NOT_YET_IMPLEMENTED, "batch_topology"), e.getMessage());
		}
	}
	
	@Test
	public void test_objectEmitting() throws InterruptedException, ExecutionException, InstantiationException, IllegalAccessException, ClassNotFoundException {
		_logger.info("run test_objectEmitting");

		Tuple2<AnalyticsContext, BatchEnrichmentContext> context_pair = getContextPair();
		final BatchEnrichmentContext test_context = context_pair._2();
		
		final AnalyticThreadJobBean analytic_job1 = BeanTemplateUtils.build(AnalyticThreadJobBean.class)
				.with(AnalyticThreadJobBean::name, "analytic_job1")
				.done().get();
		
		final AnalyticThreadBean analytic_thread = BeanTemplateUtils.build(AnalyticThreadBean.class)
				.with(AnalyticThreadBean::jobs, Arrays.asList(analytic_job1))
			.done().get();

		
		final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::_id, "test_objectemitting")
				.with(DataBucketBean::analytic_thread, analytic_thread)
				.with(DataBucketBean::full_name, "/test/object/emitting")
				.with(DataBucketBean::modified, new Date())
				.with("data_schema", BeanTemplateUtils.build(DataSchemaBean.class)
						.with("search_index_schema", BeanTemplateUtils.build(DataSchemaBean.SearchIndexSchemaBean.class)
								.done().get())
						.done().get())
				.done().get();

		final SharedLibraryBean mod_library = BeanTemplateUtils.build(SharedLibraryBean.class)
				.with(SharedLibraryBean::_id, "_test_module")
				.with(SharedLibraryBean::path_name, "/test/module")
				.done().get();
		final SharedLibraryBean tech_library = BeanTemplateUtils.build(SharedLibraryBean.class)
				.with(SharedLibraryBean::path_name, "/test/tech")
				.done().get();
		
		context_pair._1().resetLibraryConfigs(							
				ImmutableMap.<String, SharedLibraryBean>builder()
					.put(mod_library.path_name(), mod_library)
					.put(mod_library._id(), mod_library)
					.build());
		context_pair._1().setTechnologyConfig(tech_library);
		context_pair._1().setBucket(test_bucket);
		context_pair._2().setJob(analytic_job1);
		
		// Empty service set:
		final String signature = test_context.getEnrichmentContextSignature(Optional.of(test_bucket), Optional.empty());

		final ICrudService<DataBucketBean> raw_mock_db = _core_management_db.getDataBucketStore(); // (note this is actually the underlying db because we're in a test here)
		raw_mock_db.deleteDatastore().get();
		assertEquals(0L, (long)raw_mock_db.countObjects().get());
		raw_mock_db.storeObject(test_bucket).get();		
		assertEquals(1L, (long)raw_mock_db.countObjects().get());
		
		final IEnrichmentModuleContext test_external1a = ContextUtils.getEnrichmentContext(signature);		

		final JsonNode jn1 = _mapper.createObjectNode().put("test", "test1");
		final JsonNode jn2 = _mapper.createObjectNode().put("test", "test2");
		
		//(try some errors)
		try {
			test_external1a.emitMutableObject(0, test_external1a.convertToMutable(jn1), Optional.of(BeanTemplateUtils.build(AnnotationBean.class).done().get()), Optional.empty());
			fail("Should have thrown exception");
		}
		catch (Exception e) {
			assertEquals(ErrorUtils.get(ErrorUtils.NOT_YET_IMPLEMENTED, "annotations"), e.getMessage());
		}
		try {
			test_external1a.emitImmutableObject(0, jn2, Optional.empty(), Optional.of(BeanTemplateUtils.build(AnnotationBean.class).done().get()), Optional.empty());
			fail("Should have thrown exception");
		}
		catch (Exception e) {
			assertEquals(ErrorUtils.get(ErrorUtils.NOT_YET_IMPLEMENTED, "annotations"), e.getMessage());
		}
		
		test_external1a.emitMutableObject(0, test_external1a.convertToMutable(jn1), Optional.empty(), Optional.empty());
		test_external1a.emitImmutableObject(0, jn2, Optional.empty(), Optional.empty(), Optional.empty());		
		test_external1a.emitImmutableObject(0, jn2, 
				Optional.of(_mapper.createObjectNode().put("extra", "test3_extra").put("test", "test3")), 
				Optional.empty(), Optional.empty());
		
		BatchEnrichmentContext test_peek_inside_external1 = (BatchEnrichmentContext)  test_external1a;
		
		assertEquals(3, test_peek_inside_external1.getOutputRecords().size());
		test_peek_inside_external1.clearOutputRecords();
		assertEquals(0, test_peek_inside_external1.getOutputRecords().size());
	}

	public static class TestBean {}	
	
	@Test
	public void test_objectStateRetrieval() throws InterruptedException, ExecutionException {
		_logger.info("run test_objectStateRetrieval");
		
		Tuple2<AnalyticsContext, BatchEnrichmentContext> context_pair = getContextPair();
		final BatchEnrichmentContext test_context = context_pair._2();
		
		final DataBucketBean bucket = BeanTemplateUtils.build(DataBucketBean.class).with("full_name", "TEST_HARVEST_CONTEXT").done().get();

		final SharedLibraryBean lib_bean = BeanTemplateUtils.build(SharedLibraryBean.class).with("path_name", "TEST_HARVEST_CONTEXT").with("_id", "_ID_TEST_HARVEST_CONTEXT").done().get();
		context_pair._1().resetLibraryConfigs(							
				ImmutableMap.<String, SharedLibraryBean>builder()
					.put(lib_bean.path_name(), lib_bean)
					.put(lib_bean._id(), lib_bean)
					.build());
		//(note deliberately don't set tech config library here to double check it accesses the right one...)
		context_pair._2().setBucket(bucket);
		
		assertFalse("No module library store avaiable yet", test_context.getGlobalEnrichmentModuleObjectStore(TestBean.class, Optional.of("test")).isPresent());		
		context_pair._2().setModule(lib_bean);
		
		ICrudService<AssetStateDirectoryBean> dir_a = _core_management_db.getStateDirectory(Optional.empty(), Optional.of(AssetStateDirectoryBean.StateDirectoryType.analytic_thread));
		ICrudService<AssetStateDirectoryBean> dir_e = _core_management_db.getStateDirectory(Optional.empty(), Optional.of(AssetStateDirectoryBean.StateDirectoryType.enrichment));
		ICrudService<AssetStateDirectoryBean> dir_h = _core_management_db.getStateDirectory(Optional.empty(), Optional.of(AssetStateDirectoryBean.StateDirectoryType.harvest));
		ICrudService<AssetStateDirectoryBean> dir_s = _core_management_db.getStateDirectory(Optional.empty(), Optional.of(AssetStateDirectoryBean.StateDirectoryType.library));

		dir_a.deleteDatastore().get();
		dir_e.deleteDatastore().get();
		dir_h.deleteDatastore().get();
		dir_s.deleteDatastore().get();
		assertEquals(0, dir_a.countObjects().get().intValue());
		assertEquals(0, dir_e.countObjects().get().intValue());
		assertEquals(0, dir_h.countObjects().get().intValue());
		assertEquals(0, dir_s.countObjects().get().intValue());
		
		@SuppressWarnings("unused")
		ICrudService<TestBean> s1 = test_context.getGlobalEnrichmentModuleObjectStore(TestBean.class, Optional.of("test")).get();
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
		
		context_pair._1().setBucket(bucket);
		
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

		ICrudService<AssetStateDirectoryBean> dir_e_2 = test_context.getBucketObjectStore(AssetStateDirectoryBean.class, Optional.empty(), Optional.empty(), Optional.empty());
		assertEquals(1, dir_e_2.countObjects().get().intValue());
		
		@SuppressWarnings("unused")
		ICrudService<TestBean> h2 = test_context.getBucketObjectStore(TestBean.class, Optional.empty(), Optional.of("test_2"), Optional.empty());
		assertEquals(1, dir_a.countObjects().get().intValue());
		assertEquals(2, dir_e.countObjects().get().intValue());
		assertEquals(1, dir_h.countObjects().get().intValue());
		assertEquals(1, dir_s.countObjects().get().intValue());		
		assertEquals(2, dir_e_2.countObjects().get().intValue());
		
		try {
			test_context.getBucketObjectStore(TestBean.class, Optional.empty(), Optional.of("test_2"), Optional.of(AssetStateDirectoryBean.StateDirectoryType.library));
			fail("Should have thrown");
		}
		catch (Exception e) {
		}
	}
}
