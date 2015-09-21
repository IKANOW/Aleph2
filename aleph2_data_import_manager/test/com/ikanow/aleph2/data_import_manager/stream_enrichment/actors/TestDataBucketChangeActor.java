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
package com.ikanow.aleph2.data_import_manager.stream_enrichment.actors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import scala.Tuple2;
import scala.concurrent.duration.Duration;
import akka.actor.ActorRef;
import akka.actor.Inbox;
import akka.actor.Props;
import akka.pattern.Patterns;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.analytics.storm.services.MockStormAnalyticTechnologyService;
import com.ikanow.aleph2.data_import_manager.stream_enrichment.actors.DataBucketChangeActor;
import com.ikanow.aleph2.data_import_manager.services.DataImportActorContext;
import com.ikanow.aleph2.data_import_manager.services.GeneralInformationService;
import com.ikanow.aleph2.data_import_manager.utils.LibraryCacheUtils;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.distributed_services.utils.AkkaFutureUtils;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage.BucketActionEventBusWrapper;
import com.ikanow.aleph2.management_db.services.ManagementDbActorContext;
import com.ikanow.aleph2.management_db.utils.ActorUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import fj.data.Validation;

public class TestDataBucketChangeActor {
	protected static final Logger _logger = LogManager.getLogger();	

	////////////////////////////////////////////////////////////////////////////////////
	
	// TEST ENVIRONMENT	
	
	@Inject 
	protected IServiceContext _service_context = null;
	
	protected DataImportActorContext _actor_context;
	protected ManagementDbActorContext _db_actor_context;
	
	@SuppressWarnings("deprecation")
	@Before
	public void setupDependencies() throws Exception {
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
		
		_db_actor_context = new ManagementDbActorContext(_service_context, true);				
		
		_actor_context = new DataImportActorContext(_service_context, new GeneralInformationService()); 
		app_injector.injectMembers(_actor_context);
		
		// Have to do this in order for the underlying management db to live...		
		_service_context.getCoreManagementDbService();
	}
	
	@Test
	public void test_setup() {
		final String temp_dir = System.getProperty("java.io.tmpdir") + File.separator;
		
		assertTrue("setup completed - service context", _service_context != null);
		assertTrue("setup completed - services", _service_context.getCoreManagementDbService() != null);
		assertEquals(temp_dir, _service_context.getGlobalProperties().local_root_dir());
		
		if (File.separator.equals("\\")) { // windows mode!
			assertTrue("WINDOWS MODE: hadoop home needs to be set (use -Dhadoop.home.dir={HADOOP_HOME} in JAVA_OPTS)", null != System.getProperty("hadoop.home.dir"));
			assertTrue("WINDOWS MODE: hadoop home needs to exist: " + System.getProperty("hadoop.home.dir"), null != System.getProperty("hadoop.home.dir"));
		}
	}
	
	////////////////////////////////////////////////////////////////////////////////////
	
	// ACTOR TESTING	
	
	@Test
	public void test_actor_streamEnrichment() throws IllegalArgumentException, InterruptedException, ExecutionException, TimeoutException, IOException {		
		
		// Create a bucket
		
		final EnrichmentControlMetadataBean enrichment_module = new EnrichmentControlMetadataBean(
				"test_name", Collections.emptyList(), true, null, Arrays.asList("test_tech_id_stream", "test_module_id"), null, new LinkedHashMap<>());
		
		final DataBucketBean bucket = // (don't convert to actor, this is going via the message bus, ie "raw") 				
						BeanTemplateUtils.clone(createBucket("test_actor"))
								.with(DataBucketBean::analytic_thread, null)
								.with(DataBucketBean::streaming_enrichment_topology, enrichment_module)
						.done()
						;
		
		// Create an actor:
		
		final ActorRef handler = _db_actor_context.getActorSystem().actorOf(Props.create(DataBucketChangeActor.class), "test_host");
		_db_actor_context.getStreamingEnrichmentMessageBus().subscribe(handler, ActorUtils.STREAMING_ENRICHMENT_EVENT_BUS);

		// create the inbox:
		final Inbox inbox = Inbox.create(_actor_context.getActorSystem());		
		
		// 1) A message that it will ignore because it's not for this actor
		{
			final BucketActionMessage.DeleteBucketActionMessage delete =
					new BucketActionMessage.DeleteBucketActionMessage(bucket, new HashSet<String>(Arrays.asList("a", "b")));
			
			inbox.send(handler, delete);
			try {
				inbox.receive(Duration.create(1L, TimeUnit.SECONDS));
				fail("should have timed out");
			}
			catch (Exception e) {
				assertEquals(TimeoutException.class, e.getClass());
			}
		}
		
		// 2a) Send an offer (ignored, no storm properties)
		
		//(bypass this - this logic is now managed by the storm analytic services)
		{
			try {
				new File(_service_context.getGlobalProperties().local_yarn_config_dir() + File.separator + MockStormAnalyticTechnologyService.DISABLE_FILE).createNewFile();
			}
			catch (Exception e) {
				//(don't care if fails, probably just first time through)
			}
			
			final BucketActionMessage.BucketActionOfferMessage broadcast =
					new BucketActionMessage.BucketActionOfferMessage(bucket);
			
			_db_actor_context.getStreamingEnrichmentMessageBus().publish(new BucketActionEventBusWrapper(inbox.getRef(), broadcast));
			
			final Object msg = inbox.receive(Duration.create(5L, TimeUnit.SECONDS));
		
			assertEquals(BucketActionReplyMessage.BucketActionIgnoredMessage.class, msg.getClass());
		}
		
		// 2b) Send an offer (accepted, create file)
		{
			try {
				new File(_service_context.getGlobalProperties().local_yarn_config_dir() + File.separator + MockStormAnalyticTechnologyService.DISABLE_FILE).delete();
			}
			catch (Exception e) {
				//(don't care if fails, probably just first time through)
			}
			
			final BucketActionMessage.BucketActionOfferMessage broadcast =
					new BucketActionMessage.BucketActionOfferMessage(bucket);
			
			assertTrue(DataBucketChangeActor.isEnrichmentRequest(broadcast));			
			
			_db_actor_context.getStreamingEnrichmentMessageBus().publish(new BucketActionEventBusWrapper(inbox.getRef(), broadcast));
			
			final Object msg = inbox.receive(Duration.create(5L, TimeUnit.SECONDS));
		
			assertEquals(BucketActionReplyMessage.BucketActionWillAcceptMessage.class, msg.getClass());
		}
		
		// 3) Send a message, currently just check it arrives back
		{
			final BucketActionMessage.UpdateBucketActionMessage update =
					new BucketActionMessage.UpdateBucketActionMessage(bucket, false, bucket, new HashSet<String>(Arrays.asList(_actor_context.getInformationService().getHostname())));
			
			final CompletableFuture<BucketActionReplyMessage> reply4 = AkkaFutureUtils.efficientWrap(Patterns.ask(handler, update, 5000L), _db_actor_context.getActorSystem().dispatcher());
			@SuppressWarnings("unused")
			final BucketActionReplyMessage msg4 = reply4.get();
		}		
	}	

	@Test
	public void test_cacheJars_streamEnrichment() throws UnsupportedFileSystemException, InterruptedException, ExecutionException {
		try {
			// Preamble:
			// 0) Insert 2 library beans into the management db
			
			final DataBucketBean bucket = DataBucketChangeActor.convertStreamingEnrichmentToAnalyticBucket(createBucket("test_tech_id_stream"));		
			
			final String pathname1 = System.getProperty("user.dir") + "/misc_test_assets/simple-harvest-example.jar";
			final Path path1 = FileContext.getLocalFSFileContext().makeQualified(new Path(pathname1));		
			final String pathname2 = System.getProperty("user.dir") + "/misc_test_assets/simple-harvest-example2.jar";
			final Path path2 = FileContext.getLocalFSFileContext().makeQualified(new Path(pathname2));		
			
			List<SharedLibraryBean> lib_elements = createSharedLibraryBeans(path1, path2);
	
			final IManagementDbService underlying_db = _service_context.getService(IManagementDbService.class, Optional.empty()).get();			
			final IManagementCrudService<SharedLibraryBean> library_crud = underlying_db.getSharedLibraryStore();
			library_crud.deleteDatastore();
			assertEquals("Cleansed library store", 0L, (long)library_crud.countObjects().get());
			library_crud.storeObjects(lib_elements).get();
			
			assertEquals("Should have 4 library beans", 4L, (long)library_crud.countObjects().get());
			
			// 0a) Check with no streaming, gets nothing
			{			
				CompletableFuture<Validation<BasicMessageBean, Map<String, Tuple2<SharedLibraryBean, String>>>> reply_structure =
						LibraryCacheUtils.cacheJars(bucket, DataBucketChangeActor.getQuery(bucket, false),
								_service_context.getCoreManagementDbService(), _service_context.getGlobalProperties(), _service_context.getStorageService(), _service_context,
								"test1_source", "test1_command"
							);
				
				if (reply_structure.get().isFail()) {
					fail("About to crash with: " + reply_structure.get().fail().message());
				}		
				assertTrue("cacheJars should return valid reply", reply_structure.get().isSuccess());
			
				final Map<String, Tuple2<SharedLibraryBean, String>> reply_map = reply_structure.get().success();
				
				assertEquals(0L, reply_map.size()); // (both modules, 1x for _id and 1x for name) 
			}
			
			// 0b) Create the more complex bucket
			
			final EnrichmentControlMetadataBean enrichment_module = new EnrichmentControlMetadataBean(
					"test_name", Collections.emptyList(), true, null, Arrays.asList("test_tech_id_stream", "test_module_id"), null, new LinkedHashMap<>());
			
			final DataBucketBean bucket2 =
					DataBucketChangeActor.convertStreamingEnrichmentToAnalyticBucket(
						BeanTemplateUtils.clone(bucket)
									.with(DataBucketBean::analytic_thread, null)
									.with(DataBucketBean::streaming_enrichment_topology, enrichment_module)
									.done());
			
			// 1) Normal operation
			
			CompletableFuture<Validation<BasicMessageBean, Map<String, Tuple2<SharedLibraryBean, String>>>> reply_structure =
					LibraryCacheUtils.cacheJars(bucket2, DataBucketChangeActor.getQuery(bucket2, false),
						_service_context.getCoreManagementDbService(), _service_context.getGlobalProperties(), _service_context.getStorageService(), _service_context,
						"test1_source", "test1_command"
					);
			
			if (reply_structure.get().isFail()) {
				fail("About to crash with: " + reply_structure.get().fail().message());
			}		
			assertTrue("cacheJars should return valid reply", reply_structure.get().isSuccess());
		
			final Map<String, Tuple2<SharedLibraryBean, String>> reply_map = reply_structure.get().success();
			
			assertEquals("Should have 4 beans: " + reply_map.toString(), 4L, reply_map.size()); // (both modules, 1x for _id and 1x for name) 
			
			// 3) Couple of error cases:
			
			final EnrichmentControlMetadataBean enrichment_module2 = new EnrichmentControlMetadataBean(
					"test_name", Collections.emptyList(), true, null, Arrays.asList("test_tech_id_stream", "test_module_id", "failtest"), null, new LinkedHashMap<>());
			
			final DataBucketBean bucket3 = 
					DataBucketChangeActor.convertStreamingEnrichmentToAnalyticBucket(
						BeanTemplateUtils.clone(bucket)
									.with(DataBucketBean::analytic_thread, null)
									.with(DataBucketBean::streaming_enrichment_topology, enrichment_module2)
									.done());
			
			CompletableFuture<Validation<BasicMessageBean, Map<String, Tuple2<SharedLibraryBean, String>>>> reply_structure3 =
					LibraryCacheUtils.cacheJars(bucket3, DataBucketChangeActor.getQuery(bucket3, false),
							_service_context.getCoreManagementDbService(), _service_context.getGlobalProperties(), _service_context.getStorageService(), _service_context,
							"test2_source", "test2_command"
						);
			
			assertTrue("cacheJars should return error", reply_structure3.get().isFail());
		}
		catch (Exception e) {
			System.out.println(ErrorUtils.getLongForm("guice? {0}", e));
			throw e;
		}
	}
	
	//TODO: test code for the enrichment -> analytics conversion code
	
	//TODO: are these now tested from somewhere else? or do they need to get duplicated in the storm analytic services
	
//	@Test
//	public void test_getStreamingTopology() throws UnsupportedFileSystemException, InterruptedException, ExecutionException {
//		final DataBucketBean bucket = createBucket("test_tech_id_stream");		
//		
//		final String pathname1 = System.getProperty("user.dir") + "/misc_test_assets/simple-topology-example.jar";
//		final Path path1 = FileContext.getLocalFSFileContext().makeQualified(new Path(pathname1));		
//		final String pathname2 = System.getProperty("user.dir") + "/misc_test_assets/simple-harvest-example2.jar";
//		final Path path2 = FileContext.getLocalFSFileContext().makeQualified(new Path(pathname2));		
//		
//		List<SharedLibraryBean> lib_elements = createSharedLibraryBeans(path1, path2);
//		
//		//////////////////////////////////////////////////////
//
//		// 1) Check - if called with an error, then just passes that error along
//		
//		final BasicMessageBean error = SharedErrorUtils.buildErrorMessage("test_source", "test_message", "test_error");
//		
//		final Validation<BasicMessageBean, IEnrichmentStreamingTopology> test1 = DataBucketChangeActor.getStreamingTopology(bucket,  
//				new BucketActionMessage.BucketActionOfferMessage(bucket), "test_source2", Validation.fail(error));
//		
//		assertTrue("Got error back", test1.isFail());
//		assertEquals("test_source", test1.fail().source());
//		assertEquals("test_message", test1.fail().command());
//		assertEquals("test_error", test1.fail().message());
//		
//		//////////////////////////////////////////////////////
//
//		// 2) Check the error handling inside getStreamingTopology
//		
//		final ImmutableMap<String, Tuple2<SharedLibraryBean, String>> test2_input = 
//				ImmutableMap.<String, Tuple2<SharedLibraryBean, String>>builder()
//					.put("test_tech_id_stream_2b", Tuples._2T(null, null))
//					.build();
//
//		final Validation<BasicMessageBean, IEnrichmentStreamingTopology> test2a = DataBucketChangeActor.getStreamingTopology(
//				BeanTemplateUtils.clone(bucket).with(DataBucketBean::streaming_enrichment_topology,
//						BeanTemplateUtils.build(EnrichmentControlMetadataBean.class).with(EnrichmentControlMetadataBean::library_names_or_ids, Arrays.asList("test_tech_id_stream_2a"))
//						.done().get()).done(), 
//				new BucketActionMessage.BucketActionOfferMessage(bucket), "test_source2a", 
//				Validation.success(test2_input));
//
//		assertTrue("Got error back", test2a.isFail());
//		assertEquals("test_source2a", test2a.fail().source());
//		assertEquals("BucketActionOfferMessage", test2a.fail().command());
//		assertEquals(ErrorUtils.get(SharedErrorUtils.SHARED_LIBRARY_NAME_NOT_FOUND, bucket.full_name(), "(unknown)"), // (cloned bucket above)
//						test2a.fail().message());
//		
//		final Validation<BasicMessageBean, IEnrichmentStreamingTopology> test2b = DataBucketChangeActor.getStreamingTopology(
//				BeanTemplateUtils.clone(bucket).with(DataBucketBean::streaming_enrichment_topology,
//						BeanTemplateUtils.build(EnrichmentControlMetadataBean.class).with(EnrichmentControlMetadataBean::library_names_or_ids, Arrays.asList("test_tech_id_stream_2b"))
//						.done().get()).done(), 
//				new BucketActionMessage.BucketActionOfferMessage(bucket), "test_source2b", 
//				Validation.success(test2_input));
//
//		assertTrue("Got error back", test2b.isFail());
//		assertEquals("test_source2b", test2b.fail().source());
//		assertEquals("BucketActionOfferMessage", test2b.fail().command());
//		assertEquals(ErrorUtils.get(SharedErrorUtils.SHARED_LIBRARY_NAME_NOT_FOUND, bucket.full_name(), "(unknown)"), // (cloned bucket above)
//						test2a.fail().message());
//		
//		//////////////////////////////////////////////////////
//
//		// 3) OK now it will actually do something 
//		
//		final String java_name = _service_context.getGlobalProperties().local_cached_jar_dir() + File.separator + "test_tech_id_stream.cache.jar";
//		
//		_logger.info("Needed to delete locally cached file? " + java_name + ": " + new File(java_name).delete());		
//		
//		// Requires that the file has already been cached:
//		final Validation<BasicMessageBean, String> cached_file = JarCacheUtils.getCachedJar(_service_context.getGlobalProperties().local_cached_jar_dir(), 
//				lib_elements.get(0), 
//				_service_context.getStorageService(),
//				"test3", "test3").get();
//		
//		if (cached_file.isFail()) {
//			fail("About to crash with: " + cached_file.fail().message());
//		}		
//		
//		assertTrue("The cached file exists: " + java_name, new File(java_name).exists());
//		
//		// OK the setup is done and validated now actually test the underlying call:
//		
//		final ImmutableMap<String, Tuple2<SharedLibraryBean, String>> test3_input = 
//				ImmutableMap.<String, Tuple2<SharedLibraryBean, String>>builder()
//					.put("test_tech_id_stream", Tuples._2T(
//							lib_elements.get(0),
//							cached_file.success()))
//					.build();		
//		
//		final Validation<BasicMessageBean, IEnrichmentStreamingTopology> test3 = DataBucketChangeActor.getStreamingTopology(
//				BeanTemplateUtils.clone(bucket).with(DataBucketBean::streaming_enrichment_topology,
//						BeanTemplateUtils.build(EnrichmentControlMetadataBean.class).with(EnrichmentControlMetadataBean::library_names_or_ids, Arrays.asList("test_tech_id_stream"))
//						.done().get()).done(), 
//				new BucketActionMessage.BucketActionOfferMessage(bucket), "test_source3", 
//				Validation.success(test3_input));
//
//		if (test3.isFail()) {
//			fail("About to crash with: " + test3.fail().message());
//		}		
//		assertTrue("getStreamingTopology call succeeded", test3.isSuccess());
//		assertTrue("topology created: ", test3.success() != null);
//		assertEquals(lib_elements.get(0).misc_entry_point(), test3.success().getClass().getName());
//		
//		// (Try again but with failing version, due to class not found)
//
//		final ImmutableMap<String, Tuple2<SharedLibraryBean, String>> test3a_input = 
//				ImmutableMap.<String, Tuple2<SharedLibraryBean, String>>builder()
//					.put("test_tech_id_stream_fail", Tuples._2T(
//							lib_elements.get(3),
//							cached_file.success()))
//					.build();		
//		
//		final Validation<BasicMessageBean, IEnrichmentStreamingTopology> test3a = DataBucketChangeActor.getStreamingTopology(
//				BeanTemplateUtils.clone(bucket).with(DataBucketBean::streaming_enrichment_topology,
//						BeanTemplateUtils.build(EnrichmentControlMetadataBean.class).with(EnrichmentControlMetadataBean::library_names_or_ids, Arrays.asList("test_tech_id_stream_fail"))
//						.done().get()).done(), 
//				new BucketActionMessage.BucketActionOfferMessage(bucket), "test_source3", 
//				Validation.success(test3a_input));
//
//		assertTrue("Got error back", test3a.isFail());	
//		assertTrue("Right error: " + test3a.fail().message(), test3a.fail().message().contains("com.ikanow.aleph2.test.example.ExampleStreamTopology"));
//		
//		// Now check with the "not just the harvest tech" flag set
//		
//		final String java_name2 = _service_context.getGlobalProperties().local_cached_jar_dir() + File.separator + "test_module_id.cache.jar";
//		
//		_logger.info("Needed to delete locally cached file? " + java_name2 + ": " + new File(java_name2).delete());		
//		
//		// Requires that the file has already been cached:
//		final Validation<BasicMessageBean, String> cached_file2 = JarCacheUtils.getCachedJar(_service_context.getGlobalProperties().local_cached_jar_dir(), 
//				lib_elements.get(1), 
//				_service_context.getStorageService(),
//				"test3b", "test3b").get();
//		
//		if (cached_file2.isFail()) {
//			fail("About to crash with: " + cached_file2.fail().message());
//		}		
//		
//		assertTrue("The cached file exists: " + java_name, new File(java_name2).exists());				
//		
//		final ImmutableMap<String, Tuple2<SharedLibraryBean, String>> test3b_input = 
//				ImmutableMap.<String, Tuple2<SharedLibraryBean, String>>builder()
//					.put("test_tech_id_stream", Tuples._2T(
//							lib_elements.get(0),
//							cached_file.success()))
//					.put("test_module_id", Tuples._2T(
//							lib_elements.get(1),
//							cached_file.success()))
//					.build();		
//		
//		final EnrichmentControlMetadataBean enrichment_module = new EnrichmentControlMetadataBean(
//				"test_tech_name", Collections.emptyList(), true, null, Arrays.asList("test_tech_id_stream", "test_module_id"), null, null
//				);
//		
//		final Validation<BasicMessageBean, IEnrichmentStreamingTopology> test3b = DataBucketChangeActor.getStreamingTopology(
//				BeanTemplateUtils.clone(bucket)
//					.with(DataBucketBean::streaming_enrichment_topology, enrichment_module)
//					.done(), 
//				new BucketActionMessage.BucketActionOfferMessage(bucket), "test_source3b", 
//				Validation.success(test3b_input));
//
//		if (test3b.isFail()) {
//			fail("About to crash with: " + test3b.fail().message());
//		}		
//		assertTrue("getStreamingTopology call succeeded", test3b.isSuccess());
//		assertTrue("topology created: ", test3b.success() != null);
//		assertEquals(lib_elements.get(0).misc_entry_point(), test3b.success().getClass().getName());	
//		
//		//TODO add a test for disabled streaming but config given (should default to passthrough top and
//		//ignore given topology
//	}
	
	////////////////////////////////////////////////////////////////////////////////////
	
	// UTILS
	
	protected DataBucketBean createBucket(final String harvest_tech_id) {
		// (Add streaming logic outside this via clone() - see cacheJars)
		return BeanTemplateUtils.build(DataBucketBean.class)
							.with(DataBucketBean::_id, "test1")
							.with(DataBucketBean::owner_id, "person_id")
							.with(DataBucketBean::full_name, "/test/path/")
							.with(DataBucketBean::analytic_thread,
									BeanTemplateUtils.build(AnalyticThreadBean.class)
										.with(AnalyticThreadBean::jobs,
												Arrays.asList(
														BeanTemplateUtils.build(AnalyticThreadJobBean.class)
															.with(AnalyticThreadJobBean::analytic_technology_name_or_id, "/test/tech")
														.done().get()
														)
												)
									.done().get()
									)
							.with(DataBucketBean::harvest_technology_name_or_id, harvest_tech_id)
							.done().get();
	}
	
	protected List<SharedLibraryBean> createSharedLibraryBeans(Path path1, Path path2) {
		final SharedLibraryBean lib_element = BeanTemplateUtils.build(SharedLibraryBean.class)
			.with(SharedLibraryBean::_id, "test_tech_id_stream")
			.with(SharedLibraryBean::path_name, path1.toString())
			.with(SharedLibraryBean::misc_entry_point, "com.ikanow.aleph2.data_import.stream_enrichment.storm.PassthroughTopology")
			.done().get();

		final SharedLibraryBean lib_element2 = BeanTemplateUtils.build(SharedLibraryBean.class)
			.with(SharedLibraryBean::_id, "test_module_id")
			.with(SharedLibraryBean::path_name, path2.toString())
			.done().get();

		final SharedLibraryBean lib_element3 = BeanTemplateUtils.build(SharedLibraryBean.class)
			.with(SharedLibraryBean::_id, "failtest")
			.with(SharedLibraryBean::path_name, "/not_exist/here.fghgjhgjhg")
			.done().get();

		// (result in classloader error)
		final SharedLibraryBean lib_element4 = BeanTemplateUtils.build(SharedLibraryBean.class)
				.with(SharedLibraryBean::_id, "test_tech_id_stream_fail")
				.with(SharedLibraryBean::path_name, path1.toString())
				.with(SharedLibraryBean::streaming_enrichment_entry_point, "com.ikanow.aleph2.test.example.ExampleStreamTopology")
				.done().get();
		
		return Arrays.asList(lib_element, lib_element2, lib_element3, lib_element4);
	}
}
