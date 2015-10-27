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
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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
import akka.actor.ActorSelection;
import akka.actor.Inbox;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.pattern.Patterns;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.analytics.hadoop.services.MockHadoopTechnologyService;
import com.ikanow.aleph2.analytics.services.AnalyticsContext;
import com.ikanow.aleph2.analytics.storm.services.MockStormAnalyticTechnologyService;
import com.ikanow.aleph2.core.shared.utils.ClassloaderUtils;
import com.ikanow.aleph2.core.shared.utils.JarCacheUtils;
import com.ikanow.aleph2.core.shared.utils.SharedErrorUtils;
import com.ikanow.aleph2.data_import_manager.analytics.actors.DataBucketAnalyticsChangeActor;
import com.ikanow.aleph2.data_import_manager.services.DataImportActorContext;
import com.ikanow.aleph2.data_import_manager.services.GeneralInformationService;
import com.ikanow.aleph2.data_import_manager.utils.LibraryCacheUtils;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsTechnologyModule;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsTechnologyService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean.MasterEnrichmentType;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.ProcessingTestSpecBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.distributed_services.utils.AkkaFutureUtils;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage.BucketActionAnalyticJobMessage.JobMessageType;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage.BucketActionEventBusWrapper;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage.PurgeBucketActionMessage;
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
		
		_actor_context = new DataImportActorContext(_service_context, new GeneralInformationService(), null, null); 
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
				"test_name", Collections.emptyList(), Collections.emptyList(), true, null, Arrays.asList("test_tech_id_stream", "test_module_id"), null, new LinkedHashMap<>(), new LinkedHashMap<>());
		
		final DataBucketBean bucket = // (don't convert to actor, this is going via the message bus, ie "raw") 				
						BeanTemplateUtils.clone(createBucket("test_actor"))
								.with(DataBucketBean::analytic_thread, null)
								.with(DataBucketBean::streaming_enrichment_topology, enrichment_module)
								.with(DataBucketBean::master_enrichment_type, MasterEnrichmentType.streaming)
						.done()
						;
		
		// Create an actor:
		
		final ActorRef handler = _db_actor_context.getActorSystem().actorOf(Props.create(DataBucketAnalyticsChangeActor.class), "test_host");
		_db_actor_context.getAnalyticsMessageBus().subscribe(handler, ActorUtils.BUCKET_ANALYTICS_EVENT_BUS);

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
			
			_db_actor_context.getAnalyticsMessageBus().publish(new BucketActionEventBusWrapper(inbox.getRef(), broadcast));
			
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
			
			assertTrue(DataBucketAnalyticsChangeActor.isEnrichmentRequest(broadcast));			
			
			_db_actor_context.getAnalyticsMessageBus().publish(new BucketActionEventBusWrapper(inbox.getRef(), broadcast));
			
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
	public void test_actor_batchEnrichment() throws IllegalArgumentException, InterruptedException, ExecutionException, TimeoutException, IOException {		
		
		// Create a bucket
		
		final EnrichmentControlMetadataBean enrichment_module = new EnrichmentControlMetadataBean(
				"test_name", Collections.emptyList(), null, true, null, Arrays.asList("test_tech_id_batch", "test_module_id"), null, new LinkedHashMap<>(), null);
		
		final DataBucketBean bucket = // (don't convert to actor, this is going via the message bus, ie "raw") 				
						BeanTemplateUtils.clone(createBucket("test_actor"))
								.with(DataBucketBean::analytic_thread, null)
								.with(DataBucketBean::batch_enrichment_configs, Arrays.asList(enrichment_module))
								.with(DataBucketBean::master_enrichment_type, MasterEnrichmentType.batch)
						.done()
						;
		
		// Create an actor:
		
		final ActorRef handler = _db_actor_context.getActorSystem().actorOf(Props.create(DataBucketAnalyticsChangeActor.class), "test_host");
		_db_actor_context.getAnalyticsMessageBus().subscribe(handler, ActorUtils.BUCKET_ANALYTICS_EVENT_BUS);

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
				new File(_service_context.getGlobalProperties().local_yarn_config_dir() + File.separator + MockHadoopTechnologyService.DISABLE_FILE).createNewFile();
			}
			catch (Exception e) {
				//(don't care if fails, probably just first time through)
			}
			
			final BucketActionMessage.BucketActionOfferMessage broadcast =
					new BucketActionMessage.BucketActionOfferMessage(bucket);
			
			_db_actor_context.getAnalyticsMessageBus().publish(new BucketActionEventBusWrapper(inbox.getRef(), broadcast));
			
			final Object msg = inbox.receive(Duration.create(5L, TimeUnit.SECONDS));
		
			assertEquals(BucketActionReplyMessage.BucketActionIgnoredMessage.class, msg.getClass());
		}
		
		// 2b) Send an offer (accepted, create file)
		{
			try {
				new File(_service_context.getGlobalProperties().local_yarn_config_dir() + File.separator + MockHadoopTechnologyService.DISABLE_FILE).delete();
			}
			catch (Exception e) {
				//(don't care if fails, probably just first time through)
			}
			
			final BucketActionMessage.BucketActionOfferMessage broadcast =
					new BucketActionMessage.BucketActionOfferMessage(bucket);
			
			assertTrue(DataBucketAnalyticsChangeActor.isEnrichmentRequest(broadcast));			
			
			_db_actor_context.getAnalyticsMessageBus().publish(new BucketActionEventBusWrapper(inbox.getRef(), broadcast));
			
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
	public void test_send_dummyUpdateMessage() throws InterruptedException, ExecutionException {
		
		// Create a bucket
		
		final AnalyticThreadJobBean job = BeanTemplateUtils.build(AnalyticThreadJobBean.class)
												.with(AnalyticThreadJobBean::analytic_technology_name_or_id, "StreamingEnrichmentService")
												.with(AnalyticThreadJobBean::name, "test")
												.with(AnalyticThreadJobBean::config, new LinkedHashMap<String, Object>())
											.done().get();
		
		final AnalyticThreadBean thread1 = BeanTemplateUtils.build(AnalyticThreadBean.class)
												.with(AnalyticThreadBean::jobs, Arrays.asList(job))
											.done().get();
		
		final AnalyticThreadBean thread2 = BeanTemplateUtils.build(AnalyticThreadBean.class)
												.with(AnalyticThreadBean::jobs, Arrays.asList())
											.done().get();
		
		final DataBucketBean old_bucket = // (don't convert to actor, this is going via the message bus, ie "raw") 				
						BeanTemplateUtils.clone(createBucket("test_actor"))
								.with(DataBucketBean::analytic_thread, thread1)
						.done()
						;
		
		final DataBucketBean new_bucket = BeanTemplateUtils.clone(old_bucket)
												.with(DataBucketBean::analytic_thread, thread2)
											.done()
											;
		
		// Create an actor:
		
		final ActorRef handler = _db_actor_context.getActorSystem().actorOf(Props.create(DataBucketAnalyticsChangeActor.class), "test_host");
		_db_actor_context.getAnalyticsMessageBus().subscribe(handler, ActorUtils.BUCKET_ANALYTICS_EVENT_BUS);

		// 1) A message that will be converted
		{
			final BucketActionMessage.UpdateBucketActionMessage update =
					new BucketActionMessage.UpdateBucketActionMessage(new_bucket, true, old_bucket, new HashSet<String>(Arrays.asList(_actor_context.getInformationService().getHostname())));
			
			final CompletableFuture<BucketActionReplyMessage> reply4 = AkkaFutureUtils.efficientWrap(Patterns.ask(handler, update, 5000L), _db_actor_context.getActorSystem().dispatcher());
			final BucketActionReplyMessage msg4 = reply4.get();
			
			assertTrue("Job should be the right type: " + msg4.getClass(), msg4 instanceof BucketActionReplyMessage.BucketActionCollectedRepliesMessage);
			assertTrue("Job should reply once", ((BucketActionReplyMessage.BucketActionCollectedRepliesMessage)msg4).replies().size()==1);
			assertTrue("Job should work", ((BucketActionReplyMessage.BucketActionCollectedRepliesMessage)msg4).replies().get(0).success());
			assertEquals("Job should be a stop", "stopJob", ((BucketActionReplyMessage.BucketActionCollectedRepliesMessage)msg4).replies().get(0).command());
		}
	}	

	@Test
	public void test_getAnalyticsTechnology_specialCases() throws UnsupportedFileSystemException, InterruptedException, ExecutionException {
		// Just check the streaming enrichment error and success cases
		
		{
			final DataBucketBean bucket = createBucket(DataBucketAnalyticsChangeActor.STREAMING_ENRICHMENT_TECH_NAME); //(note this also sets the analytics name in the jobs)	
			
			final Validation<BasicMessageBean, Tuple2<IAnalyticsTechnologyModule, ClassLoader>> test1 = DataBucketAnalyticsChangeActor.getAnalyticsTechnology(
					bucket, DataBucketAnalyticsChangeActor.STREAMING_ENRICHMENT_TECH_NAME, 
					true, 
					Optional.empty(), Optional.empty(),
					new BucketActionMessage.BucketActionOfferMessage(bucket), "test1", 
					Validation.success(Collections.emptyMap()));
			
			assertTrue("Failed with no analytic technology", test1.isFail());
			
			final Validation<BasicMessageBean, Tuple2<IAnalyticsTechnologyModule, ClassLoader>> test2 = DataBucketAnalyticsChangeActor.getAnalyticsTechnology(
					bucket, DataBucketAnalyticsChangeActor.STREAMING_ENRICHMENT_TECH_NAME, 
					true, 
					_service_context.getService(IAnalyticsTechnologyService.class, DataBucketAnalyticsChangeActor.STREAMING_ENRICHMENT_DEFAULT)
						.map(s->(IAnalyticsTechnologyModule)s), Optional.empty(),
					new BucketActionMessage.BucketActionOfferMessage(bucket), "test2", 
					Validation.success(Collections.emptyMap()));
	
			assertTrue("Failed with no analytic technology: " + test2.validation(f -> f.message(),  s -> "(worked)"), test2.isSuccess());
		}
		
		
		{
			final DataBucketBean bucket = createBucket(DataBucketAnalyticsChangeActor.BATCH_ENRICHMENT_TECH_NAME); //(note this also sets the analytics name in the jobs)	
			
			final Validation<BasicMessageBean, Tuple2<IAnalyticsTechnologyModule, ClassLoader>> test1 = DataBucketAnalyticsChangeActor.getAnalyticsTechnology(
					bucket, DataBucketAnalyticsChangeActor.BATCH_ENRICHMENT_TECH_NAME, 
					true, 
					Optional.empty(), Optional.empty(),
					new BucketActionMessage.BucketActionOfferMessage(bucket), "test1", 
					Validation.success(Collections.emptyMap()));
			
			assertTrue("Failed with no analytic technology", test1.isFail());
			
			final Validation<BasicMessageBean, Tuple2<IAnalyticsTechnologyModule, ClassLoader>> test2 = DataBucketAnalyticsChangeActor.getAnalyticsTechnology(
					bucket, DataBucketAnalyticsChangeActor.BATCH_ENRICHMENT_TECH_NAME, 
					true, 
					Optional.empty(),
					_service_context.getService(IAnalyticsTechnologyService.class, DataBucketAnalyticsChangeActor.BATCH_ENRICHMENT_DEFAULT)
						.map(s->(IAnalyticsTechnologyModule)s), 
					new BucketActionMessage.BucketActionOfferMessage(bucket), "test2", 
					Validation.success(Collections.emptyMap()));
	
			assertTrue("Failed with no analytic technology: " + test2.validation(f -> f.message(),  s -> "(worked)"), test2.isSuccess());
		}
		
		// (Later will include the system classpath cases also)		
	}

	
	@Test
	public void test_getAnalyticsTechnology() throws UnsupportedFileSystemException, InterruptedException, ExecutionException {
		final DataBucketBean bucket = createBucket("test_tech_id_analytics"); //(note this also sets the analytics name in the jobs)	
		
		final String pathname1 = System.getProperty("user.dir") + "/misc_test_assets/simple-analytics-example.jar";
		final Path path1 = FileContext.getLocalFSFileContext().makeQualified(new Path(pathname1));		
		final String pathname2 = System.getProperty("user.dir") + "/misc_test_assets/simple-harvest-example2.jar";
		final Path path2 = FileContext.getLocalFSFileContext().makeQualified(new Path(pathname2));		
		
		List<SharedLibraryBean> lib_elements = createSharedLibraryBeans_analytics(path1, path2);
		
		//////////////////////////////////////////////////////

		// 1) Check - if called with an error, then just passes that error along
		
		final BasicMessageBean error = SharedErrorUtils.buildErrorMessage("test_source", "test_message", "test_error");
		
		final Validation<BasicMessageBean, Tuple2<IAnalyticsTechnologyModule, ClassLoader>> test1 = 
				DataBucketAnalyticsChangeActor.getAnalyticsTechnology(bucket, "test_tech_id_analytics", true, Optional.empty(), Optional.empty(),
						new BucketActionMessage.BucketActionOfferMessage(bucket), "test_source2", Validation.fail(error));
		
		assertTrue("Got error back", test1.isFail());
		assertEquals("test_source", test1.fail().source());
		assertEquals("test_message", test1.fail().command());
		assertEquals("test_error", test1.fail().message());
		
		//////////////////////////////////////////////////////

		// 2) Check the error handling inside getAnalyticsTechnology
		
		final ImmutableMap<String, Tuple2<SharedLibraryBean, String>> test2_input = 
				ImmutableMap.<String, Tuple2<SharedLibraryBean, String>>builder()
					.put("test_tech_id_analytics_2b", Tuples._2T(null, null))
					.build();

		final Validation<BasicMessageBean, Tuple2<IAnalyticsTechnologyModule, ClassLoader>> test2a = DataBucketAnalyticsChangeActor.getAnalyticsTechnology(
				createBucket("test_tech_id_analytics_2a"), "test_tech_id_analytics_2a", 
				true, Optional.empty(), Optional.empty(),
				new BucketActionMessage.BucketActionOfferMessage(bucket), "test_source2a", 
				Validation.success(test2_input));

		assertTrue("Got error back", test2a.isFail());
		assertEquals("test_source2a", test2a.fail().source());
		assertEquals("BucketActionOfferMessage", test2a.fail().command());
		assertEquals(ErrorUtils.get(SharedErrorUtils.SHARED_LIBRARY_NAME_NOT_FOUND, bucket.full_name(), "test_tech_id_analytics_2a"), // (cloned bucket above)
						test2a.fail().message());
		
		final Validation<BasicMessageBean, Tuple2<IAnalyticsTechnologyModule, ClassLoader>> test2b = DataBucketAnalyticsChangeActor.getAnalyticsTechnology(
				createBucket("test_tech_id_analytics_2b"), "test_tech_id_analytics_2b",
				true, Optional.empty(), Optional.empty(),
				new BucketActionMessage.BucketActionOfferMessage(bucket), "test_source2b", 
				Validation.success(test2_input));

		assertTrue("Got error back", test2b.isFail());
		assertEquals("test_source2b", test2b.fail().source());
		assertEquals("BucketActionOfferMessage", test2b.fail().command());
		assertEquals(ErrorUtils.get(SharedErrorUtils.SHARED_LIBRARY_NAME_NOT_FOUND, bucket.full_name(), "test_tech_id_analytics_2a"), // (cloned bucket above)
						test2a.fail().message());
		
		//////////////////////////////////////////////////////

		// 3) OK now it will actually do something 
		
		final String java_name = _service_context.getGlobalProperties().local_cached_jar_dir() + File.separator + "test_tech_id_analytics.cache.jar";
		
		System.out.println("Needed to delete locally cached file? " + java_name + ": " + new File(java_name).delete());		
		
		// Requires that the file has already been cached:
		final Validation<BasicMessageBean, String> cached_file = JarCacheUtils.getCachedJar(_service_context.getGlobalProperties().local_cached_jar_dir(), 
				lib_elements.get(0), 
				_service_context.getStorageService(),
				"test3", "test3").get();
		
		if (cached_file.isFail()) {
			fail("About to crash with: " + cached_file.fail().message());
		}		
		
		assertTrue("The cached file should exist: " + java_name, new File(java_name).exists());
		
		// OK the setup is done and validated now actually test the underlying call:
		
		final ImmutableMap<String, Tuple2<SharedLibraryBean, String>> test3_input = 
				ImmutableMap.<String, Tuple2<SharedLibraryBean, String>>builder()
					.put("test_tech_id_analytics", Tuples._2T(
							lib_elements.get(0),
							cached_file.success()))
					.build();		
		
		final Validation<BasicMessageBean, Tuple2<IAnalyticsTechnologyModule, ClassLoader>> test3 = DataBucketAnalyticsChangeActor.getAnalyticsTechnology(
				bucket, "test_tech_id_analytics",
				true, Optional.empty(), Optional.empty(),
				new BucketActionMessage.BucketActionOfferMessage(bucket), "test_source3", 
				Validation.success(test3_input));

		if (test3.isFail()) {
			fail("About to crash with: " + test3.fail().message());
		}		
		assertTrue("getAnalyticsTechnology call succeeded", test3.isSuccess());
		assertTrue("harvest tech created: ", test3.success() != null);
		assertEquals(lib_elements.get(0).misc_entry_point(), test3.success()._1().getClass().getName());
		
		// Now check with the "not just the harvest tech" flag set
		
		final String java_name2 = _service_context.getGlobalProperties().local_cached_jar_dir() + File.separator + "test_module_id.cache.jar";
		
		System.out.println("Needed to delete locally cached file? " + java_name2 + ": " + new File(java_name2).delete());		
		
		// Requires that the file has already been cached:
		final Validation<BasicMessageBean, String> cached_file2 = JarCacheUtils.getCachedJar(_service_context.getGlobalProperties().local_cached_jar_dir(), 
				lib_elements.get(1), 
				_service_context.getStorageService(),
				"test3b", "test3b").get();
		
		if (cached_file2.isFail()) {
			fail("About to crash with: " + cached_file2.fail().message());
		}		
		
		assertTrue("The cached file exists: " + java_name, new File(java_name2).exists());				
		
		final ImmutableMap<String, Tuple2<SharedLibraryBean, String>> test3b_input = 
				ImmutableMap.<String, Tuple2<SharedLibraryBean, String>>builder()
					.put("test_tech_id_analytics", Tuples._2T(
							lib_elements.get(0),
							cached_file.success()))
					.put("test_module_id", Tuples._2T(
							lib_elements.get(1),
							cached_file.success()))
					.build();		
		
		final Validation<BasicMessageBean, Tuple2<IAnalyticsTechnologyModule, ClassLoader>> test3b = DataBucketAnalyticsChangeActor.getAnalyticsTechnology(
				bucket, "test_tech_id_analytics",
				false, Optional.empty(), Optional.empty(),
				new BucketActionMessage.BucketActionOfferMessage(bucket), "test_source3b", 
				Validation.success(test3b_input));

		if (test3b.isFail()) {
			fail("About to crash with: " + test3b.fail().message());
		}		
		assertTrue("getAnalyticsTechnology call succeeded", test3b.isSuccess());
		assertTrue("harvest tech created: ", test3b.success() != null);
		assertEquals(lib_elements.get(0).misc_entry_point(), test3b.success()._1().getClass().getName());		
	}
	
	@Test
	public void test_cacheJars_streamEnrichment() throws UnsupportedFileSystemException, InterruptedException, ExecutionException {
		try {
			// Preamble:
			// 0) Insert 2 library beans into the management db
			
			final DataBucketBean bucket = DataBucketAnalyticsChangeActor.convertEnrichmentToAnalyticBucket(createBucket("test_tech_id_stream"));		
			
			final String pathname1 = System.getProperty("user.dir") + "/misc_test_assets/simple-harvest-example.jar";
			final Path path1 = FileContext.getLocalFSFileContext().makeQualified(new Path(pathname1));		
			final String pathname2 = System.getProperty("user.dir") + "/misc_test_assets/simple-harvest-example2.jar";
			final Path path2 = FileContext.getLocalFSFileContext().makeQualified(new Path(pathname2));		
			
			List<SharedLibraryBean> lib_elements = createSharedLibraryBeans_streaming(path1, path2);
	
			final IManagementDbService underlying_db = _service_context.getService(IManagementDbService.class, Optional.empty()).get();			
			final IManagementCrudService<SharedLibraryBean> library_crud = underlying_db.getSharedLibraryStore();
			library_crud.deleteDatastore();
			assertEquals("Cleansed library store", 0L, (long)library_crud.countObjects().get());
			library_crud.storeObjects(lib_elements).get();
			
			assertEquals("Should have 4 library beans", 4L, (long)library_crud.countObjects().get());
			
			// 0a) Check with no streaming, gets nothing
			{			
				final DataBucketBean bucket0 = DataBucketAnalyticsChangeActor.convertEnrichmentToAnalyticBucket(createBucket("broken"));		
				
				CompletableFuture<Validation<BasicMessageBean, Map<String, Tuple2<SharedLibraryBean, String>>>> reply_structure =
						LibraryCacheUtils.cacheJars(bucket0, DataBucketAnalyticsChangeActor.getQuery(bucket0, false),
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
					"test_name", Collections.emptyList(), null, true, null, Arrays.asList("test_tech_id_stream", "test_module_id"), null, new LinkedHashMap<>(), null);
			
			final DataBucketBean bucket2 =
					DataBucketAnalyticsChangeActor.convertEnrichmentToAnalyticBucket(
						BeanTemplateUtils.clone(bucket)
									.with(DataBucketBean::analytic_thread, null)
									.with(DataBucketBean::streaming_enrichment_topology, enrichment_module)
									.with(DataBucketBean::master_enrichment_type, MasterEnrichmentType.streaming)
									.done());
			
			// 1) Normal operation
			
			CompletableFuture<Validation<BasicMessageBean, Map<String, Tuple2<SharedLibraryBean, String>>>> reply_structure =
					LibraryCacheUtils.cacheJars(bucket2, DataBucketAnalyticsChangeActor.getQuery(bucket2, false),
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
					"test_name", Collections.emptyList(), null, true, null, Arrays.asList("test_tech_id_stream", "test_module_id", "failtest"), null, new LinkedHashMap<>(), new LinkedHashMap<>());
			
			final DataBucketBean bucket3 = 
					DataBucketAnalyticsChangeActor.convertEnrichmentToAnalyticBucket(
						BeanTemplateUtils.clone(bucket)
									.with(DataBucketBean::analytic_thread, null)
									.with(DataBucketBean::streaming_enrichment_topology, enrichment_module2)
									.with(DataBucketBean::master_enrichment_type, MasterEnrichmentType.streaming)
									.done());
			
			CompletableFuture<Validation<BasicMessageBean, Map<String, Tuple2<SharedLibraryBean, String>>>> reply_structure3 =
					LibraryCacheUtils.cacheJars(bucket3, DataBucketAnalyticsChangeActor.getQuery(bucket3, false),
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

	@Test
	public void test_cacheJars_batchEnrichment() throws UnsupportedFileSystemException, InterruptedException, ExecutionException {
		try {
			// Preamble:
			// 0) Insert 2 library beans into the management db

			final DataBucketBean bucket = DataBucketAnalyticsChangeActor.convertEnrichmentToAnalyticBucket(createBucket("test_tech_id_batch"));		
			
			final String pathname1 = System.getProperty("user.dir") + "/misc_test_assets/simple-harvest-example.jar";
			final Path path1 = FileContext.getLocalFSFileContext().makeQualified(new Path(pathname1));		
			final String pathname2 = System.getProperty("user.dir") + "/misc_test_assets/simple-harvest-example2.jar";
			final Path path2 = FileContext.getLocalFSFileContext().makeQualified(new Path(pathname2));		
			
			List<SharedLibraryBean> lib_elements = createSharedLibraryBeans_batch(path1, path2);
	
			final IManagementDbService underlying_db = _service_context.getService(IManagementDbService.class, Optional.empty()).get();			
			final IManagementCrudService<SharedLibraryBean> library_crud = underlying_db.getSharedLibraryStore();
			library_crud.deleteDatastore();
			assertEquals("Cleansed library store", 0L, (long)library_crud.countObjects().get());
			library_crud.storeObjects(lib_elements).get();
			
			assertEquals("Should have 4 library beans", 4L, (long)library_crud.countObjects().get());
			
			// 0a) Check with no streaming, gets nothing
			{			
				final DataBucketBean bucket0 = DataBucketAnalyticsChangeActor.convertEnrichmentToAnalyticBucket(createBucket("broken"));		
				
				CompletableFuture<Validation<BasicMessageBean, Map<String, Tuple2<SharedLibraryBean, String>>>> reply_structure =
						LibraryCacheUtils.cacheJars(bucket0, DataBucketAnalyticsChangeActor.getQuery(bucket0, false),
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
					"test_name", Collections.emptyList(), Collections.emptyList(), true, null, Arrays.asList("test_tech_id_batch", "test_module_id"), null, new LinkedHashMap<>(), null);
			
			final DataBucketBean bucket2 =
					DataBucketAnalyticsChangeActor.convertEnrichmentToAnalyticBucket(
						BeanTemplateUtils.clone(bucket)
									.with(DataBucketBean::analytic_thread, null)
									.with(DataBucketBean::batch_enrichment_configs, Arrays.asList(enrichment_module))
									.with(DataBucketBean::master_enrichment_type, MasterEnrichmentType.batch)
									.done());
			
			// 1) Normal operation
			
			CompletableFuture<Validation<BasicMessageBean, Map<String, Tuple2<SharedLibraryBean, String>>>> reply_structure =
					LibraryCacheUtils.cacheJars(bucket2, DataBucketAnalyticsChangeActor.getQuery(bucket2, false),
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
					"test_name", Collections.emptyList(), null, true, null, Arrays.asList("test_tech_id_batch", "test_module_id", "failtest"), null, new LinkedHashMap<>(), null);
			
			final DataBucketBean bucket3 = 
					DataBucketAnalyticsChangeActor.convertEnrichmentToAnalyticBucket(
						BeanTemplateUtils.clone(bucket)
									.with(DataBucketBean::analytic_thread, null)
									.with(DataBucketBean::batch_enrichment_configs, Arrays.asList(enrichment_module2))
									.with(DataBucketBean::master_enrichment_type, MasterEnrichmentType.batch)
									.done());
			
			CompletableFuture<Validation<BasicMessageBean, Map<String, Tuple2<SharedLibraryBean, String>>>> reply_structure3 =
					LibraryCacheUtils.cacheJars(bucket3, DataBucketAnalyticsChangeActor.getQuery(bucket3, false),
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

	@Test
	public void test_talkToAnalytics_realCases() throws InterruptedException, ExecutionException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, IOException {
		
		// Some test cases from real life that failed for some reason

		// 1) This _should_ just be a dup to test_talkToAnalytics case 5c but was failing in real life
		{
			final Validation<BasicMessageBean, IAnalyticsTechnologyModule> ret_val = 
					ClassloaderUtils.getFromCustomClasspath(IAnalyticsTechnologyModule.class, 
							"com.ikanow.aleph2.test.example.ExampleAnalyticsTechnology", 
							Optional.of(new File(System.getProperty("user.dir") + File.separator + "misc_test_assets" + File.separator + "simple-analytics-example.jar").getAbsoluteFile().toURI().toString()),
							Collections.emptyList(), "test1", "test");						
			
			if (ret_val.isFail()) {
				fail("getAnalyticsTechnology call failed: " + ret_val.fail().message());
			}
			assertTrue("harvest tech created: ", ret_val.success() != null);
			
			final IAnalyticsTechnologyModule analytics_tech = ret_val.success();			
			
			final ActorRef test_counter = _db_actor_context.getDistributedServices().getAkkaSystem().actorOf(Props.create(TestActor_Counter.class, "test_counter_real1"), "test_counter_real1");			
			final ActorSelection test_counter_selection = _actor_context.getActorSystem().actorSelection("/user/test_counter_real1");
			
			final String json_bucket = Resources.toString(Resources.getResource("com/ikanow/aleph2/data_import_manager/analytics/actors/real_test_case_batch_1.json"), Charsets.UTF_8);
			final DataBucketBean bucket_batch = BeanTemplateUtils.from(json_bucket, DataBucketBean.class).get();
			TestActor_Counter.reset();			
			
			final BucketActionMessage.UpdateBucketActionMessage update = new BucketActionMessage.UpdateBucketActionMessage(bucket_batch, true, bucket_batch, Collections.emptySet());
			
			final CompletableFuture<BucketActionReplyMessage> test = DataBucketAnalyticsChangeActor.talkToAnalytics(
					bucket_batch, update,
					"test1", 
					_actor_context.getNewAnalyticsContext(), Tuples._2T(test_counter, test_counter_selection), 
					Collections.emptyMap(), 
					Validation.success(Tuples._2T(analytics_tech, analytics_tech.getClass().getClassLoader())));
						
			assertEquals(BucketActionReplyMessage.BucketActionCollectedRepliesMessage.class, test.get().getClass());
			final BucketActionReplyMessage.BucketActionCollectedRepliesMessage test_reply = (BucketActionReplyMessage.BucketActionCollectedRepliesMessage) test.get();

			assertEquals("test1", test_reply.source());
			assertEquals(2, test_reply.replies().size());
			final BasicMessageBean test_reply1 = test_reply.replies().stream().skip(0).findFirst().get();
			assertEquals("called onUpdatedThread: true", test_reply1.message());
			assertEquals(true, test_reply1.success());
			final BasicMessageBean test_reply2 = test_reply.replies().stream().skip(1).findFirst().get();
			assertEquals("called resumeAnalyticJob", test_reply2.message());
			assertEquals(true, test_reply2.success());
			
			Thread.sleep(100L); // give the sibling messages a chance to be delivered			
			assertEquals(1, TestActor_Counter.job_counter.get());
			assertEquals(1, TestActor_Counter.msg_counter.get());
			assertTrue("wrong message types: " + TestActor_Counter.message_types.toString(), TestActor_Counter.message_types.keySet().contains(JobMessageType.starting));
		}
	}	
	
	@Test
	public void test_talkToAnalytics() throws InterruptedException, ExecutionException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		final DataBucketBean bucket = createBucket("test_tech_id_analytics");		
		final DataBucketBean bucket_batch = createBatchBucket("test_tech_id_analytics");
		
		// Get the analytics tech module standalone ("in app" way is covered above)
		
		final Validation<BasicMessageBean, IAnalyticsTechnologyModule> ret_val = 
				ClassloaderUtils.getFromCustomClasspath(IAnalyticsTechnologyModule.class, 
						"com.ikanow.aleph2.test.example.ExampleAnalyticsTechnology", 
						Optional.of(new File(System.getProperty("user.dir") + File.separator + "misc_test_assets" + File.separator + "simple-analytics-example.jar").getAbsoluteFile().toURI().toString()),
						Collections.emptyList(), "test1", "test");						
		
		if (ret_val.isFail()) {
			fail("getAnalyticsTechnology call failed: " + ret_val.fail().message());
		}
		assertTrue("harvest tech created: ", ret_val.success() != null);
		
		final IAnalyticsTechnologyModule analytics_tech = ret_val.success();
		
		final ActorRef test_counter = _db_actor_context.getDistributedServices().getAkkaSystem().actorOf(Props.create(TestActor_Counter.class, "test_counter"), "test_counter");
		
		final ActorSelection test_counter_selection = _actor_context.getActorSystem().actorSelection("/user/test_counter");
		
		// Test 1: pass along errors:
		{
			final BasicMessageBean error = SharedErrorUtils.buildErrorMessage("test_source", "test_message", "test_error");

			final CompletableFuture<BucketActionReplyMessage> test1 = DataBucketAnalyticsChangeActor.talkToAnalytics(
					bucket, new BucketActionMessage.DeleteBucketActionMessage(bucket, Collections.emptySet()), 
					"test1", 
					_actor_context.getNewAnalyticsContext(), null, 
					Collections.emptyMap(), 
					Validation.fail(error));
	
			assertEquals(BucketActionReplyMessage.BucketActionHandlerMessage.class, test1.get().getClass());
			final BucketActionReplyMessage.BucketActionHandlerMessage test1err = (BucketActionReplyMessage.BucketActionHandlerMessage) test1.get();
			assertEquals(false, test1err.reply().success());
			assertEquals("test_source", test1err.reply().source());
			assertEquals("test_message", test1err.reply().command());
			assertEquals("test_error", test1err.reply().message());
		}		
		// Test 2: offer
		{
			final BucketActionMessage.BucketActionOfferMessage offer = new BucketActionMessage.BucketActionOfferMessage(bucket);
			
			final CompletableFuture<BucketActionReplyMessage> test2 = DataBucketAnalyticsChangeActor.talkToAnalytics(
					bucket, offer,
					"test2", 
					_actor_context.getNewAnalyticsContext(), null,
					Collections.emptyMap(), 
					Validation.success(Tuples._2T(analytics_tech, analytics_tech.getClass().getClassLoader())));
			
			assertEquals(BucketActionReplyMessage.BucketActionWillAcceptMessage.class, test2.get().getClass());
			final BucketActionReplyMessage.BucketActionWillAcceptMessage test2_reply = (BucketActionReplyMessage.BucketActionWillAcceptMessage) test2.get();
			assertEquals("test2", test2_reply.source());
		}		
		// Test 3: delete
		{
			final BucketActionMessage.DeleteBucketActionMessage delete = new BucketActionMessage.DeleteBucketActionMessage(bucket, Collections.emptySet());

			final CompletableFuture<BucketActionReplyMessage> test3 = DataBucketAnalyticsChangeActor.talkToAnalytics(
					bucket, delete,
					"test3", 
					_actor_context.getNewAnalyticsContext(), null, 
					Collections.emptyMap(), 
					Validation.success(Tuples._2T(analytics_tech, analytics_tech.getClass().getClassLoader())));
						
			assertEquals(BucketActionReplyMessage.BucketActionCollectedRepliesMessage.class, test3.get().getClass());
			final BucketActionReplyMessage.BucketActionCollectedRepliesMessage test_reply = (BucketActionReplyMessage.BucketActionCollectedRepliesMessage) test3.get();
			assertEquals("test3", test_reply.source());
			assertEquals(2, test_reply.replies().size());
			final BasicMessageBean test_reply1 = test_reply.replies().stream().skip(0).findFirst().get();
			assertEquals("called onDeleteThread", test_reply1.message());
			assertEquals(true, test_reply1.success());
			final BasicMessageBean test_reply2 = test_reply.replies().stream().skip(1).findFirst().get();
			assertEquals("called stopAnalyticJob", test_reply2.message());
			assertEquals(true, test_reply2.success());
		}
		// Test 4: new (activated)
		{
			final BucketActionMessage.NewBucketActionMessage create = new BucketActionMessage.NewBucketActionMessage(bucket, false);
			
			final CompletableFuture<BucketActionReplyMessage> test4 = DataBucketAnalyticsChangeActor.talkToAnalytics(
					bucket, create,
					"test4", 
					_actor_context.getNewAnalyticsContext(), null, 
					Collections.emptyMap(), 
					Validation.success(Tuples._2T(analytics_tech, analytics_tech.getClass().getClassLoader())));
						
			assertEquals(BucketActionReplyMessage.BucketActionCollectedRepliesMessage.class, test4.get().getClass());
			final BucketActionReplyMessage.BucketActionCollectedRepliesMessage test_reply = (BucketActionReplyMessage.BucketActionCollectedRepliesMessage) test4.get();
			assertEquals("test4", test_reply.source());
			assertEquals(2, test_reply.replies().size());
			final BasicMessageBean test_reply1 = test_reply.replies().stream().skip(0).findFirst().get();
			assertEquals("called onNewThread: true", test_reply1.message());
			assertEquals(true, test_reply1.success());
			final BasicMessageBean test_reply2 = test_reply.replies().stream().skip(1).findFirst().get();
			assertEquals("called startAnalyticJob", test_reply2.message());
			assertEquals(true, test_reply2.success());
		}		
		// Test 4b: new (suspended)
		{
			final BucketActionMessage.NewBucketActionMessage create = new BucketActionMessage.NewBucketActionMessage(bucket, true);
			
			final CompletableFuture<BucketActionReplyMessage> test4b = DataBucketAnalyticsChangeActor.talkToAnalytics(
					bucket, create,
					"test4b", 
					_actor_context.getNewAnalyticsContext(), null, 
					Collections.emptyMap(), 
					Validation.success(Tuples._2T(analytics_tech, analytics_tech.getClass().getClassLoader())));
						
			assertEquals(BucketActionReplyMessage.BucketActionHandlerMessage.class, test4b.get().getClass());
			final BucketActionReplyMessage.BucketActionHandlerMessage test_reply = (BucketActionReplyMessage.BucketActionHandlerMessage) test4b.get();					
			assertEquals("test4b", test_reply.source());
			assertEquals("called onNewThread: false", test_reply.reply().message());
		}		
		// Test 5: update
		{
			TestActor_Counter.reset();			
			
			final BucketActionMessage.UpdateBucketActionMessage update = new BucketActionMessage.UpdateBucketActionMessage(bucket, true, bucket, Collections.emptySet());
			
			final CompletableFuture<BucketActionReplyMessage> test5 = DataBucketAnalyticsChangeActor.talkToAnalytics(
					bucket, update,
					"test5", 
					_actor_context.getNewAnalyticsContext(), Tuples._2T(test_counter, test_counter_selection), 
						//(send sibling to check does nothing with sibling if not batch)
					Collections.emptyMap(), 
					Validation.success(Tuples._2T(analytics_tech, analytics_tech.getClass().getClassLoader())));
						
			assertEquals(BucketActionReplyMessage.BucketActionCollectedRepliesMessage.class, test5.get().getClass());
			final BucketActionReplyMessage.BucketActionCollectedRepliesMessage test_reply = (BucketActionReplyMessage.BucketActionCollectedRepliesMessage) test5.get();
			assertEquals("test5", test_reply.source());
			assertEquals(2, test_reply.replies().size());
			final BasicMessageBean test_reply1 = test_reply.replies().stream().skip(0).findFirst().get();
			assertEquals("called onUpdatedThread: true", test_reply1.message());
			assertEquals(true, test_reply1.success());
			final BasicMessageBean test_reply2 = test_reply.replies().stream().skip(1).findFirst().get();
			assertEquals("called resumeAnalyticJob", test_reply2.message());
			assertEquals(true, test_reply2.success());			
			
			Thread.sleep(100L); // give the sibling messages a chance to be delivered						
			assertEquals(0, TestActor_Counter.job_counter.get()); // streaming => no sibling messages
		}		
		// Test 5a: update - but with no enabled jobs
		{
			final DataBucketBean disabled_bucket = BeanTemplateUtils.clone(bucket)
														.with(DataBucketBean::analytic_thread, 
																BeanTemplateUtils.clone(bucket.analytic_thread())
																	.with(AnalyticThreadBean::jobs, 
																			bucket.analytic_thread().jobs().stream().map(j ->
																				BeanTemplateUtils.clone(j).with(AnalyticThreadJobBean::enabled, false).done())
																			.collect(Collectors.toList()))
																.done()
																)
													.done();
			
			final BucketActionMessage.UpdateBucketActionMessage update = new BucketActionMessage.UpdateBucketActionMessage(disabled_bucket, true, disabled_bucket, Collections.emptySet());
			
			final CompletableFuture<BucketActionReplyMessage> test5 = DataBucketAnalyticsChangeActor.talkToAnalytics(
					disabled_bucket, update,
					"test5a", 
					_actor_context.getNewAnalyticsContext(), null, 
					Collections.emptyMap(), 
					Validation.success(Tuples._2T(analytics_tech, analytics_tech.getClass().getClassLoader())));
						
			assertEquals(BucketActionReplyMessage.BucketActionCollectedRepliesMessage.class, test5.get().getClass());
			final BucketActionReplyMessage.BucketActionCollectedRepliesMessage test_reply = (BucketActionReplyMessage.BucketActionCollectedRepliesMessage) test5.get();
			assertEquals("test5a", test_reply.source());
			assertEquals(2, test_reply.replies().size());
			final BasicMessageBean test_reply1 = test_reply.replies().stream().skip(0).findFirst().get();
			assertEquals("called onUpdatedThread: true", test_reply1.message());
			assertEquals(true, test_reply1.success());
			final BasicMessageBean test_reply2 = test_reply.replies().stream().skip(1).findFirst().get();
			// SUSPEND BECAUSE IT'S DISABLED
			assertEquals("called suspendAnalyticJob", test_reply2.message());
			assertEquals(true, test_reply2.success());
		}		
		// Test 5b: suspend
		{
			final BucketActionMessage.UpdateBucketActionMessage update = new BucketActionMessage.UpdateBucketActionMessage(bucket, false, bucket, Collections.emptySet());
			
			final CompletableFuture<BucketActionReplyMessage> test5 = DataBucketAnalyticsChangeActor.talkToAnalytics(
					bucket, update,
					"test5b", 
					_actor_context.getNewAnalyticsContext(), null, 
					Collections.emptyMap(), 
					Validation.success(Tuples._2T(analytics_tech, analytics_tech.getClass().getClassLoader())));
						
			assertEquals(BucketActionReplyMessage.BucketActionCollectedRepliesMessage.class, test5.get().getClass());
			final BucketActionReplyMessage.BucketActionCollectedRepliesMessage test_reply = (BucketActionReplyMessage.BucketActionCollectedRepliesMessage) test5.get();
			assertEquals("test5b", test_reply.source());
			assertEquals(2, test_reply.replies().size());
			final BasicMessageBean test_reply1 = test_reply.replies().stream().skip(0).findFirst().get();
			assertEquals("called onUpdatedThread: false", test_reply1.message());
			assertEquals(true, test_reply1.success());
			final BasicMessageBean test_reply2 = test_reply.replies().stream().skip(1).findFirst().get();
			assertEquals("called suspendAnalyticJob", test_reply2.message());
			assertEquals(true, test_reply2.success());
		}		
		// Test 5c: update batch (only dependency-less jobs will be run)
		{
			TestActor_Counter.reset();			
			
			final BucketActionMessage.UpdateBucketActionMessage update = new BucketActionMessage.UpdateBucketActionMessage(bucket_batch, true, bucket_batch, Collections.emptySet());
			
			final CompletableFuture<BucketActionReplyMessage> test5 = DataBucketAnalyticsChangeActor.talkToAnalytics(
					bucket_batch, update,
					"test5c", 
					_actor_context.getNewAnalyticsContext(), Tuples._2T(test_counter, test_counter_selection), 
					Collections.emptyMap(), 
					Validation.success(Tuples._2T(analytics_tech, analytics_tech.getClass().getClassLoader())));
						
			assertEquals(BucketActionReplyMessage.BucketActionCollectedRepliesMessage.class, test5.get().getClass());
			final BucketActionReplyMessage.BucketActionCollectedRepliesMessage test_reply = (BucketActionReplyMessage.BucketActionCollectedRepliesMessage) test5.get();

			assertEquals("test5c", test_reply.source());
			assertEquals(3, test_reply.replies().size());
			final BasicMessageBean test_reply1 = test_reply.replies().stream().skip(0).findFirst().get();
			assertEquals("called onUpdatedThread: true", test_reply1.message());
			assertEquals(true, test_reply1.success());
			final BasicMessageBean test_reply2 = test_reply.replies().stream().skip(1).findFirst().get();
			assertEquals("called resumeAnalyticJob", test_reply2.message());
			assertEquals(true, test_reply2.success());
			
			Thread.sleep(100L); // give the sibling messages a chance to be delivered			
			assertEquals(2, TestActor_Counter.job_counter.get());
			assertEquals(1, TestActor_Counter.msg_counter.get());
			assertTrue("wrong message types: " + TestActor_Counter.message_types.toString(), TestActor_Counter.message_types.keySet().contains(JobMessageType.starting));
		}
		// Test 5d: suspend batch (all jobs will be suspended)
		{
			TestActor_Counter.reset();
			
			final BucketActionMessage.UpdateBucketActionMessage update = new BucketActionMessage.UpdateBucketActionMessage(bucket_batch, false, bucket, Collections.emptySet());
			
			final CompletableFuture<BucketActionReplyMessage> test5 = DataBucketAnalyticsChangeActor.talkToAnalytics(
					bucket_batch, update,
					"test5d", 
					_actor_context.getNewAnalyticsContext(), Tuples._2T(test_counter, test_counter_selection), 
					Collections.emptyMap(), 
					Validation.success(Tuples._2T(analytics_tech, analytics_tech.getClass().getClassLoader())));
						
			assertEquals(BucketActionReplyMessage.BucketActionCollectedRepliesMessage.class, test5.get().getClass());
			final BucketActionReplyMessage.BucketActionCollectedRepliesMessage test_reply = (BucketActionReplyMessage.BucketActionCollectedRepliesMessage) test5.get();
			assertEquals("test5d", test_reply.source());
			assertEquals(4, test_reply.replies().size());
			final BasicMessageBean test_reply1 = test_reply.replies().stream().skip(0).findFirst().get();
			assertEquals("called onUpdatedThread: false", test_reply1.message());
			assertEquals(true, test_reply1.success());
			final BasicMessageBean test_reply2 = test_reply.replies().stream().skip(1).findFirst().get();
			assertEquals("called suspendAnalyticJob", test_reply2.message());
			assertEquals(true, test_reply2.success());
			
			Thread.sleep(100L); // give the sibling messages a chance to be delivered			
			assertEquals(3, TestActor_Counter.job_counter.get());
			assertEquals(1, TestActor_Counter.msg_counter.get());
			assertTrue("wrong message types: " + TestActor_Counter.message_types.toString(), TestActor_Counter.message_types.keySet().contains(JobMessageType.stopping));			
		}
		// Test 5e: update bucket with triggers (should see no jobs)
		// Test 5e.1: 
		{
			// Automated trigger
			final DataBucketBean trigger_batch_bucket = BeanTemplateUtils.clone(bucket_batch)
															.with(DataBucketBean::analytic_thread, 
																	BeanTemplateUtils.clone(bucket_batch.analytic_thread())
																		.with(AnalyticThreadBean::trigger_config, 
																				BeanTemplateUtils.build(AnalyticThreadTriggerBean.class)
																				.done().get()
																				)
																	.done()
																	)
															.done();
			
			final BucketActionMessage.UpdateBucketActionMessage update = new BucketActionMessage.UpdateBucketActionMessage(trigger_batch_bucket, true, bucket_batch, Collections.emptySet());
			
			final CompletableFuture<BucketActionReplyMessage> test5 = DataBucketAnalyticsChangeActor.talkToAnalytics(
					trigger_batch_bucket, update,
					"test5e.1", 
					_actor_context.getNewAnalyticsContext(), null, 
					Collections.emptyMap(), 
					Validation.success(Tuples._2T(analytics_tech, analytics_tech.getClass().getClassLoader())));
						
			assertEquals(BucketActionReplyMessage.BucketActionHandlerMessage.class, test5.get().getClass());
			final BucketActionReplyMessage.BucketActionHandlerMessage test_reply = (BucketActionReplyMessage.BucketActionHandlerMessage) test5.get();					
			assertEquals("test5e.1", test_reply.source());
			assertEquals("called onUpdatedThread: true", test_reply.reply().message());			
		}
		// Test 5e.2: 
		{
			// Disabled trigger
			final DataBucketBean trigger_batch_bucket = BeanTemplateUtils.clone(bucket_batch)
															.with(DataBucketBean::analytic_thread, 
																	BeanTemplateUtils.clone(bucket_batch.analytic_thread())
																		.with(AnalyticThreadBean::trigger_config, 
																				BeanTemplateUtils.build(AnalyticThreadTriggerBean.class)
																					.with(AnalyticThreadTriggerBean::enabled, false)
																				.done().get()
																				)
																	.done()
																	)
															.done();
			
			final BucketActionMessage.UpdateBucketActionMessage update = new BucketActionMessage.UpdateBucketActionMessage(trigger_batch_bucket, true, bucket_batch, Collections.emptySet());
			
			final CompletableFuture<BucketActionReplyMessage> test5 = DataBucketAnalyticsChangeActor.talkToAnalytics(
					trigger_batch_bucket, update,
					"test5e.2", 
					_actor_context.getNewAnalyticsContext(), null, 
					Collections.emptyMap(), 
					Validation.success(Tuples._2T(analytics_tech, analytics_tech.getClass().getClassLoader())));
						
			assertEquals(BucketActionReplyMessage.BucketActionCollectedRepliesMessage.class, test5.get().getClass());
			final BucketActionReplyMessage.BucketActionCollectedRepliesMessage test_reply = (BucketActionReplyMessage.BucketActionCollectedRepliesMessage) test5.get();
			assertEquals("test5e.2", test_reply.source());
			assertEquals(3, test_reply.replies().size());
			final BasicMessageBean test_reply1 = test_reply.replies().stream().skip(0).findFirst().get();
			assertEquals("called onUpdatedThread: true", test_reply1.message());
			assertEquals(true, test_reply1.success());
			final BasicMessageBean test_reply2 = test_reply.replies().stream().skip(1).findFirst().get();
			assertEquals("called resumeAnalyticJob", test_reply2.message());
			assertEquals(true, test_reply2.success());
		}
		// Test 5e.3: 
		{
			// Automated trigger
			final DataBucketBean trigger_batch_bucket = BeanTemplateUtils.clone(bucket_batch)
															.with(DataBucketBean::analytic_thread, 
																	BeanTemplateUtils.clone(bucket_batch.analytic_thread())
																		.with(AnalyticThreadBean::trigger_config, 
																				BeanTemplateUtils.build(AnalyticThreadTriggerBean.class)
																				.done().get()
																				)
																	.done()
																	)
															.done();
			final BucketActionMessage.UpdateBucketActionMessage update = new BucketActionMessage.UpdateBucketActionMessage(trigger_batch_bucket, false, bucket, Collections.emptySet());
			
			final CompletableFuture<BucketActionReplyMessage> test5 = DataBucketAnalyticsChangeActor.talkToAnalytics(
					trigger_batch_bucket, update,
					"test5e.3", 
					_actor_context.getNewAnalyticsContext(), null, 
					Collections.emptyMap(), 
					Validation.success(Tuples._2T(analytics_tech, analytics_tech.getClass().getClassLoader())));
						
			assertEquals(BucketActionReplyMessage.BucketActionCollectedRepliesMessage.class, test5.get().getClass());
			final BucketActionReplyMessage.BucketActionCollectedRepliesMessage test_reply = (BucketActionReplyMessage.BucketActionCollectedRepliesMessage) test5.get();
			assertEquals("test5e.3", test_reply.source());
			assertEquals(4, test_reply.replies().size());
			final BasicMessageBean test_reply1 = test_reply.replies().stream().skip(0).findFirst().get();
			assertEquals("called onUpdatedThread: false", test_reply1.message());
			assertEquals(true, test_reply1.success());
			final BasicMessageBean test_reply2 = test_reply.replies().stream().skip(1).findFirst().get();
			assertEquals("called suspendAnalyticJob", test_reply2.message());
			assertEquals(true, test_reply2.success());
		}
		
		// Test 6: update state - (a) resume, (b) suspend
		{
			//(REMOVED NOW SUSPEND/RESUME ARE HANDLED BY UPDATE WITH THE APPROPRIATE DIFF SENT)
		}		
		// Test 7: purge
		{
			final PurgeBucketActionMessage purge_msg = new PurgeBucketActionMessage(bucket, Collections.emptySet());
			
			final CompletableFuture<BucketActionReplyMessage> test7 = DataBucketAnalyticsChangeActor.talkToAnalytics(
					bucket, purge_msg,
					"test7", 
					_actor_context.getNewAnalyticsContext(), null, 
					Collections.emptyMap(), 
					Validation.success(Tuples._2T(analytics_tech, analytics_tech.getClass().getClassLoader())));
						
			assertEquals(BucketActionReplyMessage.BucketActionHandlerMessage.class, test7.get().getClass());
			final BucketActionReplyMessage.BucketActionHandlerMessage test7_reply = (BucketActionReplyMessage.BucketActionHandlerMessage) test7.get();		
			
			assertEquals("test7", test7_reply.source());
			assertEquals("called onPurge", test7_reply.reply().message());
		}		
		// Test 8: test
		{
			final ProcessingTestSpecBean test_spec = new ProcessingTestSpecBean(10L, 1L);
			final BucketActionMessage.TestBucketActionMessage test = new BucketActionMessage.TestBucketActionMessage(bucket, test_spec);
			
			final CompletableFuture<BucketActionReplyMessage> test8 = DataBucketAnalyticsChangeActor.talkToAnalytics(
					bucket, test,
					"test8", 
					_actor_context.getNewAnalyticsContext(), null, 
					Collections.emptyMap(), 
					Validation.success(Tuples._2T(analytics_tech, analytics_tech.getClass().getClassLoader())));
									
			assertEquals(BucketActionReplyMessage.BucketActionCollectedRepliesMessage.class, test8.get().getClass());
			final BucketActionReplyMessage.BucketActionCollectedRepliesMessage test_reply = (BucketActionReplyMessage.BucketActionCollectedRepliesMessage) test8.get();
			assertEquals("test8", test_reply.source());
			assertEquals(2, test_reply.replies().size());
			final BasicMessageBean test_reply1 = test_reply.replies().stream().skip(0).findFirst().get();
			assertEquals("called onTestThread", test_reply1.message());
			assertEquals(true, test_reply1.success());
			final BasicMessageBean test_reply2 = test_reply.replies().stream().skip(1).findFirst().get();
			assertEquals("called startAnalyticJobTest", test_reply2.message());
			assertEquals(true, test_reply2.success());
		}
		// Test 8b: test - but with no enabled jobs
		{
			final DataBucketBean disabled_bucket = BeanTemplateUtils.clone(bucket)
														.with(DataBucketBean::analytic_thread, 
																BeanTemplateUtils.clone(bucket.analytic_thread())
																	.with(AnalyticThreadBean::jobs, 
																			bucket.analytic_thread().jobs().stream().map(j ->
																				BeanTemplateUtils.clone(j).with(AnalyticThreadJobBean::enabled, false).done())
																			.collect(Collectors.toList()))
																.done()
																)
													.done();
			
			final ProcessingTestSpecBean test_spec = new ProcessingTestSpecBean(10L, 1L);
			final BucketActionMessage.TestBucketActionMessage test = new BucketActionMessage.TestBucketActionMessage(bucket, test_spec);
			
			final CompletableFuture<BucketActionReplyMessage> test8 = DataBucketAnalyticsChangeActor.talkToAnalytics(
					disabled_bucket, test,
					"test8b", 
					_actor_context.getNewAnalyticsContext(), null, 
					Collections.emptyMap(), 
					Validation.success(Tuples._2T(analytics_tech, analytics_tech.getClass().getClassLoader())));
									
			assertEquals(BucketActionReplyMessage.BucketActionHandlerMessage.class, test8.get().getClass());
			final BucketActionReplyMessage.BucketActionHandlerMessage test_reply = (BucketActionReplyMessage.BucketActionHandlerMessage) test8.get();
			assertEquals("test8b", test_reply.source());
			final BasicMessageBean test_reply1 = test_reply.reply();
			assertEquals("called onTestThread", test_reply1.message());
			assertEquals(true, test_reply1.success());
		}	
		
		// Test 9: poll
		{
			final BucketActionMessage.PollFreqBucketActionMessage poll = new BucketActionMessage.PollFreqBucketActionMessage(bucket);
			
			final CompletableFuture<BucketActionReplyMessage> test9 = DataBucketAnalyticsChangeActor.talkToAnalytics(
					bucket, poll,
					"test9", 
					_actor_context.getNewAnalyticsContext(), null, 
					Collections.emptyMap(), 
					Validation.success(Tuples._2T(analytics_tech, analytics_tech.getClass().getClassLoader())));
						
			assertEquals(BucketActionReplyMessage.BucketActionHandlerMessage.class, test9.get().getClass());
			final BucketActionReplyMessage.BucketActionHandlerMessage test9_reply = (BucketActionReplyMessage.BucketActionHandlerMessage) test9.get();		
			
			assertEquals("test9", test9_reply.source());
			assertEquals("called onPeriodicPoll", test9_reply.reply().message());
		}

		// Test X: unrecognized
		{
			// Use reflection to create a "raw" BucketActionMessage
			final Constructor<BucketActionMessage> contructor = (Constructor<BucketActionMessage>) BucketActionMessage.class.getDeclaredConstructor(DataBucketBean.class);
			contructor.setAccessible(true);
			BucketActionMessage bad_msg = contructor.newInstance(bucket);
	
			final CompletableFuture<BucketActionReplyMessage> testX = DataBucketAnalyticsChangeActor.talkToAnalytics(
					bucket, bad_msg,
					"testX", 
					_actor_context.getNewAnalyticsContext(), null, 
					Collections.emptyMap(), 
					Validation.success(Tuples._2T(analytics_tech, analytics_tech.getClass().getClassLoader())));
									
			assertEquals(BucketActionReplyMessage.BucketActionHandlerMessage.class, testX.get().getClass());
			final BucketActionReplyMessage.BucketActionHandlerMessage testX_reply = (BucketActionReplyMessage.BucketActionHandlerMessage) testX.get();
			assertEquals(false, testX_reply.reply().success());
			assertEquals("testX", testX_reply.source());
			assertEquals("Message type BucketActionMessage not recognized for bucket /test/path/", testX_reply.reply().message());
		}		
		
	}	
	
	@Test
	public void test_talkToAnalytics_analyticsMessages() throws InterruptedException, ExecutionException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		//final DataBucketBean bucket = createBucket("test_tech_id_analytics");		
		final DataBucketBean bucket_batch = createBatchBucket("test_tech_id_analytics");
		
		// Get the analytics tech module standalone ("in app" way is covered above)
		
		final Validation<BasicMessageBean, IAnalyticsTechnologyModule> ret_val = 
				ClassloaderUtils.getFromCustomClasspath(IAnalyticsTechnologyModule.class, 
						"com.ikanow.aleph2.test.example.ExampleAnalyticsTechnology", 
						Optional.of(new File(System.getProperty("user.dir") + File.separator + "misc_test_assets" + File.separator + "simple-analytics-example.jar").getAbsoluteFile().toURI().toString()),
						Collections.emptyList(), "test1", "test");						
		
		if (ret_val.isFail()) {
			fail("getAnalyticsTechnology call failed: " + ret_val.fail().message());
		}
		assertTrue("harvest tech created: ", ret_val.success() != null);
		
		final IAnalyticsTechnologyModule analytics_tech = ret_val.success();
		
		final ActorRef test_counter = _db_actor_context.getDistributedServices().getAkkaSystem().actorOf(Props.create(TestActor_Counter.class, "test_counter_2"), "test_counter_2");
		
		final ActorSelection test_counter_selection = _actor_context.getActorSystem().actorSelection("/user/test_counter_2");
		
		// Automated trigger
		final DataBucketBean trigger_batch_bucket = BeanTemplateUtils.clone(bucket_batch)
														.with(DataBucketBean::analytic_thread, 
																BeanTemplateUtils.clone(bucket_batch.analytic_thread())
																	.with(AnalyticThreadBean::trigger_config, 
																			BeanTemplateUtils.build(AnalyticThreadTriggerBean.class)
																			.done().get()
																			)
																.done()
																)
														.done();
		
		// 1) Check completion
		{
			TestActor_Counter.reset();
			
			final BucketActionMessage.BucketActionAnalyticJobMessage check_complete = 
					new BucketActionMessage.BucketActionAnalyticJobMessage(
							trigger_batch_bucket, 
							Arrays.asList(
									trigger_batch_bucket.analytic_thread().jobs().stream().findFirst().get(),
									trigger_batch_bucket.analytic_thread().jobs().stream().skip(1).findFirst().get()
									), 
							JobMessageType.check_completion);
					
					//.UpdateBucketActionMessage(trigger_batch_bucket, true, bucket_batch, Collections.emptySet());
			
			final CompletableFuture<BucketActionReplyMessage> test = DataBucketAnalyticsChangeActor.talkToAnalytics(
					trigger_batch_bucket, check_complete,
					"test1", 
					_actor_context.getNewAnalyticsContext(), Tuples._2T(test_counter, test_counter_selection), 
					Collections.emptyMap(), 
					Validation.success(Tuples._2T(analytics_tech, analytics_tech.getClass().getClassLoader())));
						
			Thread.sleep(100L);
			assertEquals(BucketActionReplyMessage.BucketActionNullReplyMessage.class, test.get().getClass());
			assertEquals(2, TestActor_Counter.job_counter.get());
			assertEquals(1, TestActor_Counter.msg_counter.get());
			assertTrue("wrong message types: " + TestActor_Counter.message_types.toString(), TestActor_Counter.message_types.keySet().contains(JobMessageType.stopping));
		}
		// 2) Start notification that bucket has triggered
		{
			TestActor_Counter.reset();
			
			final BucketActionMessage.BucketActionAnalyticJobMessage bucket_starting = 
					new BucketActionMessage.BucketActionAnalyticJobMessage(
							trigger_batch_bucket, 
							null, 
							JobMessageType.starting);
					
					//.UpdateBucketActionMessage(trigger_batch_bucket, true, bucket_batch, Collections.emptySet());
			
			final CompletableFuture<BucketActionReplyMessage> test = DataBucketAnalyticsChangeActor.talkToAnalytics(
					trigger_batch_bucket, bucket_starting,
					"test2", 
					_actor_context.getNewAnalyticsContext(), Tuples._2T(test_counter, test_counter_selection), 
					Collections.emptyMap(), 
					Validation.success(Tuples._2T(analytics_tech, analytics_tech.getClass().getClassLoader())));
						
			Thread.sleep(100L);
			assertEquals(BucketActionReplyMessage.BucketActionNullReplyMessage.class, test.get().getClass());
			assertEquals(2, TestActor_Counter.job_counter.get());
			assertEquals(1, TestActor_Counter.msg_counter.get());
			assertTrue("wrong message types: " + TestActor_Counter.message_types.toString(), TestActor_Counter.message_types.keySet().contains(JobMessageType.starting));
		}
		// 3) Instruction to start some jobs for an active bucket
		{
			TestActor_Counter.reset();
			
			final BucketActionMessage.BucketActionAnalyticJobMessage job_starting = 
					new BucketActionMessage.BucketActionAnalyticJobMessage(
							trigger_batch_bucket, 
							Arrays.asList(trigger_batch_bucket.analytic_thread().jobs().stream().findFirst().get()), 
							JobMessageType.starting);
					
					//.UpdateBucketActionMessage(trigger_batch_bucket, true, bucket_batch, Collections.emptySet());
			
			final CompletableFuture<BucketActionReplyMessage> test = DataBucketAnalyticsChangeActor.talkToAnalytics(
					trigger_batch_bucket, job_starting,
					"test3", 
					_actor_context.getNewAnalyticsContext(), Tuples._2T(test_counter, test_counter_selection), 
					Collections.emptyMap(), 
					Validation.success(Tuples._2T(analytics_tech, analytics_tech.getClass().getClassLoader())));
						
			//(these come from sibling so not replying there with anything)
			Thread.sleep(100L);
			assertEquals(BucketActionReplyMessage.BucketActionNullReplyMessage.class, test.get().getClass());
			assertEquals(0, TestActor_Counter.job_counter.get());
			assertEquals(0, TestActor_Counter.msg_counter.get());
		}
		// 4) notification that bucket has stopped
		{
			TestActor_Counter.reset();
			
			final BucketActionMessage.BucketActionAnalyticJobMessage bucket_stopping = 
					new BucketActionMessage.BucketActionAnalyticJobMessage(
							trigger_batch_bucket, 
							null, 
							JobMessageType.stopping);
					
					//.UpdateBucketActionMessage(trigger_batch_bucket, true, bucket_batch, Collections.emptySet());
			
			final CompletableFuture<BucketActionReplyMessage> test = DataBucketAnalyticsChangeActor.talkToAnalytics(
					trigger_batch_bucket, bucket_stopping,
					"test4", 
					_actor_context.getNewAnalyticsContext(), Tuples._2T(test_counter, test_counter_selection), 
					Collections.emptyMap(), 
					Validation.success(Tuples._2T(analytics_tech, analytics_tech.getClass().getClassLoader())));
						
			Thread.sleep(100L);
			assertEquals(BucketActionReplyMessage.BucketActionNullReplyMessage.class, test.get().getClass());
			assertEquals(0, TestActor_Counter.job_counter.get());
			assertEquals(0, TestActor_Counter.msg_counter.get());
		}
		// 5) instruction to stop a job
		{
			TestActor_Counter.reset();
			
			final BucketActionMessage.BucketActionAnalyticJobMessage jobs_stopping = 
					new BucketActionMessage.BucketActionAnalyticJobMessage(
							trigger_batch_bucket, 
							trigger_batch_bucket.analytic_thread().jobs().stream().collect(Collectors.toList()), 
							JobMessageType.stopping);
					
					//.UpdateBucketActionMessage(trigger_batch_bucket, true, bucket_batch, Collections.emptySet());
			
			final CompletableFuture<BucketActionReplyMessage> test = DataBucketAnalyticsChangeActor.talkToAnalytics(
					trigger_batch_bucket, jobs_stopping,
					"test5", 
					_actor_context.getNewAnalyticsContext(), Tuples._2T(test_counter, test_counter_selection), 
					Collections.emptyMap(), 
					Validation.success(Tuples._2T(analytics_tech, analytics_tech.getClass().getClassLoader())));
						
			Thread.sleep(100L);
			assertEquals(BucketActionReplyMessage.BucketActionNullReplyMessage.class, test.get().getClass());
			assertEquals(0, TestActor_Counter.job_counter.get());
			assertEquals(0, TestActor_Counter.msg_counter.get());
		}
		
	}	
	
	@Test
	public void test_setPerJobContextParams() {
		
		List<SharedLibraryBean> l = createSharedLibraryBeans_analytics(new Path("/test1"), new Path("/test2"));
		Map<String, Tuple2<SharedLibraryBean, String>> map = l.stream().collect(Collectors.toMap(
				bean -> bean.path_name(),
				bean -> Tuples._2T(bean, bean.path_name())
				));
		
		final AnalyticsContext context = _actor_context.getNewAnalyticsContext(); 
		
		assertTrue("module config not present", context.getLibraryConfigs().isEmpty());
		
		// Check works
		{
			final AnalyticThreadJobBean test = BeanTemplateUtils.build(AnalyticThreadJobBean.class)
												.with(AnalyticThreadJobBean::module_name_or_id, "/test1")
												.done().get();
			
			DataBucketAnalyticsChangeActor.setPerJobContextParams(test, context, map);

			assertTrue("Finds module config", context.getLibraryConfigs().containsKey("/test1"));
		}
		// Check works again
		{
			final AnalyticThreadJobBean test = BeanTemplateUtils.build(AnalyticThreadJobBean.class)
												.with(AnalyticThreadJobBean::module_name_or_id, "/test2")
												.done().get();
			
			DataBucketAnalyticsChangeActor.setPerJobContextParams(test, context, map);

			assertTrue("Finds module config", context.getLibraryConfigs().containsKey("/test2"));
		}
		// Check unsets - no module name
		{
			final AnalyticThreadJobBean test = BeanTemplateUtils.build(AnalyticThreadJobBean.class)
												.done().get();
			
			DataBucketAnalyticsChangeActor.setPerJobContextParams(test, context, map);

			assertTrue("module config not present", context.getLibraryConfigs().isEmpty());
		}
		// (and then works again)
		{
			final AnalyticThreadJobBean test = BeanTemplateUtils.build(AnalyticThreadJobBean.class)
												.with(AnalyticThreadJobBean::module_name_or_id, "/test2")
												.done().get();
			
			DataBucketAnalyticsChangeActor.setPerJobContextParams(test, context, map);

			assertTrue("Finds module config", context.getLibraryConfigs().containsKey("/test2"));
		}
		// Check unsets - module name not found
		{
			final AnalyticThreadJobBean test = BeanTemplateUtils.build(AnalyticThreadJobBean.class)
										.with(AnalyticThreadJobBean::module_name_or_id, "/test3")
												.done().get();
			
			DataBucketAnalyticsChangeActor.setPerJobContextParams(test, context, map);

			assertTrue("module config not present", context.getLibraryConfigs().isEmpty());
		}
		// (and then works again)
		{
			final AnalyticThreadJobBean test = BeanTemplateUtils.build(AnalyticThreadJobBean.class)
												.with(AnalyticThreadJobBean::module_name_or_id, "/test1")
												.done().get();
			
			DataBucketAnalyticsChangeActor.setPerJobContextParams(test, context, map);

			assertTrue("Finds module config", context.getLibraryConfigs().containsKey("/test1"));
		}
	}
	
	@Test
	public void test_combineResults() {		
	
		List<CompletableFuture<BasicMessageBean>> test1_jobs = Arrays.asList(
				CompletableFuture.completedFuture(ErrorUtils.buildMessage(true, "test1_jobs", "test1_jobs", "test1_jobs"))
				);
		
		// case 1: blank message returned from top level call
		
		BasicMessageBean top_level = ErrorUtils.buildMessage(true, "test1", "test1", "");
		
		BucketActionReplyMessage res1 = 
				DataBucketAnalyticsChangeActor.combineResults(CompletableFuture.completedFuture(top_level), test1_jobs, "test1").join();
		
		assertTrue("Is a multi message type: " + res1.getClass(), res1 instanceof BucketActionReplyMessage.BucketActionCollectedRepliesMessage);
		assertEquals("One reply", 1, ((BucketActionReplyMessage.BucketActionCollectedRepliesMessage)res1).replies().size());
		
		// case 2: blank but error so return

		BasicMessageBean top_level2 = ErrorUtils.buildMessage(false, "test1", "test1", "");
		
		BucketActionReplyMessage res2 = 
				DataBucketAnalyticsChangeActor.combineResults(CompletableFuture.completedFuture(top_level2), test1_jobs, "test2").join();
		
		assertTrue("Is a multi message type: " + res2.getClass(), res2 instanceof BucketActionReplyMessage.BucketActionCollectedRepliesMessage);
		assertEquals("2 replies", 2, ((BucketActionReplyMessage.BucketActionCollectedRepliesMessage)res2).replies().size());
	}
	
	@Test
	public void test_enrichToAnalyticsConversion_batch() throws IOException {
		// Simple functional test
		final String bucket_in_str = Resources.toString(Resources.getResource("com/ikanow/aleph2/data_import_manager/analytics/actors/batch_enrichment_test_in.json"), Charsets.UTF_8);
		final String bucket_out_str = Resources.toString(Resources.getResource("com/ikanow/aleph2/data_import_manager/analytics/actors/batch_enrichment_test_out.json"), Charsets.UTF_8);
		
		final DataBucketBean in = BeanTemplateUtils.from(bucket_in_str, DataBucketBean.class).get();
		final DataBucketBean out = BeanTemplateUtils.from(bucket_out_str, DataBucketBean.class).get();
		
		{
			final DataBucketBean res = DataBucketAnalyticsChangeActor.convertEnrichmentToAnalyticBucket(in);
			
			assertEquals(BeanTemplateUtils.toJson(out).toString(), BeanTemplateUtils.toJson(res).toString());
		}		
	}
	
	@Test
	public void test_enrichToAnalyticsConversion_streaming() throws IOException {
		// Simple functional test
		final String bucket_in_str = Resources.toString(Resources.getResource("com/ikanow/aleph2/data_import_manager/analytics/actors/stream_enrichment_test_in.json"), Charsets.UTF_8);
		final String bucket_out_str = Resources.toString(Resources.getResource("com/ikanow/aleph2/data_import_manager/analytics/actors/stream_enrichment_test_out.json"), Charsets.UTF_8);
		final String bucket_final_str = Resources.toString(Resources.getResource("com/ikanow/aleph2/data_import_manager/analytics/actors/stream_enrichment_test_final.json"), Charsets.UTF_8);
		
		final DataBucketBean in = BeanTemplateUtils.from(bucket_in_str, DataBucketBean.class).get();
		final DataBucketBean out = BeanTemplateUtils.from(bucket_out_str, DataBucketBean.class).get();
		final DataBucketBean final_bucket = BeanTemplateUtils.from(bucket_final_str, DataBucketBean.class).get();
		
		{
			final DataBucketBean res = DataBucketAnalyticsChangeActor.convertEnrichmentToAnalyticBucket(in);
			
			assertEquals(BeanTemplateUtils.toJson(out).toString(), BeanTemplateUtils.toJson(res).toString());
		}
		
		// Test some aspects of the final conversion also:
		
		// empty
		final Map<String, Tuple2<SharedLibraryBean, String>> map1 = Collections.emptyMap();
		
		// no entry point
		final Map<String, Tuple2<SharedLibraryBean, String>> map2 = ImmutableMap.<String, Tuple2<SharedLibraryBean, String>>builder()
					.put("/app/aleph2/library/storm_js_main.jar", 
							Tuples._2T(
									BeanTemplateUtils.build(SharedLibraryBean.class).done().get()
									, 
									"")
							)
							.build();
		
		// misc entry point
		final Map<String, Tuple2<SharedLibraryBean, String>> map3 = ImmutableMap.<String, Tuple2<SharedLibraryBean, String>>builder()
				.put("/app/aleph2/library/storm_js_main.jar", 
						Tuples._2T(
								BeanTemplateUtils.build(SharedLibraryBean.class)
									.with(SharedLibraryBean::misc_entry_point, "test.entry.point")
								.done().get()
								, 
								"")
						)
						.build();
		
		// misc entry point
		final Map<String, Tuple2<SharedLibraryBean, String>> map4 = ImmutableMap.<String, Tuple2<SharedLibraryBean, String>>builder()
				.put("/app/aleph2/library/storm_js_test.jar", 
						Tuples._2T(
								BeanTemplateUtils.build(SharedLibraryBean.class)
									.with(SharedLibraryBean::streaming_enrichment_entry_point, "test.entry.point")
								.done().get()
								, 
								"")
						)
						.build();
		
		// Error with libs 
		{
			final DataBucketBean res = DataBucketAnalyticsChangeActor.finalBucketConversion(DataBucketAnalyticsChangeActor.STREAMING_ENRICHMENT_TECH_NAME, out, Validation.fail(ErrorUtils.buildErrorMessage("", "", "")));
			assertEquals(res, out);
		}
		// No error, but not streaming
		{
			final DataBucketBean res = DataBucketAnalyticsChangeActor.finalBucketConversion("NOT_STREAMING", out, Validation.success(map3));
			assertEquals(res, out);
		}
		// Can't find item in map
		{
			final DataBucketBean res = DataBucketAnalyticsChangeActor.finalBucketConversion(DataBucketAnalyticsChangeActor.STREAMING_ENRICHMENT_TECH_NAME, out, Validation.success(map1));
			assertEquals(res, out);
		}
		// No entry point
		{
			final DataBucketBean res = DataBucketAnalyticsChangeActor.finalBucketConversion(DataBucketAnalyticsChangeActor.STREAMING_ENRICHMENT_TECH_NAME, out, Validation.success(map2));
			assertEquals(res, out);
		}
		// Misc entry point, success
		{
			final DataBucketBean res = DataBucketAnalyticsChangeActor.finalBucketConversion(DataBucketAnalyticsChangeActor.STREAMING_ENRICHMENT_TECH_NAME, out, Validation.success(map3));
			assertEquals(BeanTemplateUtils.toJson(final_bucket).toString(), BeanTemplateUtils.toJson(res).toString());
		}
		// Streaming entry point, success
		{
			final DataBucketBean res = DataBucketAnalyticsChangeActor.finalBucketConversion(DataBucketAnalyticsChangeActor.STREAMING_ENRICHMENT_TECH_NAME, out, Validation.success(map4));
			assertEquals(BeanTemplateUtils.toJson(final_bucket).toString(), BeanTemplateUtils.toJson(res).toString());
		}
	}
		
	@Test
	public void test_classloader() throws ClassNotFoundException {
		
		// TEst some standard operations:
		
		final Validation<BasicMessageBean, IAnalyticsTechnologyModule> res = ClassloaderUtils.getFromCustomClasspath(IAnalyticsTechnologyModule.class, 
				"com.ikanow.aleph2.test.example.ExampleAnalyticsTechnology", 
				Optional.empty(),
				Arrays.asList("file:misc_test_assets/simple-analytics-example.jar"),
				"test",
				"test");
		
		assertTrue("Should get module: " + res.validation(f->f.message(), s->""), res.isSuccess());
		
		// Check can't find example harvest technology in classpath
		
		try {
			Class.forName("com.ikanow.aleph2.test.example.ExampleHarvestTechnology");
			fail("Should throw class not found exception");
		}
		catch (Throwable e) {}
		
		final ClassLoader saved_current_classloader = Thread.currentThread().getContextClassLoader();		
		try {			
			_logger.info("Set active classloader=" + res.success().getClass().getClassLoader() + " class=" + res.success().getClass());					
			Thread.currentThread().setContextClassLoader(res.success().getClass().getClassLoader());
			
			try {
				// This _doesn't_ work (defaults to system classloader):
				Class.forName("com.ikanow.aleph2.test.example.ExampleHarvestTechnology");
				fail("Should still throw class not found exception");
			}
			catch (Throwable t) {}
			
			Class<?> h = Class.forName("com.ikanow.aleph2.test.example.ExampleHarvestTechnology", true, Thread.currentThread().getContextClassLoader());
			assertEquals("com.ikanow.aleph2.test.example.ExampleHarvestTechnology", h.getName());
		}
		finally {
			Thread.currentThread().setContextClassLoader(saved_current_classloader);
		}
		// Back to original classpath
		try {
			Class.forName("com.ikanow.aleph2.test.example.ExampleHarvestTechnology");
			fail("Should throw class not found exception");
		}
		catch (Throwable e) {}		
		
		// Specify classpath
		
		Class<?> h = Class.forName("com.ikanow.aleph2.test.example.ExampleHarvestTechnology", true, res.success().getClass().getClassLoader());
		assertEquals("com.ikanow.aleph2.test.example.ExampleHarvestTechnology", h.getName());
		
		
	}
	
	@Test
	public void test_classloader_systemClasspathCase() throws ClassNotFoundException {
		
		//(so it's already on the classpath)
		@SuppressWarnings("unused")
		IAnalyticsTechnologyModule x = new com.ikanow.aleph2.analytics.storm.services.MockStormAnalyticTechnologyService();
		
		// TEst some standard operations:
		
		final Validation<BasicMessageBean, Tuple2<IAnalyticsTechnologyModule, ClassLoader>> res = ClassloaderUtils.getFromCustomClasspath_withClassloader(IAnalyticsTechnologyModule.class, 
				"com.ikanow.aleph2.analytics.storm.services.MockStormAnalyticTechnologyService",
				Optional.empty(),
				Arrays.asList("file:misc_test_assets/simple-analytics-example.jar"),
				"test",
				"test");
		
		assertTrue("Should get module: " + res.validation(f->f.message(), s->""), res.isSuccess());
		
		// Check can't find example harvest technology in classpath
		
		ClassLoader class_loader = 
				res.success()._2()
				;
		
		try {
			Class.forName("com.ikanow.aleph2.test.example.ExampleHarvestTechnology");
			fail("Should throw class not found exception");
		}
		catch (Throwable e) {}
		
		final ClassLoader saved_current_classloader = Thread.currentThread().getContextClassLoader();		
		try {			
			Thread.currentThread().setContextClassLoader(class_loader);
			
			try {
				// This _doesn't_ work (defaults to system classloader):
				Class.forName("com.ikanow.aleph2.test.example.ExampleHarvestTechnology");
				fail("Should still throw class not found exception");
			}
			catch (Throwable t) {}
			
			Class<?> h = Class.forName("com.ikanow.aleph2.test.example.ExampleHarvestTechnology", true, Thread.currentThread().getContextClassLoader());
			assertEquals("com.ikanow.aleph2.test.example.ExampleHarvestTechnology", h.getName());
		}
		finally {
			Thread.currentThread().setContextClassLoader(saved_current_classloader);
		}
		// Back to original classpath
		try {
			Class.forName("com.ikanow.aleph2.test.example.ExampleHarvestTechnology");
			fail("Should throw class not found exception");
		}
		catch (Throwable e) {}		
		
		// Specify classpath
		
		Class<?> h = Class.forName("com.ikanow.aleph2.test.example.ExampleHarvestTechnology", true, class_loader);
		assertEquals("com.ikanow.aleph2.test.example.ExampleHarvestTechnology", h.getName());
		
		
	}
	
	////////////////////////////////////////////////////////////////////////////////////
	
	// UTILS
	
	protected DataBucketBean createBucket(final String analytic_tech_id) {
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
															.with(AnalyticThreadJobBean::analytic_technology_name_or_id, analytic_tech_id)
														.done().get()
														)
												)
									.done().get()
									)
							.done().get();
	}
	protected DataBucketBean createBatchBucket(final String analytic_tech_id) {
		// (Add streaming logic outside this via clone() - see cacheJars)
		return BeanTemplateUtils.build(DataBucketBean.class)
							.with(DataBucketBean::_id, "test1")
							.with(DataBucketBean::owner_id, "person_id")
							.with(DataBucketBean::full_name, "/test/path/")
							.with(DataBucketBean::master_enrichment_type, MasterEnrichmentType.batch)
							.with(DataBucketBean::analytic_thread,
									BeanTemplateUtils.build(AnalyticThreadBean.class)
										.with(AnalyticThreadBean::jobs,
												Arrays.asList(
														BeanTemplateUtils.build(AnalyticThreadJobBean.class)
															.with(AnalyticThreadJobBean::analytic_technology_name_or_id, analytic_tech_id + "_1")
															.with(AnalyticThreadJobBean::name, analytic_tech_id + "_1")
															.with(AnalyticThreadJobBean::analytic_type, MasterEnrichmentType.batch)
															.with(AnalyticThreadJobBean::dependencies, null) // (null - counts as none)
														.done().get()
														,
														BeanTemplateUtils.build(AnalyticThreadJobBean.class)
															.with(AnalyticThreadJobBean::analytic_technology_name_or_id, analytic_tech_id + "_2")
															.with(AnalyticThreadJobBean::name, analytic_tech_id + "_2")
															.with(AnalyticThreadJobBean::analytic_type, MasterEnrichmentType.batch)
															.with(AnalyticThreadJobBean::dependencies, Arrays.asList("test")) //dependency ... will be ignored)
														.done().get()
														,
														BeanTemplateUtils.build(AnalyticThreadJobBean.class)
															.with(AnalyticThreadJobBean::analytic_technology_name_or_id, analytic_tech_id + "_3")
															.with(AnalyticThreadJobBean::name, analytic_tech_id + "_3")
															.with(AnalyticThreadJobBean::analytic_type, MasterEnrichmentType.batch)
															.with(AnalyticThreadJobBean::dependencies, Arrays.asList()) // (empty - counts as none)
														.done().get()
														)
												)
									.done().get()
									)
							.done().get();
	}
	
	protected List<SharedLibraryBean> createSharedLibraryBeans_analytics(Path path1, Path path2) {
		final SharedLibraryBean lib_element = BeanTemplateUtils.build(SharedLibraryBean.class)
				.with(SharedLibraryBean::_id, "test_tech_id_analytics")
				.with(SharedLibraryBean::path_name, path1.toString())
				.with(SharedLibraryBean::misc_entry_point, "com.ikanow.aleph2.test.example.ExampleAnalyticsTechnology")
				.done().get();

		final SharedLibraryBean lib_element2 = BeanTemplateUtils.build(SharedLibraryBean.class)
		.with(SharedLibraryBean::_id, "test_module_id")
		.with(SharedLibraryBean::path_name, path2.toString())
		.done().get();

		final SharedLibraryBean lib_element3 = BeanTemplateUtils.build(SharedLibraryBean.class)
		.with(SharedLibraryBean::_id, "failtest")
		.with(SharedLibraryBean::path_name, "/not_exist/here.fghgjhgjhg")
		.done().get();

		return Arrays.asList(lib_element, lib_element2, lib_element3);		
	}
	
	protected List<SharedLibraryBean> createSharedLibraryBeans_batch(Path path1, Path path2) {
		final SharedLibraryBean lib_element = BeanTemplateUtils.build(SharedLibraryBean.class)
			.with(SharedLibraryBean::_id, "test_tech_id_batch")
			.with(SharedLibraryBean::path_name, path1.toString())
			.with(SharedLibraryBean::misc_entry_point, "com.ikanow.aleph2.analytics.hadoop.assets.BePassthroughModule")
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
				.with(SharedLibraryBean::_id, "test_tech_id_batch_fail")
				.with(SharedLibraryBean::path_name, path1.toString())
				.with(SharedLibraryBean::batch_enrichment_entry_point, "com.ikanow.aleph2.test.example.ExampleBatchTopology")
				.done().get();
		
		return Arrays.asList(lib_element, lib_element2, lib_element3, lib_element4);
	}
	
	
	protected List<SharedLibraryBean> createSharedLibraryBeans_streaming(Path path1, Path path2) {
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

	////////////////////////////////////////////////////////////////////////////////////
	
	// ACTOR UTILS
	
	public static class TestActor_Counter extends UntypedActor {
		
		public static AtomicInteger msg_counter = new AtomicInteger(0);
		public static AtomicInteger job_counter = new AtomicInteger(0);
		public static ConcurrentHashMap<JobMessageType, Integer> message_types = new ConcurrentHashMap<>();
		
		public static synchronized void reset() {
			_logger.info("Reset counter");
			msg_counter.set(0);
			job_counter.set(0);
			message_types.clear();
		}
		
		public TestActor_Counter(String uuid) {
			this.uuid = uuid;
		}
		private final String uuid;
		@Override
		public void onReceive(Object arg0) throws Exception {
			_logger.info("Count from: " + uuid + ": " + arg0.getClass().getSimpleName());
						
			synchronized (TestActor_Counter.class) {
				if (arg0 instanceof AnalyticTriggerMessage) {
					final AnalyticTriggerMessage msg = (AnalyticTriggerMessage) arg0;
	
					if (msg.bucket_action_message() instanceof BucketActionMessage.BucketActionAnalyticJobMessage) {
						final BucketActionMessage.BucketActionAnalyticJobMessage fwd_msg = (BucketActionMessage.BucketActionAnalyticJobMessage) msg.bucket_action_message();
					
						msg_counter.incrementAndGet();
						job_counter.addAndGet(Optional.ofNullable(fwd_msg.jobs()).map(l -> l.size()).orElse(0));
						message_types.merge(fwd_msg.type(), 1, (x, y) -> x + y);
					}
					else _logger.warn("Received invalid trigger message: " + msg.bucket_action_message());
	
				}
			}
		}		
	}

	

}
