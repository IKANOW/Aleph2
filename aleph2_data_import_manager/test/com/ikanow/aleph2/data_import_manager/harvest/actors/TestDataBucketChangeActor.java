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
package com.ikanow.aleph2.data_import_manager.harvest.actors;

import static org.junit.Assert.*;

import java.io.File;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
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
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Before;
import org.junit.Test;

import scala.Tuple2;
import scala.concurrent.duration.Duration;
import akka.actor.ActorRef;
import akka.actor.Inbox;
import akka.actor.Props;
import akka.pattern.Patterns;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_import_manager.harvest.utils.HarvestErrorUtils;
import com.ikanow.aleph2.data_import_manager.services.DataImportActorContext;
import com.ikanow.aleph2.data_import_manager.services.GeneralInformationService;
import com.ikanow.aleph2.data_import_manager.utils.ClassloaderUtils;
import com.ikanow.aleph2.data_import_manager.utils.JarCacheUtils;
import com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestTechnologyModule;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.HarvestControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.FutureUtils;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage.BucketActionEventBusWrapper;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage;
import com.ikanow.aleph2.management_db.services.LocalBucketActionMessageBus;
import com.ikanow.aleph2.management_db.services.ManagementDbActorContext;
import com.ikanow.aleph2.management_db.utils.ActorUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import fj.data.Either;

public class TestDataBucketChangeActor {

	@Inject 
	protected IServiceContext _service_context = null;
	
	protected DataImportActorContext _actor_context;
	protected ManagementDbActorContext _db_actor_context;
	
	@Before
	public void setupDependencies() throws Exception {
		if (null != _service_context) {
			return;
		}
		
		final String temp_dir = System.getProperty("java.io.tmpdir");
		
		// OK we're going to use guice, it was too painful doing this by hand...				
		Config config = ConfigFactory.parseReader(new InputStreamReader(this.getClass().getResourceAsStream("test_data_bucket_change.properties")))
							.withValue("globals.local_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
							.withValue("globals.local_cached_jar_dir", ConfigValueFactory.fromAnyRef(temp_dir))
							.withValue("globals.distributed_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
							.withValue("globals.local_yarn_config_dir", ConfigValueFactory.fromAnyRef(temp_dir));
		
		Injector app_injector = ModuleUtils.createInjector(Arrays.asList(), Optional.of(config));	
		app_injector.injectMembers(this);
		
		_db_actor_context = new ManagementDbActorContext(_service_context, new LocalBucketActionMessageBus());
		
		_actor_context = new DataImportActorContext(_service_context, new GeneralInformationService());
		app_injector.injectMembers(_actor_context);
		
		// Have to do this in order for the underlying management db to live...		
		_service_context.getCoreManagementDbService();
	}
	
	@Test
	public void testSetup() {
		final String temp_dir = System.getProperty("java.io.tmpdir");
		
		assertTrue("setup completed - service context", _service_context != null);
		assertTrue("setup completed - services", _service_context.getCoreManagementDbService() != null);
		assertEquals(temp_dir, _service_context.getGlobalProperties().local_root_dir());
		
		if (File.separator.equals("\\")) { // windows mode!
			assertTrue("WINDOWS MODE: hadoop home needs to be set (use -Dhadoop.home.dir={HADOOP_HOME} in JAVA_OPTS)", null != System.getProperty("hadoop.home.dir"));
			assertTrue("WINDOWS MODE: hadoop home needs to exist: " + System.getProperty("hadoop.home.dir"), null != System.getProperty("hadoop.home.dir"));
		}
	}
	
	protected DataBucketBean createBucket(final @Nullable String harvest_tech_id) {
		return BeanTemplateUtils.build(DataBucketBean.class)
							.with(DataBucketBean::_id, "test1")
							.with(DataBucketBean::full_name, "/test/path/")
							.with(DataBucketBean::harvest_technology_name_or_id, harvest_tech_id)
							.done().get();
	}

	protected List<SharedLibraryBean> createSharedLibraryBeans(Path path1, Path path2) {
		final SharedLibraryBean lib_element = BeanTemplateUtils.build(SharedLibraryBean.class)
				.with(SharedLibraryBean::_id, "test_tech_id")
				.with(SharedLibraryBean::path_name, path1.toString())
				.with(SharedLibraryBean::misc_entry_point, "com.ikanow.aleph2.test.example.ExampleHarvestTechnology")
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
	
	@Test
	public void test_getHarvestTechnology() throws UnsupportedFileSystemException, InterruptedException, ExecutionException {
		final DataBucketBean bucket = createBucket("test_tech_id");		
		
		final String pathname1 = System.getProperty("user.dir") + "/misc_test_assets/simple-harvest-example.jar";
		final Path path1 = FileContext.getLocalFSFileContext().makeQualified(new Path(pathname1));		
		final String pathname2 = System.getProperty("user.dir") + "/misc_test_assets/simple-harvest-example2.jar";
		final Path path2 = FileContext.getLocalFSFileContext().makeQualified(new Path(pathname2));		
		
		List<SharedLibraryBean> lib_elements = createSharedLibraryBeans(path1, path2);
		
		//////////////////////////////////////////////////////

		// 1) Check - if called with an error, then just passes that error along
		
		final BasicMessageBean error = HarvestErrorUtils.buildErrorMessage("test_source", "test_message", "test_error");
		
		final Either<BasicMessageBean, IHarvestTechnologyModule> test1 = DataBucketChangeActor.getHarvestTechnology(bucket, true, 
				new BucketActionMessage.BucketActionOfferMessage(bucket), "test_source2", Either.left(error));
		
		assertTrue("Got error back", test1.isLeft());
		assertEquals("test_source", test1.left().value().source());
		assertEquals("test_message", test1.left().value().command());
		assertEquals("test_error", test1.left().value().message());
		
		//////////////////////////////////////////////////////

		// 2) Check the error handling inside getHarvestTechnology
		
		final ImmutableMap<String, Tuple2<SharedLibraryBean, String>> test2_input = 
				ImmutableMap.<String, Tuple2<SharedLibraryBean, String>>builder()
					.put("test_tech_id_2b", Tuples._2T(null, null))
					.build();

		final Either<BasicMessageBean, IHarvestTechnologyModule> test2a = DataBucketChangeActor.getHarvestTechnology(
				BeanTemplateUtils.clone(bucket).with(DataBucketBean::harvest_technology_name_or_id,  "test_tech_id_2a").done(), 
				true, 
				new BucketActionMessage.BucketActionOfferMessage(bucket), "test_source2a", 
				Either.right(test2_input));

		assertTrue("Got error back", test2a.isLeft());
		assertEquals("test_source2a", test2a.left().value().source());
		assertEquals("BucketActionOfferMessage", test2a.left().value().command());
		assertEquals(ErrorUtils.get(HarvestErrorUtils.SHARED_LIBRARY_NAME_NOT_FOUND, bucket.full_name(), "test_tech_id_2a"), // (cloned bucket above)
						test2a.left().value().message());
		
		final Either<BasicMessageBean, IHarvestTechnologyModule> test2b = DataBucketChangeActor.getHarvestTechnology(
				BeanTemplateUtils.clone(bucket).with(DataBucketBean::harvest_technology_name_or_id,  "test_tech_id_2b").done(), 
				true, 
				new BucketActionMessage.BucketActionOfferMessage(bucket), "test_source2b", 
				Either.right(test2_input));

		assertTrue("Got error back", test2b.isLeft());
		assertEquals("test_source2b", test2b.left().value().source());
		assertEquals("BucketActionOfferMessage", test2b.left().value().command());
		assertEquals(ErrorUtils.get(HarvestErrorUtils.SHARED_LIBRARY_NAME_NOT_FOUND, bucket.full_name(), "test_tech_id_2a"), // (cloned bucket above)
						test2a.left().value().message());
		
		//////////////////////////////////////////////////////

		// 3) OK now it will actually do something 
		
		final String java_name = _service_context.getGlobalProperties().local_cached_jar_dir() + File.separator + "test_tech_id.cache.jar";
		
		System.out.println("Needed to delete locally cached file? " + java_name + ": " + new File(java_name).delete());		
		
		// Requires that the file has already been cached:
		final Either<BasicMessageBean, String> cached_file = JarCacheUtils.getCachedJar(_service_context.getGlobalProperties().local_cached_jar_dir(), 
				lib_elements.get(0), 
				_service_context.getStorageService(),
				"test3", "test3").get();
		
		if (cached_file.isLeft()) {
			fail("About to crash with: " + cached_file.left().value().message());
		}		
		
		assertTrue("The cached file exists: " + java_name, new File(java_name).exists());
		
		// OK the setup is done and validated now actually test the underlying call:
		
		final ImmutableMap<String, Tuple2<SharedLibraryBean, String>> test3_input = 
				ImmutableMap.<String, Tuple2<SharedLibraryBean, String>>builder()
					.put("test_tech_id", Tuples._2T(
							lib_elements.get(0),
							cached_file.right().value()))
					.build();		
		
		final Either<BasicMessageBean, IHarvestTechnologyModule> test3 = DataBucketChangeActor.getHarvestTechnology(
				BeanTemplateUtils.clone(bucket).with(DataBucketBean::harvest_technology_name_or_id,  "test_tech_id").done(), 
				true, 
				new BucketActionMessage.BucketActionOfferMessage(bucket), "test_source3", 
				Either.right(test3_input));

		if (test3.isLeft()) {
			fail("About to crash with: " + test3.left().value().message());
		}		
		assertTrue("getHarvestTechnology call succeeded", test3.isRight());
		assertTrue("harvest tech created: ", test3.right().value() != null);
		assertEquals(lib_elements.get(0).misc_entry_point(), test3.right().value().getClass().getName());
		
		// Now check with the "not just the harvest tech" flag set
		
		final String java_name2 = _service_context.getGlobalProperties().local_cached_jar_dir() + File.separator + "test_module_id.cache.jar";
		
		System.out.println("Needed to delete locally cached file? " + java_name2 + ": " + new File(java_name2).delete());		
		
		// Requires that the file has already been cached:
		final Either<BasicMessageBean, String> cached_file2 = JarCacheUtils.getCachedJar(_service_context.getGlobalProperties().local_cached_jar_dir(), 
				lib_elements.get(1), 
				_service_context.getStorageService(),
				"test3b", "test3b").get();
		
		if (cached_file2.isLeft()) {
			fail("About to crash with: " + cached_file2.left().value().message());
		}		
		
		assertTrue("The cached file exists: " + java_name, new File(java_name2).exists());				
		
		final ImmutableMap<String, Tuple2<SharedLibraryBean, String>> test3b_input = 
				ImmutableMap.<String, Tuple2<SharedLibraryBean, String>>builder()
					.put("test_tech_id", Tuples._2T(
							lib_elements.get(0),
							cached_file.right().value()))
					.put("test_module_id", Tuples._2T(
							lib_elements.get(1),
							cached_file.right().value()))
					.build();		
		
		final HarvestControlMetadataBean harvest_module = new HarvestControlMetadataBean(
				"test_tech_name", true, Arrays.asList("test_module_id"), null
				);
		
		final Either<BasicMessageBean, IHarvestTechnologyModule> test3b = DataBucketChangeActor.getHarvestTechnology(
				BeanTemplateUtils.clone(bucket)
					.with(DataBucketBean::harvest_technology_name_or_id,  "test_tech_id")
					.with(DataBucketBean::harvest_configs, Arrays.asList(harvest_module))
					.done(), 
				false, 
				new BucketActionMessage.BucketActionOfferMessage(bucket), "test_source3b", 
				Either.right(test3b_input));

		if (test3b.isLeft()) {
			fail("About to crash with: " + test3b.left().value().message());
		}		
		assertTrue("getHarvestTechnology call succeeded", test3b.isRight());
		assertTrue("harvest tech created: ", test3b.right().value() != null);
		assertEquals(lib_elements.get(0).misc_entry_point(), test3b.right().value().getClass().getName());		
	}
	
	@Test
	public void test_talkToHarvester() throws InterruptedException, ExecutionException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		final DataBucketBean bucket = createBucket("test_tech_id");		
		
		// Get the harvest tech module standalone ("in app" way is covered above)
		
		final Either<BasicMessageBean, IHarvestTechnologyModule> ret_val = 
				ClassloaderUtils.getFromCustomClasspath(IHarvestTechnologyModule.class, 
						"com.ikanow.aleph2.test.example.ExampleHarvestTechnology", 
						Optional.of(new File(System.getProperty("user.dir") + "\\misc_test_assets\\simple-harvest-example.jar").getAbsoluteFile().toURI().toString()),
						Collections.emptyList(), "test1", "test");						
		
		assertTrue("getHarvestTechnology call succeeded", ret_val.isRight());
		assertTrue("harvest tech created: ", ret_val.right().value() != null);
		
		final IHarvestTechnologyModule harvest_tech = ret_val.right().value();
		
		// Test 1: pass along errors:
		
		final BasicMessageBean error = HarvestErrorUtils.buildErrorMessage("test_source", "test_message", "test_error");
		
		final CompletableFuture<BucketActionReplyMessage> test1 = DataBucketChangeActor.talkToHarvester(
				bucket, new BucketActionMessage.DeleteBucketActionMessage(bucket, Collections.emptySet()), "test1", _actor_context.getNewHarvestContext(), 
				Either.left(error));

		assertEquals(BucketActionReplyMessage.BucketActionHandlerMessage.class, test1.get().getClass());
		final BucketActionReplyMessage.BucketActionHandlerMessage test1err = (BucketActionReplyMessage.BucketActionHandlerMessage) test1.get();
		assertEquals(false, test1err.reply().success());
		assertEquals("test_source", test1err.reply().source());
		assertEquals("test_message", test1err.reply().command());
		assertEquals("test_error", test1err.reply().message());

		
		// Test 2: offer
		
		final BucketActionMessage.BucketActionOfferMessage offer = new BucketActionMessage.BucketActionOfferMessage(bucket);
		
		final CompletableFuture<BucketActionReplyMessage> test2 = DataBucketChangeActor.talkToHarvester(
				bucket, offer, "test2", _actor_context.getNewHarvestContext(), 
				Either.right(harvest_tech));		
		
		assertEquals(BucketActionReplyMessage.BucketActionWillAcceptMessage.class, test2.get().getClass());
		final BucketActionReplyMessage.BucketActionWillAcceptMessage test2_reply = (BucketActionReplyMessage.BucketActionWillAcceptMessage) test2.get();
		assertEquals("test2", test2_reply.source());
		
		// Test 3: delete
		
		final BucketActionMessage.DeleteBucketActionMessage delete = new BucketActionMessage.DeleteBucketActionMessage(bucket, Collections.emptySet());
		
		final CompletableFuture<BucketActionReplyMessage> test3 = DataBucketChangeActor.talkToHarvester(
				bucket, delete, "test3", _actor_context.getNewHarvestContext(), 
				Either.right(harvest_tech));		
		
		assertEquals(BucketActionReplyMessage.BucketActionHandlerMessage.class, test3.get().getClass());
		final BucketActionReplyMessage.BucketActionHandlerMessage test3_reply = (BucketActionReplyMessage.BucketActionHandlerMessage) test3.get();
		assertEquals("test3", test3_reply.source());
		assertEquals("called onDelete", test3_reply.reply().message());
		assertEquals(true, test3_reply.reply().success());
		
		// Test 4: new
		
		final BucketActionMessage.NewBucketActionMessage create = new BucketActionMessage.NewBucketActionMessage(bucket, true);
		
		final CompletableFuture<BucketActionReplyMessage> test4 = DataBucketChangeActor.talkToHarvester(
				bucket, create, "test4", _actor_context.getNewHarvestContext(), 
				Either.right(harvest_tech));		
		
		assertEquals(BucketActionReplyMessage.BucketActionHandlerMessage.class, test4.get().getClass());
		final BucketActionReplyMessage.BucketActionHandlerMessage test4_reply = (BucketActionReplyMessage.BucketActionHandlerMessage) test4.get();
		assertEquals("test4", test4_reply.source());
		assertEquals("called onNewSource: true", test4_reply.reply().message());
		assertEquals(true, test4_reply.reply().success());
		
		// Test 5: update
		
		final BucketActionMessage.UpdateBucketActionMessage update = new BucketActionMessage.UpdateBucketActionMessage(bucket, true, bucket, Collections.emptySet());
		
		final CompletableFuture<BucketActionReplyMessage> test5 = DataBucketChangeActor.talkToHarvester(
				bucket, update, "test5", _actor_context.getNewHarvestContext(), 
				Either.right(harvest_tech));		
		
		assertEquals(BucketActionReplyMessage.BucketActionHandlerMessage.class, test5.get().getClass());
		final BucketActionReplyMessage.BucketActionHandlerMessage test5_reply = (BucketActionReplyMessage.BucketActionHandlerMessage) test5.get();
		assertEquals("test5", test5_reply.source());
		assertEquals("called onUpdatedSource true", test5_reply.reply().message());
		assertEquals(true, test5_reply.reply().success());
		
		// Test 6: update state - (a) resume, (b) suspend
		
		final BucketActionMessage.UpdateBucketStateActionMessage update_state = new BucketActionMessage.UpdateBucketStateActionMessage(bucket, true, Collections.emptySet());
		
		final CompletableFuture<BucketActionReplyMessage> test6 = DataBucketChangeActor.talkToHarvester(
				bucket, update_state, "test6", _actor_context.getNewHarvestContext(), 
				Either.right(harvest_tech));		
		
		assertEquals(BucketActionReplyMessage.BucketActionHandlerMessage.class, test6.get().getClass());
		final BucketActionReplyMessage.BucketActionHandlerMessage test6_reply = (BucketActionReplyMessage.BucketActionHandlerMessage) test6.get();
		assertEquals("test6", test6_reply.source());
		assertEquals("called onSuspend", test6_reply.reply().message());
		assertEquals(true, test6_reply.reply().success());

		final BucketActionMessage.UpdateBucketStateActionMessage update_stateb = new BucketActionMessage.UpdateBucketStateActionMessage(bucket, false, Collections.emptySet());
		
		final CompletableFuture<BucketActionReplyMessage> test6b = DataBucketChangeActor.talkToHarvester(
				bucket, update_stateb, "test6b", _actor_context.getNewHarvestContext(), 
				Either.right(harvest_tech));		
		
		assertEquals(BucketActionReplyMessage.BucketActionHandlerMessage.class, test6b.get().getClass());
		final BucketActionReplyMessage.BucketActionHandlerMessage test6b_reply = (BucketActionReplyMessage.BucketActionHandlerMessage) test6b.get();
		assertEquals("test6b", test6b_reply.source());
		assertEquals("called onResume", test6b_reply.reply().message());
		assertEquals(true, test6b_reply.reply().success());
		
		// Test 7: unrecognized
		
		// Use reflection to create a "raw" BucketActionMessage
		final Constructor<BucketActionMessage> contructor = (Constructor<BucketActionMessage>) BucketActionMessage.class.getDeclaredConstructor(DataBucketBean.class);
		contructor.setAccessible(true);
		BucketActionMessage bad_msg = contructor.newInstance(bucket);

		final CompletableFuture<BucketActionReplyMessage> test7 = DataBucketChangeActor.talkToHarvester(
				bucket, bad_msg, "test7", _actor_context.getNewHarvestContext(), 
				Either.right(harvest_tech));		
		
		assertEquals(BucketActionReplyMessage.BucketActionHandlerMessage.class, test7.get().getClass());
		final BucketActionReplyMessage.BucketActionHandlerMessage test7_reply = (BucketActionReplyMessage.BucketActionHandlerMessage) test7.get();
		assertEquals(false, test7_reply.reply().success());
		assertEquals("test7", test7_reply.source());
		assertEquals("Message type BucketActionMessage not recognized for bucket /test/path/", test7_reply.reply().message());
	}
	
	@Test
	public void test_cacheJars() throws UnsupportedFileSystemException, InterruptedException, ExecutionException {
		try {
			// Preamble:
			// 0) Insert 2 library beans into the management db
			
			final DataBucketBean bucket = createBucket("test_tech_id");		
			
			final String pathname1 = System.getProperty("user.dir") + "/misc_test_assets/simple-harvest-example.jar";
			final Path path1 = FileContext.getLocalFSFileContext().makeQualified(new Path(pathname1));		
			final String pathname2 = System.getProperty("user.dir") + "/misc_test_assets/simple-harvest-example2.jar";
			final Path path2 = FileContext.getLocalFSFileContext().makeQualified(new Path(pathname2));		
			
			List<SharedLibraryBean> lib_elements = createSharedLibraryBeans(path1, path2);
	
			final IManagementDbService underlying_db = _service_context.getService(IManagementDbService.class, Optional.empty());
			final IManagementCrudService<SharedLibraryBean> library_crud = underlying_db.getSharedLibraryStore();
			library_crud.storeObjects(lib_elements).get();
			
			assertEquals(3L, (long)library_crud.countObjects().get());
			
			// 0b) Create the more complex bucket
			
			final HarvestControlMetadataBean harvest_module = new HarvestControlMetadataBean(
					"test_tech_name", true, Arrays.asList("test_module_id"), null
					);
			final DataBucketBean bucket2 = BeanTemplateUtils.clone(bucket)
								.with(DataBucketBean::harvest_technology_name_or_id,  "test_tech_id")
								.with(DataBucketBean::harvest_configs, Arrays.asList(harvest_module))
								.done();
			
			// 1) Normal operation
			
			CompletableFuture<Either<BasicMessageBean, Map<String, Tuple2<SharedLibraryBean, String>>>> reply_structure =
				DataBucketChangeActor.cacheJars(bucket, true, 
						_service_context.getCoreManagementDbService(), _service_context.getGlobalProperties(), _service_context.getStorageService(),
						"test1_source", "test1_command"
					);
			
			if (reply_structure.get().isLeft()) {
				fail("About to crash with: " + reply_structure.get().left().value().message());
			}		
			assertTrue("cacheJars should return valid reply", reply_structure.get().isRight());
		
			final Map<String, Tuple2<SharedLibraryBean, String>> reply_map = reply_structure.get().right().value();
			
			assertEquals(2L, reply_map.size()); // (harves tech only, one for name, one for id) 
			
			// 2) Normal operation - tech + module
			
			CompletableFuture<Either<BasicMessageBean, Map<String, Tuple2<SharedLibraryBean, String>>>> reply_structure2 =
					DataBucketChangeActor.cacheJars(bucket2, false, 
							_service_context.getCoreManagementDbService(), _service_context.getGlobalProperties(), _service_context.getStorageService(),
							"test2_source", "test2_command"
						);
				
			if (reply_structure2.get().isLeft()) {
				fail("About to crash with: " + reply_structure2.get().left().value().message());
			}		
			assertTrue("cacheJars should return valid reply", reply_structure2.get().isRight());
		
			final Map<String, Tuple2<SharedLibraryBean, String>> reply_map2 = reply_structure2.get().right().value();
			
			assertEquals(4L, reply_map2.size()); // (harves tech only, one for name, one for id)
				
			// 3) Couple of error cases:
			
			DataBucketBean bucket3 = BeanTemplateUtils.clone(bucket).with(DataBucketBean::harvest_technology_name_or_id, "failtest").done();
			
			CompletableFuture<Either<BasicMessageBean, Map<String, Tuple2<SharedLibraryBean, String>>>> reply_structure3 =
					DataBucketChangeActor.cacheJars(bucket3, false, 
							_service_context.getCoreManagementDbService(), _service_context.getGlobalProperties(), _service_context.getStorageService(),
							"test2_source", "test2_command"
						);
			
			assertTrue("cacheJars should return error", reply_structure3.get().isLeft());
		}
		catch (Exception e) {
			System.out.println(ErrorUtils.getLongForm("guice? {0}", e));
			throw e;
		}
	}
	
	@Test
	public void test_actor() throws UnsupportedFileSystemException, IllegalArgumentException, InterruptedException, ExecutionException, TimeoutException {		
		// Set up the DB
		// Preamble:
		// 0) Insert 2 library beans into the management db
		
		final DataBucketBean bucket = createBucket("test_tech_id");		
		
		final String pathname1 = System.getProperty("user.dir") + "/misc_test_assets/simple-harvest-example.jar";
		final Path path1 = FileContext.getLocalFSFileContext().makeQualified(new Path(pathname1));		
		final String pathname2 = System.getProperty("user.dir") + "/misc_test_assets/simple-harvest-example2.jar";
		final Path path2 = FileContext.getLocalFSFileContext().makeQualified(new Path(pathname2));		
		
		final List<SharedLibraryBean> lib_elements = createSharedLibraryBeans(path1, path2);

		final IManagementDbService underlying_db = _service_context.getService(IManagementDbService.class, Optional.empty());
		final IManagementCrudService<SharedLibraryBean> library_crud = underlying_db.getSharedLibraryStore();
		try {
			library_crud.storeObjects(lib_elements, true).get();
		}
		catch (Exception e) {} // (don't care as long as the assert below works)
		
		assertEquals(3L, (long)library_crud.countObjects().get());		
		assertEquals(3L, (long)_service_context.getCoreManagementDbService().getSharedLibraryStore().countObjects().get());
		
		// Create an actor:
		
		final ActorRef handler = _db_actor_context.getActorSystem().actorOf(Props.create(DataBucketChangeActor.class), "test_host");
		_db_actor_context.getBucketActionMessageBus().subscribe(handler, ActorUtils.BUCKET_ACTION_EVENT_BUS);

		// create the inbox:
		final Inbox inbox = Inbox.create(_actor_context.getActorSystem());		
		
		// Send it some messages:
		
		// 1) A message that it will ignore because it's the wrong type
		
		inbox.send(handler, "IGNOREME");
		try {			
			inbox.receive(Duration.create(1L, TimeUnit.SECONDS));
			fail("should have timed out");
		}
		catch (Exception e) {
			assertEquals(TimeoutException.class, e.getClass());
		}
		
		// 2) A message that it will ignore because it's not for this actor

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
		
		// 3) A message that it will process because it's a broadcast

		final BucketActionMessage.BucketActionOfferMessage broadcast =
				new BucketActionMessage.BucketActionOfferMessage(bucket);
		
		_db_actor_context.getBucketActionMessageBus().publish(new BucketActionEventBusWrapper(inbox.getRef(), broadcast));
		
		final Object msg = inbox.receive(Duration.create(5L, TimeUnit.SECONDS));
	
		assertEquals(BucketActionReplyMessage.BucketActionWillAcceptMessage.class, msg.getClass());
		
		// 4) A message that it will process because it's for this actor
		
		final BucketActionMessage.UpdateBucketStateActionMessage suspend =
				new BucketActionMessage.UpdateBucketStateActionMessage(bucket, true, new HashSet<String>(Arrays.asList(_actor_context.getInformationService().getHostname())));
		
		final CompletableFuture<BucketActionReplyMessage> reply4 = FutureUtils.wrap(Patterns.ask(handler, suspend, 5000L));
		final BucketActionReplyMessage msg4 = reply4.get();
	
		assertEquals(BucketActionReplyMessage.BucketActionHandlerMessage.class, msg4.getClass());
		final BucketActionReplyMessage.BucketActionHandlerMessage msg4b =  (BucketActionReplyMessage.BucketActionHandlerMessage) msg4;
		
		assertEquals(true, msg4b.reply().success());
		assertEquals("called onSuspend", msg4b.reply().message());
	}
}
