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
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Before;
import org.junit.Test;

import scala.Tuple2;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_import_manager.harvest.utils.HarvestErrorUtils;
import com.ikanow.aleph2.data_import_manager.services.DataImportActorContext;
import com.ikanow.aleph2.data_import_manager.services.GeneralInformationService;
import com.ikanow.aleph2.data_import_manager.utils.JarCacheUtils;
import com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestTechnologyModule;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.HarvestControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import fj.data.Either;

public class TestDataBucketChangeActor {

	@Inject 
	protected IServiceContext _service_context;
	
	protected DataImportActorContext _actor_context;
	
	@Before
	public void setupDependencies() throws Exception {
		final String temp_dir = System.getProperty("java.io.tmpdir");
		
		// OK we're going to use guice, it was too painful doing this by hand...				
		Config config = ConfigFactory.parseReader(new InputStreamReader(this.getClass().getResourceAsStream("test_data_bucket_change.properties")))
							.withValue("globals.local_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
							.withValue("globals.local_cached_jar_dir", ConfigValueFactory.fromAnyRef(temp_dir))
							.withValue("globals.distributed_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
							.withValue("globals.local_yarn_config_dir", ConfigValueFactory.fromAnyRef(temp_dir));
		
		Injector app_injector = ModuleUtils.createInjector(Arrays.asList(), Optional.of(config));	
		app_injector.injectMembers(this);
		
		_actor_context = new DataImportActorContext(_service_context, new GeneralInformationService());
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
	
	@NonNull
	protected DataBucketBean createBucket(final @Nullable String harvest_tech_id) {
		return BeanTemplateUtils.build(DataBucketBean.class)
							.with(DataBucketBean::_id, "test1")
							.with(DataBucketBean::full_name, "/test/path/")
							.with(DataBucketBean::harvest_technology_name_or_id, harvest_tech_id)
							.done().get();
	}
	
	protected void resetFiles() {
		//TODO
	}
	
	@Test
	public void test_getHarvestTechnology() throws UnsupportedFileSystemException, InterruptedException, ExecutionException {
//		protected static Either<BasicMessageBean, IHarvestTechnologyModule> getHarvestTechnology(
//				final @NonNull DataBucketBean bucket, 
//				boolean harvest_tech_only,
//				final @NonNull BucketActionMessage m, 
//				final @NonNull String source,
//				final @NonNull Either<BasicMessageBean, Map<String, Tuple2<SharedLibraryBean, String>>> err_or_libs // "pipeline element"
//				)
		

		final DataBucketBean bucket = createBucket("test_tech_id");		
		
		final String pathname = System.getProperty("user.dir") + "/simple-harvest-example.jar";
		final Path path = new Path(pathname);
		final Path path2 = FileContext.getLocalFSFileContext().makeQualified(path);		
		
		final SharedLibraryBean lib_element = BeanTemplateUtils.build(SharedLibraryBean.class)
												.with(SharedLibraryBean::_id, "test_tech_id")
												.with(SharedLibraryBean::path_name, path2.toString())
												.with(SharedLibraryBean::misc_entry_point, "com.ikanow.aleph2.test.example.ExampleHarvestTechnology")
												.done().get();

		final SharedLibraryBean lib_element2 = BeanTemplateUtils.build(SharedLibraryBean.class)
				.with(SharedLibraryBean::_id, "test_module_id")
				.with(SharedLibraryBean::path_name, path2.toString())
				.done().get();
		
		// 1) Check - if called with an error, then just passes that error along
		
		final BasicMessageBean error = HarvestErrorUtils.buildErrorMessage("test_source", "test_message", "test_error");
		
		final Either<BasicMessageBean, IHarvestTechnologyModule> test1 = DataBucketChangeActor.getHarvestTechnology(bucket, true, 
				new BucketActionMessage.BucketActionOfferMessage(bucket), "test_source2", Either.left(error));
		
		assertTrue("Got error back", test1.isLeft());
		assertEquals("test_source", test1.left().value().source());
		assertEquals("test_message", test1.left().value().command());
		assertEquals("test_error", test1.left().value().message());
		
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
		assertEquals(ErrorUtils.get(HarvestErrorUtils.SHARED_LIBRARY_NAME_NOT_FOUND, bucket.full_name(), bucket.full_name()),
						test2a.left().value().message());
		
		final Either<BasicMessageBean, IHarvestTechnologyModule> test2b = DataBucketChangeActor.getHarvestTechnology(
				BeanTemplateUtils.clone(bucket).with(DataBucketBean::harvest_technology_name_or_id,  "test_tech_id_2b").done(), 
				true, 
				new BucketActionMessage.BucketActionOfferMessage(bucket), "test_source2b", 
				Either.right(test2_input));

		assertTrue("Got error back", test2b.isLeft());
		assertEquals("test_source2b", test2b.left().value().source());
		assertEquals("BucketActionOfferMessage", test2b.left().value().command());
		assertEquals(ErrorUtils.get(HarvestErrorUtils.SHARED_LIBRARY_NAME_NOT_FOUND, bucket.full_name(), bucket.full_name()),
						test2a.left().value().message());
		
		// 3) OK now it will actually do something 
		
		//////////////////////////////////////////////////////

		final String java_name = _service_context.getGlobalProperties().local_cached_jar_dir() + File.separator + "test_tech_id.cache.jar";
		
		System.out.println("Needed to delete locally cached file? " + java_name + ": " + new File(java_name).delete());		
		
		// Requires that the file has already been cached:
		final Either<BasicMessageBean, String> cached_file = JarCacheUtils.getCachedJar(_service_context.getGlobalProperties().local_cached_jar_dir(), 
				lib_element, 
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
							lib_element,
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
		assertEquals(lib_element.misc_entry_point(), test3.right().value().getClass().getName());
		
		// Now check with the "not just the harvest tech" flag set
		
		final String java_name2 = _service_context.getGlobalProperties().local_cached_jar_dir() + File.separator + "test_module_id.cache.jar";
		
		System.out.println("Needed to delete locally cached file? " + java_name2 + ": " + new File(java_name2).delete());		
		
		// Requires that the file has already been cached:
		final Either<BasicMessageBean, String> cached_file2 = JarCacheUtils.getCachedJar(_service_context.getGlobalProperties().local_cached_jar_dir(), 
				lib_element2, 
				_service_context.getStorageService(),
				"test3b", "test3b").get();
		
		if (cached_file2.isLeft()) {
			fail("About to crash with: " + cached_file2.left().value().message());
		}		
		
		assertTrue("The cached file exists: " + java_name, new File(java_name2).exists());				
		
		final ImmutableMap<String, Tuple2<SharedLibraryBean, String>> test3b_input = 
				ImmutableMap.<String, Tuple2<SharedLibraryBean, String>>builder()
					.put("test_tech_id", Tuples._2T(
							lib_element,
							cached_file.right().value()))
					.put("test_module_id", Tuples._2T(
							lib_element2,
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
		assertEquals(lib_element.misc_entry_point(), test3b.right().value().getClass().getName());
		
	}
}
