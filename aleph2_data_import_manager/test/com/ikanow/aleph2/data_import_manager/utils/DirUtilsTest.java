package com.ikanow.aleph2.data_import_manager.utils;

import static org.junit.Assert.*;

import java.io.File;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_import_manager.services.DataImportActorContext;
import com.ikanow.aleph2.data_import_manager.services.GeneralInformationService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

public class DirUtilsTest {
    private static final Logger logger = LogManager.getLogger(DirUtilsTest.class);

	@Inject 
	protected IServiceContext _service_context = null;
	
	protected DataImportActorContext actor_context;
	
	protected Config config = null;
	protected FileContext fileContext = null;
	@Before
	public void setupDependencies() throws Exception {
		if (null != _service_context) {
			return;
		}
		
		final String temp_dir = System.getProperty("java.io.tmpdir") + File.separator;
		
		// OK we're going to use guice, it was too painful doing this by hand...				
		config = ConfigFactory.parseReader(new InputStreamReader(this.getClass().getResourceAsStream("/test_data_import_manager.properties")))
							.withValue("globals.local_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
							.withValue("globals.local_cached_jar_dir", ConfigValueFactory.fromAnyRef(temp_dir))
							.withValue("globals.distributed_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
							.withValue("globals.local_yarn_config_dir", ConfigValueFactory.fromAnyRef(temp_dir));
		
		Injector app_injector = ModuleUtils.createInjector(Arrays.asList(), Optional.of(config));	
		app_injector.injectMembers(this);
		actor_context = new DataImportActorContext(_service_context, new GeneralInformationService());
		app_injector.injectMembers(actor_context);

		// create folder structure if it does not exist for testing.
		
		fileContext = _service_context.getStorageService().getUnderlyingPlatformDriver(FileContext.class,Optional.empty()).get();
		logger.info("Root dir:"+actor_context.getGlobalProperties().distributed_root_dir());
		DirUtils.createDirectory(fileContext,actor_context.getGlobalProperties().distributed_root_dir()+"/data/misc/bucket1/managed_bucket/import/ready");
		DirUtils.createDirectory(fileContext,actor_context.getGlobalProperties().distributed_root_dir()+"/data/misc/bucket2/managed_bucket/import/ready");
		DirUtils.createDirectory(fileContext,actor_context.getGlobalProperties().distributed_root_dir()+"/data/misc/bucket3/managed_bucket/import/ready");
		DirUtils.createDirectory(fileContext,actor_context.getGlobalProperties().distributed_root_dir()+"/data/misc/bucket_parent/bucket4/managed_bucket/import/ready");
		DirUtils.createDirectory(fileContext,actor_context.getGlobalProperties().distributed_root_dir()+"/data/misc/bucket_parent/onemore/bucket5/managed_bucket/import/ready");
}
	
	@Test
	public void testFindOneSubdirectory(){
		Path start = new Path(actor_context.getGlobalProperties().distributed_root_dir()+"/data/");
		Path p = DirUtils.findOneSubdirectory(fileContext, start, "managed_bucket");
		assertNotNull(p);

	}

	@Test
	public void testFindAllSubdirectory(){
		Path start = new Path(actor_context.getGlobalProperties().distributed_root_dir()+"/data/");
		
		List<Path> paths = new ArrayList<Path>();
		DirUtils.findAllSubdirectories(paths, fileContext, start, "managed_bucket",false);
		assertEquals(5,paths.size());

	}

	
}
