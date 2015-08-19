package com.ikanow.aleph2.core.shared.utils;

import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.storage_service_hdfs.services.MockHdfsStorageService;
import com.typesafe.config.Config;

public class DirUtilsTest {
    private static final Logger logger = LogManager.getLogger(DirUtilsTest.class);

	protected String _temp_dir;
	
	protected Config config = null;
	protected FileContext fileContext = null;
	@Before
	public void setupDependencies() throws Exception {
		_temp_dir = System.getProperty("java.io.tmpdir") + File.separator;
		
		GlobalPropertiesBean globals = new GlobalPropertiesBean(_temp_dir, _temp_dir, _temp_dir, _temp_dir);
		
		// create folder structure if it does not exist for testing.
		
		IStorageService storage_service = new MockHdfsStorageService(globals);
		
		fileContext = storage_service.getUnderlyingPlatformDriver(FileContext.class,Optional.empty()).get();
		logger.info("Root dir:"+_temp_dir);
		DirUtils.createDirectory(fileContext,_temp_dir+"/data/misc/bucket1/managed_bucket/import/ready");
		DirUtils.createDirectory(fileContext,_temp_dir+"/data/misc/bucket2/managed_bucket/import/ready");
		DirUtils.createDirectory(fileContext,_temp_dir+"/data/misc/bucket3/managed_bucket/import/ready");
		DirUtils.createDirectory(fileContext,_temp_dir+"/data/misc/bucket_parent/bucket4/managed_bucket/import/ready");
		DirUtils.createDirectory(fileContext,_temp_dir+"/data/misc/bucket_parent/onemore/bucket5/managed_bucket/import/ready");
}
	
	@Test
	public void testFindOneSubdirectory(){
		Path start = new Path(_temp_dir+"/data/");
		Path p = DirUtils.findOneSubdirectory(fileContext, start, "managed_bucket");
		assertNotNull(p);

	}

	@Test
	public void testFindAllSubdirectory(){
		Path start = new Path(_temp_dir+"/data/");
		
		List<Path> paths = new ArrayList<Path>();
		DirUtils.findAllSubdirectories(paths, fileContext, start, "managed_bucket",false);
		assertEquals(5,paths.size());

	}

	
}
