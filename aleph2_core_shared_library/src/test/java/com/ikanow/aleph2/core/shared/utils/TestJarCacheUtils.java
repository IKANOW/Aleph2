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
package com.ikanow.aleph2.core.shared.utils;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Date;
import java.util.EnumSet;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.security.AccessControlException;
import org.junit.Before;
import org.junit.Test;

import com.ikanow.aleph2.core.shared.test_services.MockStorageService;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;

import fj.data.Validation;

public class TestJarCacheUtils {

	MockStorageService _mock_hdfs;
	GlobalPropertiesBean _globals;
	String _test_file_path;
	long _test_file_time;
	
	@Before
	public void setUpDependencies() throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, ParentNotDirectoryException, UnsupportedFileSystemException, IllegalArgumentException, IOException {
		
		final String temp_dir = System.getProperty("java.io.tmpdir") + File.separator;
		
		_globals = new GlobalPropertiesBean(
				temp_dir, temp_dir, temp_dir, temp_dir);	
		
		_mock_hdfs = new MockStorageService(_globals);
		
		// Create a pretend "remote" file
		
		_test_file_path = temp_dir + "/test.jar";
		final Path test_file_path = new Path(_test_file_path);
		
		final FileContext fs = _mock_hdfs.getUnderlyingPlatformDriver(FileContext.class, Optional.empty()).get();
		fs.create(test_file_path, EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE));
		
		_test_file_time = fs.getFileStatus(test_file_path).getModificationTime();
	}
	
	public static class TestMessageBean {};
	
	@Test
	public void test_setup() {
		if (File.separator.equals("\\")) { // windows mode!
			assertTrue("WINDOWS MODE: hadoop home needs to be set (use -Dhadoop.home.dir={HADOOP_HOME} in JAVA_OPTS)", null != System.getProperty("hadoop.home.dir"));
			assertTrue("WINDOWS MODE: hadoop home needs to exist: " + System.getProperty("hadoop.home.dir"), null != System.getProperty("hadoop.home.dir"));
		}		
	}
	
	@Test
	public void test_localFileNotPresent() throws InterruptedException, ExecutionException, UnsupportedFileSystemException {
		
		final FileContext localfs = FileContext.getLocalFSFileContext(new Configuration());
		
		final String local_cached_dir = Optional.of(_globals.local_cached_jar_dir())
											.map(dir -> dir.replace(File.separator, "/"))
											.map(dir -> dir.endsWith("/") ? dir : (dir + "/"))
											.get();
		
		final String expected_cache_name = local_cached_dir + "test1.cache.jar";
		final Path expected_cache_path = localfs.makeQualified(new Path(expected_cache_name));
		
		// Just make sure we've deleted the old file
		try {
			new File(expected_cache_name).delete();
		}
		catch (Exception e) {}
		
		assertTrue("Remote file exists", new File(_test_file_path).exists());
		assertFalse("Local file doesn't exist", new File(expected_cache_name).exists());
		
		final SharedLibraryBean library_bean = BeanTemplateUtils.build(SharedLibraryBean.class)
															.with(SharedLibraryBean::path_name, _test_file_path)
															.with(SharedLibraryBean::_id, "test1")
															.done().get();
		
		final Validation<BasicMessageBean, String> ret_val_1 =
				JarCacheUtils.getCachedJar(_globals.local_cached_jar_dir(), library_bean, _mock_hdfs, "test1", new TestMessageBean()).get();
		
		assertEquals(expected_cache_path.toString(), ret_val_1.success());
		
		assertTrue("Local file now exists", new File(expected_cache_name).exists());		
	}
	
	@Test
	public void test_localFilePresentButOld() throws InterruptedException, ExecutionException, AccessControlException, FileAlreadyExistsException, FileNotFoundException, ParentNotDirectoryException, IOException {
		
		final FileContext localfs = FileContext.getLocalFSFileContext(new Configuration());
		
		String java_name = _globals.local_cached_jar_dir() + File.separator + "testX.cache.jar";
		final String expected_cache_name = java_name.replace(File.separator, "/"); 
		final Path expected_cache_path = localfs.makeQualified(new Path(expected_cache_name));
		
		// Just make sure we've deleted the old file
		try {
			System.out.println("Deleted: " + new File(java_name).delete());
		}
		catch (Exception e) {
			fail("Misc Error: " + e);
		}
		
		assertTrue("Remote file exists", new File(_test_file_path).exists());
		assertFalse("Local file doesn't exist: " +
				java_name,
				new File(java_name).exists());
		
		// Now create the file
		
		localfs.create(expected_cache_path, EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE));		
		
		localfs.setTimes(expected_cache_path, 0L, 0L);
		
		// check something has happened:
		assertEquals(0L, localfs.getFileStatus(expected_cache_path).getModificationTime());
		assertNotEquals(0L, _test_file_time);
		
		// Now run the test routine
		
		final SharedLibraryBean library_bean = BeanTemplateUtils.build(SharedLibraryBean.class)
															.with(SharedLibraryBean::path_name, _test_file_path)
															.with(SharedLibraryBean::_id, "testX")
															.done().get();
		
		final Validation<BasicMessageBean, String> ret_val_1 =
				JarCacheUtils.getCachedJar(_globals.local_cached_jar_dir(), library_bean, _mock_hdfs, "testX", new TestMessageBean()).get();
		
		assertEquals(expected_cache_path.toString(), ret_val_1.success());
		
		assertTrue("Local file still exists", new File(expected_cache_name).exists());
		
		assertTrue("File time should have been updated", localfs.getFileStatus(expected_cache_path).getModificationTime() >= _test_file_time);
	}
	
	@Test
	public void test_localFilePresentAndNew() throws InterruptedException, ExecutionException, AccessControlException, FileAlreadyExistsException, FileNotFoundException, ParentNotDirectoryException, IOException {
		
		final FileContext localfs = FileContext.getLocalFSFileContext(new Configuration());
		
		final String expected_cache_name = _globals.local_cached_jar_dir().replace(File.separator, "/") + "test1.cache.jar";
		final Path expected_cache_path = localfs.makeQualified(new Path(expected_cache_name));
		
		// Just make sure we've deleted the old file
		try {
			new File(expected_cache_name).delete();
		}
		catch (Exception e) {}
		
		assertTrue("Remote file exists", new File(_test_file_path).exists());
		assertFalse("Local file doesn't exist", new File(expected_cache_name).exists());
		
		// Now create the file
		
		localfs.create(expected_cache_path, EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE));		
		
		localfs.setTimes(expected_cache_path, _test_file_time + 10000, _test_file_time + 10000);
		
		// check something has happened:
		assertEquals(_test_file_time + 10000, localfs.getFileStatus(expected_cache_path).getModificationTime());
		
		// Now run the test routine
		
		final SharedLibraryBean library_bean = BeanTemplateUtils.build(SharedLibraryBean.class)
															.with(SharedLibraryBean::path_name, _test_file_path)
															.with(SharedLibraryBean::_id, "test1")
															.done().get();
		
		final Validation<BasicMessageBean, String> ret_val_1 =
				JarCacheUtils.getCachedJar(_globals.local_cached_jar_dir(), library_bean, _mock_hdfs, "test1", new TestMessageBean()).get();
		
		assertEquals(expected_cache_path.toString(), ret_val_1.success());
		
		assertTrue("Local file still exists", new File(expected_cache_name).exists());
		
		assertEquals(localfs.getFileStatus(expected_cache_path).getModificationTime(), _test_file_time + 10000);
	}

	
	@Test
	public void test_remoteFileNotPresent() throws InterruptedException, ExecutionException, UnsupportedFileSystemException {
		
		assertFalse("Remote file exists", new File(_test_file_path + "x").exists());
		
		final SharedLibraryBean library_bean = BeanTemplateUtils.build(SharedLibraryBean.class)
															.with(SharedLibraryBean::path_name, _test_file_path + "x")
															.with(SharedLibraryBean::_id, "test1")
															.done().get();
		
		final Validation<BasicMessageBean, String> ret_val_1 =
				JarCacheUtils.getCachedJar(_globals.local_cached_jar_dir(), library_bean, _mock_hdfs, "test1", new TestMessageBean()).get();		
		
		BasicMessageBean error = ret_val_1.fail();
		
		assertEquals(error.command(), "TestMessageBean");
		assertEquals((double)error.date().getTime(), (double)((new Date()).getTime()), 1000.0);
		assertEquals(error.details(), null);
		assertTrue(error.message().contains("not found (or user does not have read permission): "));
		assertEquals(error.message_code(), null);
		assertEquals(error.source(), "test1");
		assertEquals(error.success(), false);
		
	}	
}
