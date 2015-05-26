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
package com.ikanow.aleph2.data_import_manager.harvest.utils;

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

import com.ikanow.aleph2.data_import_manager.utils.JarCacheUtils;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.storage_service_hdfs.services.MockHdfsStorageService;

import fj.data.Either;

public class TestJarCacheUtils {

	MockHdfsStorageService _mock_hdfs;
	GlobalPropertiesBean _globals;
	String _test_file_path;
	long _test_file_time;
	
	@Before
	public void setUpDependencies() throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, ParentNotDirectoryException, UnsupportedFileSystemException, IllegalArgumentException, IOException {
		
		final String temp_dir = System.getProperty("java.io.tmpdir");
		
		_globals = new GlobalPropertiesBean(
				temp_dir, temp_dir, temp_dir, temp_dir);	
		
		_mock_hdfs = new MockHdfsStorageService(_globals);
		
		// Create a pretend "remote" file
		
		_test_file_path = temp_dir + "/test.jar";
		final Path test_file_path = new Path(_test_file_path);
		
		final FileContext fs = _mock_hdfs.getUnderlyingPlatformDriver(FileContext.class, Optional.empty());
		fs.create(test_file_path, EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE));
		
		_test_file_time = fs.getFileStatus(test_file_path).getModificationTime();
	}
	
	public static class TestMessageBean {};
	
	@Test
	public void testSetup() {
		if (File.separator.equals("\\")) { // windows mode!
			assertTrue("WINDOWS MODE: hadoop home needs to be set (use -Dhadoop.home.dir={HADOOP_HOME} in JAVA_OPTS)", null != System.getProperty("hadoop.home.dir"));
			assertTrue("WINDOWS MODE: hadoop home needs to exist: " + System.getProperty("hadoop.home.dir"), null != System.getProperty("hadoop.home.dir"));
		}		
	}
	
	@Test
	public void testLocalFileNotPresent() throws InterruptedException, ExecutionException, UnsupportedFileSystemException {
		
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
		
		final SharedLibraryBean library_bean = BeanTemplateUtils.build(SharedLibraryBean.class)
															.with(SharedLibraryBean::path_name, _test_file_path)
															.with(SharedLibraryBean::_id, "test1")
															.done().get();
		
		final Either<BasicMessageBean, String> ret_val_1 =
				JarCacheUtils.getCachedJar(_globals.local_cached_jar_dir(), library_bean, _mock_hdfs, "test1", new TestMessageBean()).get();
		
		assertEquals(expected_cache_path.toString(), ret_val_1.right().value());
		
		assertTrue("Local file now exists", new File(expected_cache_name).exists());		
	}
	
	@Test
	public void testLocalFilePresentButOld() throws InterruptedException, ExecutionException, AccessControlException, FileAlreadyExistsException, FileNotFoundException, ParentNotDirectoryException, IOException {
		
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
		
		final Either<BasicMessageBean, String> ret_val_1 =
				JarCacheUtils.getCachedJar(_globals.local_cached_jar_dir(), library_bean, _mock_hdfs, "testX", new TestMessageBean()).get();
		
		assertEquals(expected_cache_path.toString(), ret_val_1.right().value());
		
		assertTrue("Local file still exists", new File(expected_cache_name).exists());
		
		assertTrue("File time should have been updated", localfs.getFileStatus(expected_cache_path).getModificationTime() >= _test_file_time);
	}
	
	@Test
	public void testLocalFilePresentAndNew() throws InterruptedException, ExecutionException, AccessControlException, FileAlreadyExistsException, FileNotFoundException, ParentNotDirectoryException, IOException {
		
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
		
		final Either<BasicMessageBean, String> ret_val_1 =
				JarCacheUtils.getCachedJar(_globals.local_cached_jar_dir(), library_bean, _mock_hdfs, "test1", new TestMessageBean()).get();
		
		assertEquals(expected_cache_path.toString(), ret_val_1.right().value());
		
		assertTrue("Local file still exists", new File(expected_cache_name).exists());
		
		assertEquals(localfs.getFileStatus(expected_cache_path).getModificationTime(), _test_file_time + 10000);
	}

	
	@Test
	public void testRemoteFileNotPresent() throws InterruptedException, ExecutionException, UnsupportedFileSystemException {
		
		assertFalse("Remote file exists", new File(_test_file_path + "x").exists());
		
		final SharedLibraryBean library_bean = BeanTemplateUtils.build(SharedLibraryBean.class)
															.with(SharedLibraryBean::path_name, _test_file_path + "x")
															.with(SharedLibraryBean::_id, "test1")
															.done().get();
		
		final Either<BasicMessageBean, String> ret_val_1 =
				JarCacheUtils.getCachedJar(_globals.local_cached_jar_dir(), library_bean, _mock_hdfs, "test1", new TestMessageBean()).get();		
		
		BasicMessageBean error = ret_val_1.left().value();
		
		assertEquals(error.command(), "TestMessageBean");
		assertEquals((double)error.date().getTime(), (double)((new Date()).getTime()), 1000.0);
		assertEquals(error.details(), null);
		String expected_message = "Shared library C:\\Users\\acp\\AppData\\Local\\Temp\\/test.jarx not found: [File file:/C:/Users/acp/AppData/Local/Temp/test.jarx does not exist: FileNotFoundException]:[RawLocalFileSystem.java:524:org.apache.hadoop.fs.RawLocalFileSystem:deprecatedGetFileStatus][FileContext.java:1112:org.apache.hadoop.fs.FileContext:getFileStatus][JarCacheUtils.java:58:com.ikanow.aleph2.data_import_manager.utils.JarCacheUtils:getCachedJar][TestJarCacheUtils.java:212:com.ikanow.aleph2.data_import_manager.harvest.utils.TestJarCacheUtils:testRemoteFileNotPresent]"
										.replaceAll(":[0-9]+", ":LINE");
		assertEquals(expected_message, error.message().replaceAll(":[0-9]+", ":LINE"));
		assertEquals(error.message_code(), null);
		assertEquals(error.source(), "test1");
		assertEquals(error.success(), false);
		
	}	
}
