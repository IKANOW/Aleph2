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
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestJarBuilderUtils {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test_hash() {		
		final String input_jar_location = System.getProperty("java.io.tmpdir");
		String hash1 = JarBuilderUtil.getHashedJarName(Arrays.asList("a","b","c"), input_jar_location);
		String hash2 = JarBuilderUtil.getHashedJarName(Arrays.asList("c","b","a"), input_jar_location);
		
		assertFalse(hash1.equals(hash2));
		String hash1_again = JarBuilderUtil.getHashedJarName(Arrays.asList("a","b","c"), input_jar_location);
		assertTrue(hash1.equals(hash1_again));
	}
	
	@Test
	public void test_getMostRecentDate() throws IOException, InterruptedException {
		//create some fake files				
		File file1 = File.createTempFile("recent_date_test_", null);
		Thread.sleep(1);
		File file2 = File.createTempFile("recent_date_test_", null);
		Thread.sleep(1);
		File file3 = File.createTempFile("recent_date_test_", null);
		List<String> files1 = Arrays.asList(file1.getCanonicalPath(),file2.getCanonicalPath(),file3.getCanonicalPath());
		List<String> files2 = Arrays.asList(file3.getCanonicalPath(),file2.getCanonicalPath(),file1.getCanonicalPath());
		
		//ensure they return file3 modified date
		assertEquals(file3.lastModified(), JarBuilderUtil.getMostRecentlyUpdatedFile(files1).getTime());
		assertEquals(file3.lastModified(), JarBuilderUtil.getMostRecentlyUpdatedFile(files2).getTime());
	
		//cleanup
		file1.delete();
		file2.delete();
		file3.delete();
	}

}
