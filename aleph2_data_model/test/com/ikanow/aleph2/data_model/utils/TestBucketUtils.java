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
 ******************************************************************************/
package com.ikanow.aleph2.data_model.utils;

import static org.junit.Assert.*;

import java.util.Optional;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;

public class TestBucketUtils {

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
	public void test_ConvertDataBucketBeanToTest() {
		String original_full_name = "/my_bean/sample_path";
		String original_id = "id12345";
		String user_id = "user12345";
		DataBucketBean original_bean = BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::_id, "id12345")
				.with(DataBucketBean::full_name, original_full_name)
				.done().get();
		
		DataBucketBean test_bean = BucketUtils.convertDataBucketBeanToTest(original_bean, user_id);
		
		assertTrue(test_bean._id().equals(original_id));
		assertTrue(test_bean.full_name().equals("/aleph2_testing/" + user_id + "/" + original_full_name));
		
		assertTrue(BucketUtils.isTestBucket(test_bean));
		assertFalse(BucketUtils.isTestBucket(original_bean));		
	}

	@Test
	public void test_getUniqueBucketSignature() {
		
		final String path1 = "/test+extra/";
		final String path2 = "/test+extra/4354____42";
		final String path3 = "test+extra/4354____42/some/more/COMPONENTS_VERY_VERY_LONG";
		
		assertEquals("test_extra__c1651d4c69ed", BucketUtils.getUniqueSignature(path1, Optional.empty()));
		assertEquals("test_extra_test_12345__c1651d4c69ed", BucketUtils.getUniqueSignature(path1, Optional.of("test+;12345")));
		assertEquals("test_extra_4354_42__bb8a6a382d7b", BucketUtils.getUniqueSignature(path2, Optional.empty()));
		assertEquals("test_extra_4354_42_t__bb8a6a382d7b", BucketUtils.getUniqueSignature(path2, Optional.of("t")));
		assertEquals("test_extra_more_components_very__7768508661fc", BucketUtils.getUniqueSignature(path3, Optional.empty()));
		assertEquals("test_extra_more_components_very_xx__7768508661fc", BucketUtils.getUniqueSignature(path3, Optional.of("XX__________")));
	}
	
	
}
