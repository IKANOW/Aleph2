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
	public void testConvertDataBucketBeanToTest() {
		String original_full_name = "/my_bean/sample_path";
		String original_id = "id12345";
		DataBucketBean original_bean = BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::_id, "id12345")
				.with(DataBucketBean::full_name, original_full_name)
				.done().get();
		
		DataBucketBean test_bean = BucketUtils.convertDataBucketBeanToTest(original_bean);
		
		assertTrue(test_bean._id().equals(original_id));
		assertTrue(test_bean.full_name().equals("/test" + original_full_name));
	}

}
