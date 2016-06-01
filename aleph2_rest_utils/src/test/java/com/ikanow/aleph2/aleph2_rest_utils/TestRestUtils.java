/*******************************************************************************
 * Copyright 2016, The IKANOW Open Source Project.
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
package com.ikanow.aleph2.aleph2_rest_utils;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import scala.Tuple2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;

import fj.data.Either;

/**
 * @author Burch
 *
 */
public class TestRestUtils {

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() throws Exception {
		final Either<String,Tuple2<ICrudService<DataBucketBean>, Class<DataBucketBean>>> crud_service_either1 = RestUtils.getCrudService(null, null, null, null, null);
		final Either<String,Tuple2<ICrudService<DataBucketBean>, Class<DataBucketBean>>> crud_service_either2 = inbetween(DataBucketBean.class);
//		ObjectMapper mapper = new ObjectMapper();
//		final String query_obj = "{\"equals\":{\"fieldx\":\"valuey\"}}";
//    	try {
//			QueryComponentBean bean = mapper.readValue(query_obj, QueryComponentBean.class);
//			QueryComponent<?> q = QueryComponentBeanUtils.convertQueryComponentBeanToComponent(bean);
//			System.out.println(q.toString());
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//    	
//    	System.out.println(CrudUtils.allOf().when("fieldx", "valuey").toString());
	}
	
	public <T> Either<String,Tuple2<ICrudService<T>, Class<T>>> inbetween(Class<T> clazz) {
		return RestUtils.getCrudService(null, null, null, null, null);
	}

}
