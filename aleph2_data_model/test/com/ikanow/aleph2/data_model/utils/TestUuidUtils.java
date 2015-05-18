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

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.junit.Test;

import com.ikanow.aleph2.data_model.interfaces.shared_services.IUuidService;

public class TestUuidUtils {

	@SuppressWarnings("unused")
	public static class TestBean {
		public static class NestedTestBean {
			public String nested1;
		}
		public static TestBean generate1() {
			NestedTestBean nested = new NestedTestBean();
			nested.nested1 = "nested1_1";
			TestBean ret = new TestBean();
			ret.test1 = "test1_1";
			ret.test2 = Arrays.asList(nested);
			ret.test3 = 1L;
			return ret;
		}
		public static TestBean generate2() {
			NestedTestBean nested = new NestedTestBean();
			nested.nested1 = "nested1_2";
			TestBean ret = new TestBean();
			ret.test1 = "test1_2";
			ret.test2 = Arrays.asList(nested);
			ret.test3 = 2L;
			return ret;			
		}
		private String test1;
		private List<NestedTestBean> test2;
		private Long test3;
	}
	
	@Test
	public void testDateUuidUtils() {
		IUuidService test_service = UuidUtils.get();
		
		// 0) performance
		
//		long start_time = System.currentTimeMillis();
//		for (int i = 0; i < 100; ++i) {
//			test_service.getTimeBasedUuid();
//		}
//		long end_time = System.currentTimeMillis();
//		System.out.println("elapsed time = " + (end_time - start_time));
		
		// 1) Check singleton pattern
		
		assertEquals(test_service, UuidUtils.get());
		
		// 2) Check I can create and reconstruct a date
		
		final Date d = new Date();
		final String time_uuid_2 = test_service.getTimeBasedUuid(d.getTime());
		final long time = test_service.getTimeUuid(time_uuid_2);
		
		assertEquals(d.getTime(), time);
		
		// 3) Check that the "get now" version returns dates around now

		final String time_uuid_3 = test_service.getTimeBasedUuid();
		final long time_3 = test_service.getTimeUuid(time_uuid_3);
		
		final Date now = new Date();
		
		assertEquals((double)now.getTime(), (double)time_3, 500.0); // within 500ms
		
		// 4) Check random UUIDs are different each time (weak, but...)
		
		for (int i = 0; i < 100; ++i) {
			assertNotEquals(test_service.getRandomUuid(), test_service.getRandomUuid());
		}
		
		// 5) Check the object code...
		
		String uuid1a = test_service.getContentBasedUuid(TestBean.generate1());
		String uuid2a = test_service.getContentBasedUuid(TestBean.generate2());
		String uuid1b = test_service.getContentBasedUuid(TestBean.generate1());
		String uuid2b = test_service.getContentBasedUuid(TestBean.generate2());
		
		assertEquals(uuid1a, uuid1b);
		assertEquals(uuid2a, uuid2b);
		assertNotEquals(uuid1a, uuid2a);
	}
}
