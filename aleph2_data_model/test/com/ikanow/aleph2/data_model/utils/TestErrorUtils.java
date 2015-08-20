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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import org.junit.Test;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;

public class TestErrorUtils {

	public static class TestErrorUtilsOverride extends ErrorUtils {
		public static String TEST_ERROR = "This is test {0} {1,number}";
		public static String EXCEPTION_HANDLE = "This is test {1,number}: {0}";
	}
	
	@Test
	public void test_errorUtils() {

		// Standard test
		
		final String test1 = TestErrorUtilsOverride.get(TestErrorUtilsOverride.TEST_ERROR, "number", 1);
		
		assertEquals("This is test number 1", test1);
		
		// Exception test combos:
		
		try {
			throw new RuntimeException("test 2");
		}
		catch (Exception e) {
			final String test2 = TestErrorUtilsOverride.get(TestErrorUtilsOverride.EXCEPTION_HANDLE, e, 2);
			
			assertEquals("This is test 2: test 2", test2);			
		}
		
		try {
			throw new RuntimeException((String)null);
		}
		catch (Exception e) {
			final String test3 = TestErrorUtilsOverride.get(TestErrorUtilsOverride.EXCEPTION_HANDLE, e, 3);
			
			assertEquals("This is test 3: (null)", test3);			
		}
		
		try {
			throw new RuntimeException("test 4a", new RuntimeException("test 4b"));
		}
		catch (Exception e) {
			final String test4 = TestErrorUtilsOverride.get(TestErrorUtilsOverride.EXCEPTION_HANDLE, e, 4);
			
			assertEquals("This is test 4: test 4a (test 4b)", test4);			
		}
		
		// Long form exception!
		
		try {
			throw new RuntimeException("test 5a");
		}
		catch (Exception e) {
			final String test5 = TestErrorUtilsOverride.getLongForm(TestErrorUtilsOverride.EXCEPTION_HANDLE, e, 5);
			
			assertEquals("This is test 5: [test 5a: RuntimeException]:[TestErrorUtils.java:74:com.ikanow.aleph2.data_model.utils.TestErrorUtils:testErrorUtils]", test5);			
		}		
		
		try {
			throw new RuntimeException("test 6a", new RuntimeException("test 6b"));
		}
		catch (Exception e) {
			final String test6 = TestErrorUtilsOverride.getLongForm(TestErrorUtilsOverride.EXCEPTION_HANDLE, e, 6);
			
			assertEquals("This is test 6: [test 6a: RuntimeException]:[TestErrorUtils.java:83:com.ikanow.aleph2.data_model.utils.TestErrorUtils:testErrorUtils] ([test 6b: RuntimeException]:[TestErrorUtils.java:83:com.ikanow.aleph2.data_model.utils.TestErrorUtils:testErrorUtils])", test6);			
		}
		
		try {
			throw new RuntimeException((String)null);
		}
		catch (Exception e) {
			final String test5 = TestErrorUtilsOverride.getLongForm(TestErrorUtilsOverride.EXCEPTION_HANDLE, e, 7);
			
			assertEquals("This is test 7: [null: RuntimeException]:[TestErrorUtils.java:92:com.ikanow.aleph2.data_model.utils.TestErrorUtils:testErrorUtils]", test5);			
		}		
		
		try {
			throw new RuntimeException("test 8a");
		}
		catch (Exception e) {
			final String test8 = TestErrorUtilsOverride.getLongForm("{0}", e);
			
			assertEquals("[test 8a: RuntimeException]:[TestErrorUtils.java:101:com.ikanow.aleph2.data_model.utils.TestErrorUtils:testErrorUtils]", test8);			
		}		
		
	}
	
	@Test
	public void test_longErrorDuringLambda() {
		List<String> output = new ArrayList<String>();
		List<Object> list = Arrays.asList("some item");
		list.stream().forEach(item -> {
			try {
				throw new Exception("Exception in Lambda");
			} catch (Exception ex) {
				output.add( ErrorUtils.getLongForm("Test: {0}", ex) );				
			}			
		});
		assertTrue(output.get(0).startsWith("Test: [Exception in Lambda: Exception]:"));		
	}
	
	public static class TestBean {}
	
	@Test
	public void test_basicMessageException() {
		final BasicMessageBean test = new BasicMessageBean(
				new Date(), false, "testsrc", "testcmd", null, "testmsg", null
				);
		
		try {
			throw new ErrorUtils.BasicMessageException(test);
		}
		catch (ErrorUtils.BasicMessageException me) {
			assertEquals("testmsg", me.getMessage());
			assertEquals("testmsg", me.getLocalizedMessage());
			assertEquals("testmsg", me.getMessageBean().message());
			assertEquals("testsrc", me.getMessageBean().source());
		}
	}
	
	@Test
	public void test_basicMessageBeanBuilding() {		
		{
			final BasicMessageBean bean = ErrorUtils.buildErrorMessage(TestBean.class, "test_msg", "test_message {0}", "test1");
			assertEquals(false, bean.success());
			assertEquals("TestBean", bean.source());
			assertEquals("test_msg", bean.command());
			assertEquals("test_message test1", bean.message());
		}
		{
			final BasicMessageBean bean = ErrorUtils.buildSuccessMessage("test_src", new TestBean(), "test_message test2");
			assertEquals(true, bean.success());
			assertEquals("test_src", bean.source());
			assertEquals("TestBean", bean.command());
			assertEquals("test_message test2", bean.message());
		}
		{
			final BasicMessageBean bean = ErrorUtils.buildMessage(true, new TestBean(), String.class, "test_message {0}{1}", "test", 3);
			assertEquals(true, bean.success());
			assertEquals("TestBean", bean.source());
			assertEquals("String", bean.command());
			assertEquals("test_message test3", bean.message());
		}
	}
}
