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
import static org.hamcrest.CoreMatchers.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.ImmutableMap;

public class TestObjectUtils {

	public static class TestBean {
		public String _testField() { return testField; } /** Test field */
		
		private String testField;
	}
	
	@Test
	public void testSingleMethodHelper() {
		
		String test1 = ObjectUtils.from(TestBean.class).field(TestBean::_testField);
		TestBean t = new TestBean();
		String test2 = ObjectUtils.from(t).field(TestBean::_testField);
		assertEquals("The type safe reference should resolve correctly (class ref)", "testField", test1);
		assertEquals("The type safe reference should resolve correctly (object ref)", "testField", test2);
	}

	public static class TestBuildBean {
		public String _testField() { return testField; } /** Test field */
		public String _test3Field() { return test3Field; } /** Test field */
		
		String testField;
		String test2Field;
		String test3Field;
		List<String> test4Fields;
	}	
	
	@Test
	public void testBeanBuilder() {
		
		TestBuildBean test = ObjectUtils.build(TestBuildBean.class)
								.with(TestBuildBean::_testField, "4")
								.with("test2Field", "5")
								.with("test4Fields", Arrays.asList("1", "2", "3"))
								.done();
		
		assertEquals("testField should have been set", "4", test._testField());
		assertEquals("test2Field should have been set", "5", test.test2Field);
		assertEquals("test3Field should have not been set", null, test._test3Field());
		assertThat("test4Fields should be this list", test.test4Fields, is(Arrays.asList("1", "2", "3")));		
	}
	
	@Rule
	public ExpectedException exception = ExpectedException.none();
	
	@Test
	public void testBeanBuilder_ContainersImmutable() {
		TestBuildBean test = ObjectUtils.build(TestBuildBean.class)
				.with(TestBuildBean::_testField, "4")
				.with("test2Field", "5")
				.with("test4Fields", Arrays.asList("1", "2", "3"))
				.done();

		exception.expect(UnsupportedOperationException.class);
		test.test4Fields.add("INF");
	}

	public static class TestCloneBean {
		public String _testField() { return testField; } /** Test field */
		public String _test3Field() { return test3Field; } /** Test field */
		
		String testField;
		String test2Field;
		String test3Field;
		String test4Field;
		String test5Field;
		Map<String, Integer> test6Fields;
	}
	
	@Test
	public void testBeanCloner() {
		
		TestCloneBean to_clone = new TestCloneBean();
		to_clone.testField = "1"; // change
		to_clone.test2Field = "2"; // null
		to_clone.test3Field = null; // write
		to_clone.test4Field = null; // leave
		to_clone.test5Field = "5"; // leave
		to_clone.test6Fields = new HashMap<String, Integer>();
		to_clone.test6Fields.put("6", 6);
		
		TestCloneBean test = ObjectUtils.clone(to_clone)
								.with(TestCloneBean::_testField, "1b")
								.with("test2Field", null)
								.with(TestCloneBean::_test3Field, "3")
								.with("test6Fields",
										ImmutableMap.<String, Integer>builder()
											.putAll(to_clone.test6Fields)
											.put("7", 7)
											.build()
										)
								.done();
		
		HashMap<String, Integer> expected = new HashMap<String, Integer>();
		expected.put("6", 6);
		expected.put("7", 7);
		
		assertEquals("testField should have been changed", "1b", test._testField());
		assertEquals("test2Field should have nulled", null, test.test2Field);
		assertEquals("test3Field should have have been set", "3", test._test3Field());
		assertEquals("test4Field should have have been left null", null, test.test4Field);
		assertEquals("test5Field should have have been left 5", "5", test.test5Field);
		assertEquals("test6Fields should be this map", test.test6Fields, expected);		
	}

	@Test
	public void testBeanCloner_ContainersImmutable() {
		TestCloneBean to_clone = new TestCloneBean();
		to_clone.testField = "1"; // change
		to_clone.test2Field = "2"; // null
		to_clone.test3Field = null; // write
		to_clone.test4Field = null; // leave
		to_clone.test5Field = "5"; // leave
		to_clone.test6Fields = new HashMap<String, Integer>();
		to_clone.test6Fields.put("6", 6);
		
		TestCloneBean test = ObjectUtils.clone(to_clone)
								.with(TestCloneBean::_testField, "1b")
								.with("test2Field", null)
								.with(TestCloneBean::_test3Field, "3")
								.with("test6Fields",
										ImmutableMap.<String, Integer>builder()
											.putAll(to_clone.test6Fields)
											.put("7", 7)
											.build()
										)
								.done();
		
		exception.expect(UnsupportedOperationException.class);
		test.test6Fields.put("INF", 0);
	}


}
