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

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Optional;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TestJsonUtils {

	public static class TestBean {
		Integer test1;
	}
	
	@Test
	public void test_foldTuple() {
		final ObjectMapper mapper = BeanTemplateUtils.configureMapper(Optional.empty());
		
		LinkedHashMap<String, Object> test1 = new LinkedHashMap<String, Object>();
		test1.put("long", 10L);
		test1.put("double", 1.1);
		test1.put("string", "val");
		test1.put("json", "{\"misc\":true,\"long\":1, \"string\":\"\"}");
		
		final JsonNode j1 = JsonUtils.foldTuple(test1, mapper, Optional.empty());
		assertEquals("{\"misc\":true,\"long\":10,\"string\":\"val\",\"double\":1.1}", j1.toString());
		
		LinkedHashMap<String, Object> test2 = new LinkedHashMap<String, Object>();
		test2.put("misc", false);
		test2.put("long", 10L);
		test2.put("json", "{\"misc\":true,\"long\":1, \"string\":\"\"}");
		test2.put("double", 1.1);
		test2.put("string", "val");
		
		final JsonNode j2 = JsonUtils.foldTuple(test2, mapper, Optional.of("json"));
		assertEquals("{\"misc\":false,\"long\":10,\"string\":\"val\",\"double\":1.1}", j2.toString());

		LinkedHashMap<String, Object> test3 = new LinkedHashMap<String, Object>();
		test3.put("long", mapper.createObjectNode());
		test3.put("double", mapper.createArrayNode());
		test3.put("string", BeanTemplateUtils.build(TestBean.class).with("test1", 4).done().get());
		test3.put("json", "{\"misc\":true,\"long\":1, \"string\":\"\"}");
		
		final JsonNode j3 = JsonUtils.foldTuple(test3, mapper, Optional.of("json"));
		assertEquals("{\"misc\":true,\"long\":{},\"string\":{\"test1\":4},\"double\":[]}", j3.toString());

		LinkedHashMap<String, Object> test4 = new LinkedHashMap<String, Object>();
		test4.put("misc", BigDecimal.ONE);
		test4.put("long", (int)10);
		test4.put("double", (float)1.1);
		test4.put("json", "{\"misc\":true,\"long\":1, \"string\":\"\"}");
		test4.put("string", "val");
		
		final JsonNode j4 = JsonUtils.foldTuple(test4, mapper, Optional.of("json"));
		assertEquals("{\"misc\":1,\"long\":10,\"string\":\"val\",\"double\":1.1}", j4.toString());
		
		try {
			test4.put("json", "{\"misc\":true,\"long\":1, string\":\"\"}"); // (Added json error)
			JsonUtils.foldTuple(test4, mapper, Optional.of("json"));
			fail("Should have thrown JSON exception");
		}
		catch (Exception e) {} // json error, check
		
		new JsonUtils(); // (just for coverage)		
	}
}
