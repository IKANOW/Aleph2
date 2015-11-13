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
package com.ikanow.aleph2.data_model.utils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;

import static org.junit.Assert.*;

import org.junit.Test;

public class TestTuples {

	@Test
	public void test1() {
		Tuple2<Integer, String> test2 = Tuples._2T(1, "test");
		Tuple3<Integer, String, List<Boolean>> test3 = Tuples._3T(1, "test", Arrays.asList(true, false));
		Tuple4<Integer, String, List<Boolean>, String> test4 = Tuples._4T(1, "test", Arrays.asList(true, true), "5");
		Tuple5<Integer, String, List<Boolean>, String, Map<String, String>> test5 = 
				Tuples._4T(1, "test", Arrays.asList(true, true), "5", 
						ImmutableMap.<String, String>builder().put("a", "b").build());
	
		assertEquals((Integer)test2._1(), (Integer)1);
		assertEquals(test2._2(), "test");
		assertEquals(test3._3().get(0), true);
		assertEquals(test4._4(), "5");
		assertEquals(test5._5().get("a"), "b");
	}
}
