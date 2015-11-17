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

import static org.junit.Assert.*;

import java.util.function.Function;

import org.junit.Test;

public class TestFunctions {

	public static int test_f1_memoization = 0;
	public static String stringify_withSideEffect(Integer i) {
		test_f1_memoization += i;
		return Integer.toString(i);
	}
	
	public Function<Integer, String> memoized_stringify_withSideEffect = Functions.memoize(TestFunctions::stringify_withSideEffect);
	
	@Test
	public void test_F1_memoization() {
		
		assertEquals(0, test_f1_memoization);
		
		// Just double check:
		
		assertEquals("1", stringify_withSideEffect(1));
		assertEquals(1, test_f1_memoization);
		assertEquals("1", stringify_withSideEffect(1));
		assertEquals(2, test_f1_memoization);
		assertEquals("2", stringify_withSideEffect(2));
		assertEquals(4, test_f1_memoization);
		assertEquals("2", stringify_withSideEffect(2));
		assertEquals(6, test_f1_memoization);
		
		// Memoized version
		
		assertEquals("1", memoized_stringify_withSideEffect.apply(1));
		assertEquals(7, test_f1_memoization);
		assertEquals("1", memoized_stringify_withSideEffect.apply(1));
		assertEquals(7, test_f1_memoization);
		assertEquals("2", memoized_stringify_withSideEffect.apply(2));
		assertEquals(9, test_f1_memoization);
		assertEquals("2", memoized_stringify_withSideEffect.apply(2));
		assertEquals(9, test_f1_memoization);
		
	}
	
}
