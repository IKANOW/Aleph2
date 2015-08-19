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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/** Function utilities for FP java
 * @author Alex
 */
public class Functions {

	/** Wrap the specified function in a memoization, ie only calls it once per parameter(s) and otherwise returns the stored results 
	 * @param function
	 * @return
	 */
	public static <T, U> Function<T, U> memoize(final Function<T, U> function) {
		return new FunctionMemoizer<T, U>().doMemoize(function);
	}
	/** Memoization wrapper for F1 
	 * @author Alex
	 *
	 * @param <T> input type
	 * @param <U> output type
	 */
	public static class FunctionMemoizer<T, U> {

		private final Map<T, U> cache = new ConcurrentHashMap<>();

		private FunctionMemoizer() {}

		private Function<T, U> doMemoize(final Function<T, U> function) {
			return input -> {
				if (cache.keySet().contains(input)) {
					return cache.get(input);
				}
				else {
					final U res = function.apply(input);
					cache.put(input, res);
					return res;
				}
			};
		}
	}
}
