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

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Utility class that converts a null collection/iterable into an empty one
 */
public class Optionals {
	
	/** Returns the first element of a list that can be empty or null, with an empty option
	 *  NOTE: the element inside can be null
	 * @param a collection of Ts
	 * @return the first element, or an empty optional if it's null or empty
	 */
	@NonNull
	public static <T> Optional<T> first(final @Nullable Iterable<T> ts) {
		try {
			return Optional.of(ts.iterator().next());
		}
		catch (Exception e) { // empty
			return Optional.empty();
		}
	}
	
	/**
	 * @param a collection of Ts
	 * @return the collection, or an empty collection if "ts" is null
	 */
	@NonNull
	public static <T> Collection<T> ofNullable(final @Nullable Collection<T> ts) {
		return Optional.ofNullable(ts).orElse(Collections.emptyList());
	}
	/**
	 * @param ts
	 * @return the iterable, or an empty iterable if "ts" is null
	 */
	@NonNull
	public static <T> Iterable<T> ofNullable(final @Nullable Iterable<T> ts) {
		return Optional.ofNullable(ts).orElse(Collections.emptyList());
	}
}