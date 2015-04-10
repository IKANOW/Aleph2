/*******************************************************************************
 * Copyright 2015, The IKANOW Open Source Project.
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package com.ikanow.aleph2.data_model.utils;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

/**
 * Utility class that converts a null collection/iterable into an empty one
 */
public class Optionals {
	
	/**
	 * @param a collection of Ts
	 * @return the collection, or an empty collection if "ts" is null
	 */
	public static <T> Collection<T> ofNullable(Collection<T> ts) {
		return Optional.ofNullable(ts).orElse(Collections.emptyList());
	}
	/**
	 * @param ts
	 * @return the iterable, or an empty iterable if "ts" is null
	 */
	public static <T> Iterable<T> ofNullable(Iterable<T> ts) {
		return Optional.ofNullable(ts).orElse(Collections.emptyList());
	}
}