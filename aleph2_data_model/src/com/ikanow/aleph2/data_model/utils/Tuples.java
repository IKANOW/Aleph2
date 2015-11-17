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


import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;

/** Utility class for creating scala Tuples in Java
 * @author acp
 *
 */
public class Tuples {
	/** Create a generic n-tuple, where n is the number of args
	 * @return a tuple of the designated size
	 */
	public static <A, B> Tuple2<A, B> _2T(final A a, final B b) { return new Tuple2<A, B>(a, b); }
	/** Create a generic n-tuple, where n is the number of args
	 * @return a tuple of the designated size
	 */
	public static <A, B, C> Tuple3<A, B, C> _3T(final A a, final B b, final C c) { return new Tuple3<A, B, C>(a, b, c); }
	/** Create a generic n-tuple, where n is the number of args
	 * @return a tuple of the designated size
	 */
	public static <A, B, C, D> Tuple4<A, B, C, D> _4T(final A a, final B b, final C c, final D d) { return new Tuple4<A, B, C, D>(a, b, c, d); }
	/** Create a generic n-tuple, where n is the number of args
	 * @return a tuple of the designated size
	 */
	public static <A, B, C, D, E> Tuple5<A, B, C, D, E> _4T(final A a, final B b, final C c, final D d, final E e) { return new Tuple5<A, B, C, D, E>(a, b, c, d, e); }
}
