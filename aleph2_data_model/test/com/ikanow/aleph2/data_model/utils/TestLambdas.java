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

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;

import fj.data.Either;

public class TestLambdas {

	@Test
	public void testLambda() {
		final String x = Lambdas.get(() -> {
			return "test";			
		});
		assertEquals("test", x);
	}
	
	@Test
	public void testEitherLambda() {

		final Either<Throwable, String> ret_val =
			Either.<Throwable, String>right("java.lang.String").right()
				.bind(Lambdas.wrap_fj_e(clazz -> Class.forName(clazz)))
				.right()
				.bind(Lambdas.wrap_fj_e(clazz -> (String) clazz.newInstance()))
				;
		
		assertTrue("No error", ret_val.isRight());
		assertEquals("", ret_val.right().value());
		
		final Either<Throwable, String> ret_val2 =
				Either.<Throwable, String>right("java.lang.xxxxString").right()
					.bind(Lambdas.wrap_fj_e(clazz -> Class.forName(clazz)))
					.right()
					.bind(Lambdas.wrap_fj_e(clazz -> (String) clazz.newInstance()))
					;
			
		assertTrue("Error", ret_val2.isLeft());
		assertEquals(ClassNotFoundException.class, ret_val2.left().value().getClass());
	}

	@Test
	public void testOptionalLambda() {
				
		//TODO
	}

	@Test
	public void testStreamLambda_map() {
				
		// One that errors
		
		final Stream<String> initial_stream = Stream.of("java.lang.String", "java.lang.xxxxString", "java.lang.String");

		final Either<Throwable, List<String>> ret_val = 
				Lambdas.wrap_e(() ->
					initial_stream
						.map(Lambdas.wrap_u(clazz -> Class.forName(clazz)))
						.map(Lambdas.wrap_u(clazz -> (String) clazz.newInstance()))
						.collect(Collectors.toList())
					).get();
		
		assertTrue("Error", ret_val.isLeft());
		assertEquals(ClassNotFoundException.class, ret_val.left().value().getClass());
		
		// One that doesn't
		
		final Stream<String> initial_stream2 = Stream.of("java.lang.String", "java.lang.xxxxString", "java.lang.String");
		
		final Either<Throwable, List<String>> ret_val2 = 
				Lambdas.wrap_e(() ->
					initial_stream2
						.filter(Lambdas.wrap_filter_u(clazz -> !clazz.contains("xxx")))
						.map(Lambdas.wrap_u(clazz -> Class.forName(clazz)))
						.map(Lambdas.wrap_u(clazz -> (String) clazz.newInstance()))
						.collect(Collectors.toList())
					).get();
		
		if (ret_val2.isLeft()) {
			fail("Error: " + ret_val2.left().value().getMessage());
		}
		assertTrue("No error", ret_val2.isRight());
		assertEquals(2, ret_val2.right().value().size());
		
		// Runtime exception version
	
		final Stream<String> initial_stream3 = Stream.of("java.lang.String", "java.lang.xxxxString", "java.lang.String");
		
		final Either<Throwable, List<String>> ret_val3 = 
				Lambdas.wrap_e(() ->
					initial_stream3
						.filter(Lambdas.wrap_filter_u(clazz -> !clazz.contains("xxx")))
						.map(Lambdas.wrap_u(clazz -> Class.forName(clazz)))
						.map(Lambdas.wrap_u(clazz -> (String) clazz.newInstance()))
						.map(Lambdas.wrap_u(clazz -> {
							if (null == clazz) return clazz;
							else throw new RuntimeException("unchecked");
						}))
						.collect(Collectors.toList())
					).get();

		assertTrue("Error", ret_val3.isLeft());
		assertEquals(RuntimeException.class, ret_val3.left().value().getClass());
		
		// Handle checked exception
		
		Either<Throwable, String> ret_val4 = 
				Lambdas.wrap_e((String clazz) -> (String) Class.forName(clazz).newInstance())
					.apply("java.lang.xxx.String");
		
		assertTrue("Error", ret_val4.isLeft());
		assertEquals(ClassNotFoundException.class, ret_val4.left().value().getClass());
	}

	@Test
	public void testEitherStreamLambda_map() {
		final Stream<String> initial_stream = Stream.of("java.lang.String", "java.lang.xxxxString", "java.lang.String");

		final List<Either<Throwable, String>> ret_val = 
				initial_stream
					.map(Lambdas::startPipe)
					.map(Lambdas.wrap((String clazz) -> Class.forName(clazz)))
					.map(Lambdas.wrap(clazz -> (String) clazz.newInstance()))
					.collect(Collectors.toList())
					;
		
		final fj.data.List<Either<Throwable, String>> l = fj.data.List.iterableList(ret_val);
		
		final fj.data.List<Throwable> errs = Either.lefts(l);
		final fj.data.List<String> valids = Either.rights(l);
		
		assertEquals(1, errs.length());
		assertEquals(2, valids.length());
	}
	
	//TODO flatmap filter
	
	//TODO: custom collect (first error and list of errors)
}
