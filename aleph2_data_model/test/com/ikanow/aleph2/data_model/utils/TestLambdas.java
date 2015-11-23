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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;

import com.ikanow.aleph2.data_model.utils.Lambdas.ThrowableWrapper;

import scala.Tuple2;
import fj.data.Either;
import fj.data.Validation;

public class TestLambdas {

	@Test
	public void testLambda() {
		final String x = Lambdas.get(() -> {
			return "test";			
		});
		assertEquals("test", x);
	}
	
	@Test
	public void testValidationLambda() {

		final Validation<Throwable, String> ret_val =
			Validation.<Throwable, String>success("java.lang.String")
				.bind(Lambdas.wrap_fj_e(clazz -> Class.forName(clazz)))
				.bind(Lambdas.wrap_fj_e(clazz -> (String) clazz.newInstance()))
				;
		
		assertTrue("No error", ret_val.isSuccess());
		assertEquals("", ret_val.success());
		
		final Validation<Throwable, String> ret_val2 =
				Validation.<Throwable, String>success("java.lang.xxxxString")
					.bind(Lambdas.wrap_fj_e(clazz -> Class.forName(clazz)))
					.bind(Lambdas.wrap_fj_e(clazz -> (String) clazz.newInstance()))
					;
			
		assertTrue("Error", ret_val2.isFail());
		assertEquals(ClassNotFoundException.class, ret_val2.fail().getClass());
		
		// A few misc other fj/wrap cases:
		
		final Validation<Throwable, Object> ret_val3a = Lambdas.wrap_fj(s -> {
			return (Object) Class.forName("java.lang.String").newInstance();
		})
		.f(Validation.fail(new RuntimeException("3a")));
		assertTrue("Error", ret_val3a.isFail());
		
		final Validation<Throwable, Object> ret_val3b = Lambdas.wrap_fj(s -> {
			return (Object) Class.forName(s.toString()).newInstance();
		})
		.f(Validation.success("java.lang.String"));
		assertTrue("Not Error", ret_val3b.isSuccess());
		
		final Validation<Throwable, Object> ret_val3c = Lambdas.wrap_fj(s -> {
			throw new RuntimeException("no", new RuntimeException("test3c"));
		})
		.f(Validation.success("java.lang.String"));
		assertTrue("Error", ret_val3c.isFail());
		assertEquals("test3c", ret_val3c.fail().getMessage());
		
		// Finally...
		
		try {
			Lambdas.wrap_fj_u(s -> {
				return (Object) Class.forName(s.toString()).newInstance();
			})
			.f(Validation.success("java.lang.xxx.String"));
			fail("Should have thrown exception");
		}
		catch (RuntimeException e) {
			assertEquals(ClassNotFoundException.class, e.getCause().getClass());
		}
	}

	/////////////////////////////////////////////
	
	@Test
	public void testStreamLambda_map() {
				
		// One that errors
		
		final Stream<String> initial_stream = Stream.of("java.lang.String", "java.lang.xxxxString", "java.lang.String");

		final Validation<Throwable, List<String>> ret_val = 
				Lambdas.wrap_e(() ->
					initial_stream
						.map(Lambdas.wrap_u(clazz -> Class.forName(clazz)))
						.map(Lambdas.wrap_u(clazz -> (String) clazz.newInstance()))
						.collect(Collectors.toList())
					).get();
		
		assertTrue("Error", ret_val.isFail());
		assertEquals(ClassNotFoundException.class, ret_val.fail().getClass());
		
		// One that doesn't
		
		final Stream<String> initial_stream2 = Stream.of("java.lang.String", "java.lang.xxxxString", "java.lang.String");
		
		final Validation<Throwable, List<String>> ret_val2 = 
				Lambdas.wrap_e(() ->
					initial_stream2
						.filter(Lambdas.wrap_filter_u(clazz -> !clazz.contains("xxx")))
						.map(Lambdas.wrap_u(clazz -> Class.forName(clazz)))
						.map(Lambdas.wrap_u(clazz -> (String) clazz.newInstance()))
						.collect(Collectors.toList())
					).get();
		
		if (ret_val2.isFail()) {
			fail("Error: " + ret_val2.fail().getMessage());
		}
		assertTrue("No error", ret_val2.isSuccess());
		assertEquals(2, ret_val2.success().size());
		
		// Runtime exception version
	
		final Stream<String> initial_stream3 = Stream.of("java.lang.String", "java.lang.xxxxString", "java.lang.String");
		
		final Validation<Throwable, List<String>> ret_val3 = 
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

		assertTrue("Error", ret_val3.isFail());
		assertEquals(RuntimeException.class, ret_val3.fail().getClass());
		
		// Handle checked exception
		
		Validation<Throwable, String> ret_val4 = 
				Lambdas.wrap_e((String clazz) -> (String) Class.forName(clazz).newInstance())
					.apply("java.lang.xxx.String");
		
		assertTrue("Error", ret_val4.isFail());
		assertEquals(ClassNotFoundException.class, ret_val4.fail().getClass());
	}

	/////////////////////////////////////////////
	
	@Test
	public void testFlatMap() {
	
		// Normal mode
		
		final Validation<Throwable, List<String>> ret_val = 
				Lambdas.wrap_e(() ->
					Stream.of(Arrays.asList("java.lang.String", "java.lang.xxxxString", "java.lang.String"))
						.<String>flatMap(l -> l.stream().map(Lambdas.wrap_u(clazz -> 
								(String) Class.forName(clazz).newInstance())))
						.collect(Collectors.toList())
				).get();

		assertTrue("Error", ret_val.isFail());
		assertEquals(ClassNotFoundException.class, ret_val.fail().getClass());
		
		// Use flatmap to initiate a pipeline
		
		final Tuple2<Optional<Throwable>, List<String>> ret_val2 = 
					Stream.of(Arrays.asList("java.lang.String", "java.lang.xxxxString", "java.lang.String"))
						.flatMap(l -> 
							l.stream().map(Lambdas.<String, String>wrap_e(clazz -> 
								(String) Class.forName(clazz).newInstance())))
						.collect(Lambdas.collector_err(Collectors.toList()));

		assertEquals(true, ret_val2._1().isPresent());
		assertEquals(2, ret_val2._2().size());
	}	
	
	@Test
	public void testMisc_filterWithIgnore() {
		
		// Flat map to ignore

		final Stream<String> initial_stream1 = Stream.of("java.lang.String", "java.lang.xxxxString", "java.lang.String");
		
		final List<String> ret_val1 = 
					initial_stream1
						.flatMap(Lambdas.flatWrap_i(clazz -> Class.forName(clazz)))
						.map(Lambdas.wrap_u(clazz -> (String) clazz.newInstance()))
						.collect(Collectors.toList());
		
		assertEquals(2, ret_val1.size());
		
		// Filter with ignore
		
		final Stream<String> initial_stream2 = Stream.of("java.lang.String", "java.lang.xxxxString", "java.lang.String");
		
		final List<String> ret_val2 = 
					initial_stream2
						.filter(Lambdas.wrap_filter_i(clazz -> {
							throw new RuntimeException("test");
						}))
						.map(Lambdas.wrap_u(clazz -> Class.forName(clazz)))
						.map(Lambdas.wrap_u(clazz -> (String) clazz.newInstance()))
						.collect(Collectors.toList());
		
		assertEquals(0, ret_val2.size());
	}
	
	@Test
	public void testMisc_filterWithIgnore_Optionals() {
		
		// Flat map to ignore

		final Optional<String> s_pass = Optional.of("java.lang.String");
		final Optional<String> s_fail = Optional.of("java.lang.xxxxString");

		final Optional<Object> res_pass = s_pass.flatMap(Lambdas.maybeWrap_i(clazz -> Class.forName(clazz)));
		assertTrue(res_pass.isPresent());
		
		final Optional<Object> res_fail = s_fail.flatMap(Lambdas.maybeWrap_i(clazz -> Class.forName(clazz)));
		assertFalse(res_fail.isPresent());
	}
	
	/////////////////////////////////////////////
	
	@Test
	public void testValidationStreamLambda_map() {
		
		// "Fail"
		
		final Stream<String> initial_stream = Stream.of("java.lang.String", "java.lang.xxxxString", "java.lang.String");

		final List<Either<Throwable, String>> ret_val = 
				initial_stream
					.map(Lambdas::startPipe)
					.map(Lambdas.wrap((String clazz) -> Class.forName(clazz)))
					.map(Lambdas.wrap(clazz -> (String) clazz.newInstance()))
					.map(v -> v.toEither())
					.collect(Collectors.toList())
					;
		
		final fj.data.List<Either<Throwable, String>> l = fj.data.List.iterableList(ret_val);
		
		final fj.data.List<Throwable> errs = Either.lefts(l);
		final fj.data.List<String> valids = Either.rights(l);
		
		assertEquals(1, errs.length());
		assertEquals(2, valids.length());
		
		// "Pass" because of filter_e
		
		final Stream<String> initial_stream2 = Stream.of("java.lang.String", "java.lang.xxxxString", "java.lang.String");

		final List<Either<Throwable, String>> ret_val2 = 
				initial_stream2
					.map(Lambdas::startPipe)
					.flatMap(Lambdas.filter_e((String clazz) -> !clazz.contains("xxx")))
					.map(Lambdas.wrap((String clazz) -> Class.forName(clazz)))
					.map(Lambdas.wrap(clazz -> (String) clazz.newInstance()))
					.map(v -> v.toEither())
					.collect(Collectors.toList())
					;
		
		final fj.data.List<Either<Throwable, String>> l2 = fj.data.List.iterableList(ret_val2);
		
		final fj.data.List<Throwable> errs2 = Either.lefts(l2);
		final fj.data.List<String> valids2 = Either.rights(l2);
		
		assertEquals(0, errs2.length());
		assertEquals(2, valids2.length());
		
		// "pass" because filter_u, before error

		final Stream<String> initial_stream3 = Stream.of("java.lang.String", "java.lang.xxxxString", "java.lang.String");

		final List<Either<Throwable, String>> ret_val3 = 
				initial_stream3
					.map(Lambdas::startPipe)
					.filter(Lambdas.filter_u(false, (String clazz) -> !clazz.contains("xxx")))
					.map(Lambdas.wrap((String clazz) -> Class.forName(clazz)))
					.map(Lambdas.wrap(clazz -> (String) clazz.newInstance()))
					.map(v -> v.toEither())
					.collect(Collectors.toList())
					;
		
		final fj.data.List<Either<Throwable, String>> l3 = fj.data.List.iterableList(ret_val3);
		
		final fj.data.List<Throwable> errs3 = Either.lefts(l3);
		final fj.data.List<String> valids3 = Either.rights(l3);
		
		assertEquals(0, errs3.length());
		assertEquals(2, valids3.length());
		
		// "pass" because filter_u, after error

		final Stream<String> initial_stream4 = Stream.of("java.lang.String", "java.lang.xxxxString", "java.lang.String");

		final List<Either<Throwable, String>> ret_val4 = 
				initial_stream4
					.map(Lambdas::startPipe)
					.map(Lambdas.wrap((String clazz) -> Class.forName(clazz)))
					.map(Lambdas.wrap(clazz -> (String) clazz.newInstance()))
					.filter(Lambdas.filter_u(true, __ -> true))
					.map(v -> v.toEither())
					.collect(Collectors.toList())
					;
		
		final fj.data.List<Either<Throwable, String>> l4 = fj.data.List.iterableList(ret_val4);
		
		final fj.data.List<Throwable> errs4 = Either.lefts(l4);
		final fj.data.List<String> valids4 = Either.rights(l4);
		
		assertEquals(0, errs4.length());
		assertEquals(2, valids4.length());
	}
	
	@Test
	public void testValidationStreamLambda() {
		
		// "Fail" - grabs first error
		
		final Stream<String> initial_stream = Stream.of("java.lang.String", "java.lang.xxxxString", "java.lang.String", "java.lang.xxxxString");

		final Tuple2<Optional<Throwable>, List<String>> ret_val = 
				initial_stream
					.map(Lambdas::startPipe)
					.map(Lambdas.wrap((String clazz) -> Class.forName(clazz)))
					.map(Lambdas.wrap(clazz -> (String) clazz.newInstance()))
					.collect(Lambdas.collector_err(Collectors.toList()))
					;
		
		assertEquals(true, ret_val._1().isPresent());
		assertEquals(2, ret_val._2().size());
		
		// "Fail" - grabs first error
		
		final Stream<String> initial_stream2 = Stream.of("java.lang.String", "java.lang.xxxxString", "java.lang.String", "java.lang.xxxxString");

		final Tuple2<List<Throwable>, List<String>> ret_val2 = 
				initial_stream2
					.map(Lambdas::startPipe)
					.map(Lambdas.wrap((String clazz) -> Class.forName(clazz)))
					.map(Lambdas.wrap(clazz -> (String) clazz.newInstance()))
					.collect(Lambdas.collector_errs(Collectors.toList()))
					;
		
		assertEquals(2, ret_val2._1().size());
		assertEquals(2, ret_val2._2().size());
		
		// "pass" because filter_u, before error

		final Stream<String> initial_stream3 = Stream.of("java.lang.String", "java.lang.xxxxString", "java.lang.String");

		final Tuple2<Optional<Throwable>, List<String>> ret_val3 = 
				initial_stream3
					.map(Lambdas::startPipe)
					.filter(Lambdas.filter_u(false, (String clazz) -> !clazz.contains("xxx")))
					.map(Lambdas.wrap((String clazz) -> Class.forName(clazz)))
					.map(Lambdas.wrap(clazz -> (String) clazz.newInstance()))
					.collect(Lambdas.collector_err(Collectors.toList()))
					;
		
		assertEquals(false, ret_val3._1().isPresent());
		assertEquals(2, ret_val3._2().size());
		
		// "pass" because filter_u, after error

		final Stream<String> initial_stream4 = Stream.of("java.lang.String", "java.lang.xxxxString", "java.lang.String");

		final Tuple2<List<Throwable>, List<String>> ret_val4 = 
				initial_stream4
					.map(Lambdas::startPipe)
					.map(Lambdas.wrap((String clazz) -> Class.forName(clazz)))
					.map(Lambdas.wrap(clazz -> (String) clazz.newInstance()))
					.filter(Lambdas.filter_u(true, __ -> true))
					.collect(Lambdas.collector_errs(Collectors.toList()))
					;

		assertEquals(0, ret_val4._1().size());
		assertEquals(2, ret_val4._2().size());		
	}	
	
	
	/////////////////////////////////////////////////////////////
	
	@Test
	public void testMisc_biFunctions() {
		final ThrowableWrapper.BiFunction<String, String, String> test_method =
				(a, b) -> {
					if (null == a) throw new RuntimeException("a");
					if (null == b) throw new IllegalArgumentException("b");
					return a + b;
				};					
		
		try {
			assertEquals(IllegalArgumentException.class, Lambdas.wrap_e(test_method).apply("a", null).fail().getClass());
			Lambdas.wrap_u(test_method).apply("a", null);
			fail("");
		}
		catch (Exception e) {
			assertEquals(RuntimeException.class, e.getClass());
		}
		try {
			assertEquals(RuntimeException.class, Lambdas.wrap_e(test_method).apply(null, "b").fail().getClass());
			Lambdas.wrap_u((String a, String b) -> test_method.apply(a, b)).apply(null, "b");
			fail("");
		}
		catch (Exception e) {
			assertEquals(RuntimeException.class, e.getClass());
		}
		assertEquals("ab", Lambdas.wrap_e(test_method).apply("a", "b").success());			
		assertEquals("ab", Lambdas.wrap_u(test_method).apply("a", "b"));			
	}
	
	@Test
	public void testMisc_Consumer() {
		
		// Test supplier
		
		assertEquals("test", Lambdas.wrap_u(() -> {return "test";}).get());
		
		try {
			Lambdas.wrap_u(() -> {throw new RuntimeException("test2b");}).get();
		}
		catch (RuntimeException e) {
			assertEquals("java.lang.RuntimeException: test2b", e.getMessage());
		}
		
		// Test consumer (convert to unchecked)
		
		final HashSet<String> set = new HashSet<>();
		
		try {
			Lambdas.<String>wrap_consumer_u(s -> set.add(s)).accept("test");
		}
		catch (RuntimeException e) {
			fail("Should not have thrown: " + e.getMessage());
		}
		assertEquals(1, set.size());

		try {
			Lambdas.<String>wrap_consumer_u(s -> {
				throw new RuntimeException("test2");
			}).accept("test");
		}
		catch (RuntimeException e) {
			assertEquals("java.lang.RuntimeException: test2", e.getMessage());
		}
		
		// Test consumer (ignore)
		
		final HashSet<String> set2 = new HashSet<>();
		
		try {
			Lambdas.<String>wrap_consumer_i(s -> set2.add(s)).accept("test");
		}
		catch (RuntimeException e) {
			fail("Should not have thrown: " + e.getMessage());
		}
		assertEquals(1, set2.size());

		try {
			Lambdas.<String>wrap_consumer_i(s -> {
				throw new RuntimeException("test2");
			}).accept("test");
		}
		catch (RuntimeException e) {
			fail("Should not have thrown " + e.getMessage());
		}		
	}

	@Test
	public void testMisc_Runnable() {
		
		// Test runnable (convert to unchecked)
		
		final HashSet<String> set = new HashSet<>();
		
		try {
			Lambdas.wrap_runnable_u(() -> set.add("test")).run();
		}
		catch (RuntimeException e) {
			fail("Should not have thrown: " + e.getMessage());
		}
		assertEquals(1, set.size());

		try {
			Lambdas.wrap_runnable_u(() -> {
				throw new RuntimeException("test2");
			}).run();
		}
		catch (RuntimeException e) {
			assertEquals("java.lang.RuntimeException: test2", e.getMessage());
		}
		
		// Test consumer (ignore)
		
		final HashSet<String> set2 = new HashSet<>();
		
		try {
			Lambdas.wrap_runnable_i(() -> set2.add("test")).run();
		}
		catch (RuntimeException e) {
			fail("Should not have thrown: " + e.getMessage());
		}
		assertEquals(1, set2.size());

		try {
			Lambdas.wrap_runnable_i(() -> {
				throw new RuntimeException("test2");
			}).run();
		}
		catch (RuntimeException e) {
			fail("Should not have thrown " + e.getMessage());
		}
		
	}

}
