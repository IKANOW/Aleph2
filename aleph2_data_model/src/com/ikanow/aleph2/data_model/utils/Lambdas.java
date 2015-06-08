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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collector.Characteristics;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.Tuple2;
import fj.F;
import fj.data.Either;

public class Lambdas {

	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	
	// WRAPPERS THAT CONVERT EXCEPTIONS
	
	public static <T> Either<Throwable, T> startPipe(final T t) {
		return Either.right(t);
	}
	
	//////////////////////////////////////////////////////////////////////////////////
	
	// SUPPLIER
	
	/** Executes the provided supplier
	 * @param supplier - the supplier to execute ie () -> { return something; } 
	 * @return the result of the application
	 */
	public static <T> T get(Supplier<T> supplier) {
		return supplier.get();
	}
	
	/** Wraps a function that returns a checked throwable (Exception/error) into one that returns an unchecked one
	 * @param f - the lambda that can return a checked throwable
	 * @return - an identical function except throws unchecked instead of checked throwable
	 */
	public static <R> Supplier<R> wrap_u(ThrowableWrapper.Supplier<R> f) {
		return () -> {
			try {
				return f.get();
			}
			catch (Throwable err) {
				throw new RuntimeException(err);
			}
		};
	}

	/** Wraps a function that returns a checked or unchecked throwable (Exception/error) into one that returns an Either<Throwable, ?> but no exception
	 * @param f - the lambda that can return a checked throwable
	 * @return - an identical function except throws unchecked instead of checked throwable
	 */
	public static <R> Supplier<Either<Throwable, R>> wrap_e(ThrowableWrapper.Supplier<R> f) {
		return () -> {
			try {
				return Either.right(f.get());
			}
			catch (RuntimeException re) {
				return denest(re);
			}
			catch (Throwable err) {
				return Either.left(err);
			}
		};		
	}
	
	//////////////////////////////////////////////////////////////////////////////////
	
	// CONSUMER
	
	/** Wraps a function that returns a checked throwable (Exception/error) into one that returns an unchecked one
	 * @param f - the lambda that can return a checked throwable
	 * @return - an identical function except throws unchecked instead of checked throwable
	 */
	public static <T> Consumer<T> wrap_consumer_u(ThrowableWrapper.Consumer<T> f) {
		return t -> {
			try {
				f.accept(t);
			}
			catch (Throwable err) {
				throw new RuntimeException(err);
			}
		};
	}

	/** Wraps a predicate that returns a checked throwable (Exception/error) into one that returns false is an exception occurs
	 * @param f - the lambda predicate that can return a checked throwable
	 * @return - an identical predicate except returns false instead of checked throwable
	 */
	public static <T> Consumer<T> wrap_consumer_i(ThrowableWrapper.Consumer<T> f) {
		return t -> {
			try {
				f.accept(t);
			}
			catch (Throwable err) {}
		};
	}

	//////////////////////////////////////////////////////////////////////////////////
	
	// RUNNABLE
	
	/** Wraps a function that returns a checked throwable (Exception/error) into one that returns an unchecked one
	 * @param f - the lambda that can return a checked throwable
	 * @return - an identical function except throws unchecked instead of checked throwable
	 */
	public static Runnable wrap_runnable_u(ThrowableWrapper.Runnable f) {
		return () -> {
			try {
				f.run();
			}
			catch (Throwable err) {
				throw new RuntimeException(err);
			}
		};
	}

	/** Wraps a predicate that returns a checked throwable (Exception/error) into one that returns false is an exception occurs
	 * @param f - the lambda predicate that can return a checked throwable
	 * @return - an identical predicate except returns false instead of checked throwable
	 */
	public static Runnable wrap_runnable_i(ThrowableWrapper.Runnable f) {
		return () -> {
			try {
				f.run();
			}
			catch (Throwable err) {}
		};
	}

	//////////////////////////////////////////////////////////////////////////////////
	
	// PREDICATE
	
	/** Wraps a function that returns a checked throwable (Exception/error) into one that returns an unchecked one
	 * @param f - the lambda that can return a checked throwable
	 * @return - an identical function except throws unchecked instead of checked throwable
	 */
	public static <T> Predicate<T> wrap_filter_u(ThrowableWrapper.Predicate<T> f) {
		return t -> {
			try {
				return f.test(t);
			}
			catch (Throwable err) {
				throw new RuntimeException(err);
			}
		};
	}

	/** Wraps a predicate that returns a checked throwable (Exception/error) into one that returns false is an exception occurs
	 * @param f - the lambda predicate that can return a checked throwable
	 * @return - an identical predicate except returns false instead of checked throwable
	 */
	public static <T> Predicate<T> wrap_filter_i(ThrowableWrapper.Predicate<T> f) {
		return t -> {
			try {
				return f.test(t);
			}
			catch (Throwable err) {
				return false;
			}
		};
	}

	/** Wraps a function that returns a checked throwable, and returns either an error or an empty optional (if the predicate fails)
	 * @param f - the lambda (predicate) that can return a checked throwable
	 * @return - a function that convert an identical function except returns either an error or an empty optional (if the predicate fails)
	 */
	public static <T> Function<Either<Throwable, T>, Stream<Either<Throwable, T>>> filter_e(ThrowableWrapper.Predicate<T> f) {
		return err_t -> {
			return err_t.<Stream<Either<Throwable, T>>>either(
				err -> Stream.of(Either.left(err))
				, 
				t -> {
					try {
						if (f.test(t)) {
							return Stream.of(Either.right(t));
						}
						else {
							return Stream.empty();
						}
					}
					catch (RuntimeException re) {
						return Stream.of(denest(re));
					}
					catch (Throwable err) {
						return Stream.of(Either.left(err));
					}
				});
		};		
	}
	
	/** Wraps a function that returns a checked throwable, and returns either an error or an empty optional (if the predicate fails)
	 * @param f - the lambda (predicate) that can return a checked throwable
	 * @return - a function that convert an identical function except returns either an error or an empty optional (if the predicate fails)
	 */
	public static <T> Predicate<Either<Throwable, T>> filter_u(final boolean drop_errors, ThrowableWrapper.Predicate<T> f) {
		return err_t -> {
			return err_t.<Boolean>either(__ -> !drop_errors
					, 
					t -> wrap_filter_u(f).test(t));
		};
	}

	//////////////////////////////////////////////////////////////////////////////////
	
	// FUNCTION
	
	public static <T, R> Function<Either<Throwable, T>, Either<Throwable, R>> wrap(ThrowableWrapper.Function<T, R> f) {
		return err_t -> {
			return err_t.<Either<Throwable, R>>either(
					err -> Either.left(err)
					, 
					t -> wrap_e(f).apply(t)
					);
		};
	}
	
	/** Wraps a function that returns a checked throwable (Exception/error) into one that returns an unchecked one
	 * @param f - the lambda that can return a checked throwable
	 * @return - an identical function except throws unchecked instead of checked throwable
	 */
	public static <T, R> Function<T, R> wrap_u(ThrowableWrapper.Function<T, R> f) {
		return t -> {
			try {
				return f.apply(t);
			}
			catch (Throwable err) {
				throw new RuntimeException(err);
			}
		};
	}

	/** Wraps a function that returns a checked or unchecked throwable (Exception/error) into one that returns an Either<Throwable, ?> but no exception
	 * @param f - the lambda that can return a checked throwable
	 * @return - an identical function except throws unchecked instead of checked throwable
	 */
	public static <T, R> Function<T, Either<Throwable, R>> wrap_e(ThrowableWrapper.Function<T, R> f) {
		return t -> {
			try {
				return Either.right(f.apply(t));
			}
			catch (RuntimeException re) {
				return denest(re);
			}
			catch (Throwable err) {
				return Either.left(err);
			}
		};		
	}
	
	public static <T, R> F<Either<Throwable, T>, Either<Throwable, R>> wrap_fj(ThrowableWrapper.Function<T, R> f) {
		return err_t -> {
			return err_t.<Either<Throwable, R>>either(
					err -> Either.left(err)
					, 
					t -> wrap_e(f).apply(t)
					);
		};
	}
	
	/** Wraps a function that returns a checked throwable (Exception/error) into one that returns an unchecked one
	 * @param f - the lambda that can return a checked throwable
	 * @return - an identical function except throws unchecked instead of checked throwable
	 */
	public static <T, R> F<T, R> wrap_fj_u(ThrowableWrapper.Function<T, R> f) {
		return t -> {
			try {
				return f.apply(t);
			}
			catch (Throwable err) {
				throw new RuntimeException(err);
			}
		};
	}

	/** Wraps a function that returns a checked or unchecked throwable (Exception/error) into one that returns an Either<Throwable, ?> but no exception
	 * @param f - the lambda that can return a checked throwable
	 * @return - an identical function except throws unchecked instead of checked throwable
	 */
	public static <T, R> F<T, Either<Throwable, R>> wrap_fj_e(ThrowableWrapper.Function<T, R> f) {
		return t -> {
			try {
				return Either.right(f.apply(t));
			}
			catch (RuntimeException re) {
				return denest(re);
			}
			catch (Throwable err) {
				return Either.left(err);
			}
		};		
	}
	
	//////////////////////////////////////////////////////////////////////////////////
	
	// BI-FUNCTION
	
	/** Wraps a function that returns a checked throwable (Exception/error) into one that returns an unchecked one
	 * @param f - the lambda that can return a checked throwable
	 * @return - an identical function except throws unchecked instead of checked throwable
	 */
	public static <T, U, R> BiFunction<T, U, R> wrap_u(ThrowableWrapper.BiFunction<T, U, R> f) {
		return (t, u) -> {
			try {
				return f.apply(t, u);
			}
			catch (Throwable err) {
				throw new RuntimeException(err);
			}
		};
	}

	/** Wraps a function that returns a checked or unchecked throwable (Exception/error) into one that returns an Either<Throwable, ?> but no exception
	 * @param f - the lambda that can return a checked throwable
	 * @return - an identical function except throws unchecked instead of checked throwable
	 */
	public static <T, U, R> BiFunction<T, U, Either<Throwable, R>> wrap_e(ThrowableWrapper.BiFunction<T, U, R> f) {
		return (t, u) -> {
			try {
				return Either.right(f.apply(t, u));
			}
			catch (RuntimeException re) {
				return denest(re);
			}
			catch (Throwable err) {
				return Either.left(err);
			}
		};		
	}
	
	//////////////////////////////////////////////////////////////////////////////////
	
	// BINARY OPERATOR
	
	/** Wraps a function that returns a checked throwable (Exception/error) into one that returns an unchecked one
	 * @param f - the lambda that can return a checked throwable
	 * @return - an identical function except throws unchecked instead of checked throwable
	 */
	public static <T> BinaryOperator<T> op_wrap_u(ThrowableWrapper.BinaryOperator<T> f) {
		return (t1, t2) -> {
			try {
				return f.apply(t1, t2);
			}
			catch (Throwable err) {
				throw new RuntimeException(err);
			}
		};
	}
	
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////

	public static <T> Either<Throwable, T> denest(final RuntimeException re) {
		return (null == re.getCause()) ? Either.left(re) : Either.left(re.getCause());		
	}
	
	/** Collects values from the collector, and a list of errors
	 * @param value_collector
	 * @return
	 */
	public static <T, A, R> Collector<Either<Throwable, T>, ?, Tuple2<List<Throwable>, R>> collector_errs(final Collector<T, A, R> value_collector)
	{
		return collector(Collectors.<Throwable>toList(), value_collector);
	}
	
	/** Collects values from the collector, and grabs the first error encountered
	 * @param value_collector
	 * @return
	 */
	public static <T, A, R> Collector<Either<Throwable, T>, MutableAccumulator<ArrayList<Throwable>, A>, Tuple2<Optional<Throwable>, R>> collector_err(final Collector<T, A, R> value_collector)
	{
		final Collector<Throwable, ArrayList<Throwable>, Optional<Throwable>> err_collector = 
				Collector.<Throwable, ArrayList<Throwable>, Optional<Throwable>>of(
						() -> new ArrayList<Throwable>(1), 
						(acc, val) -> { if (acc.isEmpty()) acc.add(val); }, 
						(acc1, acc2) -> acc1.isEmpty() ? acc1 : acc2, 
						acc -> acc.isEmpty() ? Optional.empty() : Optional.ofNullable(acc.get(0)),
						Characteristics.UNORDERED);
		
		return collector(err_collector, value_collector);
	}
	
	/** Generic double-collector
	 * @param error_collector
	 * @param value_collector
	 * @return
	 */
	public static <A1, R1, T, A, R> Collector<Either<Throwable, T>, MutableAccumulator<A1, A>, Tuple2<R1, R>> collector(
				final Collector<Throwable, A1, R1> error_collector,
				final Collector<T, A, R> value_collector
			)
	{
		return Collector.<Either<Throwable, T>, MutableAccumulator<A1, A>, Tuple2<R1, R>>of(
				() -> new MutableAccumulator<A1, A>(error_collector.supplier().get(), value_collector.supplier().get())
				, 
				(acc, err_val) -> {
					err_val.<MutableAccumulator<A1, A>>either(
						err -> {
							error_collector.accumulator().accept(acc.a1, err);
							return null;
						},
						val -> {
							value_collector.accumulator().accept(acc.a2, val);							
							return null;
						});
				}
				, 
				(acc1, acc2) -> {
					return new MutableAccumulator<A1, A>(
							error_collector.combiner().apply(acc1.a1, acc1.a1),
							value_collector.combiner().apply(acc1.a2, acc1.a2));
					
				}
				,
				acc -> {
					return Tuples._2T(error_collector.finisher().apply(acc.a1), 
										value_collector.finisher().apply(acc.a2));
				}
				, 
				Characteristics.UNORDERED
				);
	}
	
	private static class MutableAccumulator<A1, A2> {
		MutableAccumulator(A1 a1, A2 a2) {
			this.a1 = a1; this.a2 = a2;
		}
		A1 a1;
		A2 a2;
	};
	
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////

	// UTILITY
	
	/** Alternative versions of commonly used functional interfaces
	 *  with built in checked and unchecked throwable support
	 * @author acp
	 */
	public static class ThrowableWrapper {
		public interface CheckedThrowable {
			@FunctionalInterface
			public interface Runnable<E extends Throwable> {
				void run() throws E;
			}

			@FunctionalInterface
			public interface Supplier<T, E extends Throwable> {
				T get() throws E;
			}

			@FunctionalInterface
			public interface Consumer<T, E extends Throwable> {
				void accept(T t) throws E;
			}

			@FunctionalInterface
			public interface Function<T, R, E extends Throwable> {
				R apply(T t) throws E;
			}

			@FunctionalInterface
			public interface BiFunction<T, U, R, E extends Throwable> {
				R apply(T t, U u) throws E;
			}

			@FunctionalInterface
			public interface BinaryOperator<T, E extends Throwable> {
				T apply(T t1, T t2) throws E;
			}

			@FunctionalInterface
			public interface Predicate<T, E extends Throwable> {
				boolean test(T t) throws E;
			}
		}

		@FunctionalInterface
		public interface Runnable extends CheckedThrowable.Runnable<Throwable> {}

		@FunctionalInterface
		public interface Supplier<T> extends CheckedThrowable.Supplier<T, Throwable> {}

		@FunctionalInterface
		public interface Consumer<T> extends CheckedThrowable.Consumer<T, Throwable> {}

		@FunctionalInterface
		public interface Function<T, R> extends CheckedThrowable.Function<T, R, Throwable> {}

		@FunctionalInterface
		public interface BiFunction<T, U, R> extends CheckedThrowable.BiFunction<T, U, R, Throwable> {}

		@FunctionalInterface
		public interface BinaryOperator<T> extends CheckedThrowable.BinaryOperator<T, Throwable> {}

		@FunctionalInterface
		public interface Predicate<T> extends CheckedThrowable.Predicate<T, Throwable> {}
		
	}
	
}
