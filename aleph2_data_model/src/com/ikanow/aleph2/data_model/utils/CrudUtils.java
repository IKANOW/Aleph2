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

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import org.checkerframework.checker.nullness.qual.NonNull;

import scala.Tuple2;

import com.google.common.collect.Multimap;
import com.google.common.collect.LinkedHashMultimap;
import com.ikanow.aleph2.data_model.utils.ObjectTemplateUtils.MethodNamingHelper;

/** Very simple set of query builder utilties
 * @author acp
 */
public class CrudUtils {

	public static abstract class QueryComponent<T> {
		// Just an empty parent of SingleQueryComponent and MultiQueryComponent
		private QueryComponent() {}
		
		// Public interface - read
		// THIS IS FOR CRUD INTERFACE IMPLEMENTERS ONLY
		
		public abstract Operator getOp();
		
		public abstract Long getLimit();

		public abstract List<Tuple2<String, Integer>> getOrderBy();
	}
	
	public enum Operator { all_of, any_of, exists, range_open_open, range_closed_open, range_closed_closed, range_open_closed, equals };
	
	/** Returns a query component where all of the fields in t (together with other fields added using withAny/withAll) must match
	 * @param clazz - the class of the template
	 * @return the query component "helper"
	 */
	public static <T> SingleQueryComponent<T> allOf(Class<T> clazz) {
		return new SingleQueryComponent<T>(ObjectTemplateUtils.build(clazz).done(), Operator.all_of);
	}
	/** Returns a query component where all of the fields in t (together with other fields added using withAny/withAll) must match
	 *  Recommend using the clazz version unless you are generating lots of different queries from a single template
	 * @param t - the starting set of fields (can be empty generated from default c'tor)
	 * @return the query component "helper"
	 */
	public static <T> SingleQueryComponent<T> allOf(T t) {
		return new SingleQueryComponent<T>(t, Operator.all_of);
	}
	/** Returns a query component where any of the fields in t (together with other fields added using withAny/withAll) can match
	 * @param clazz - the class of the template
	 * @return the query component "helper"
	 */
	public static <T> SingleQueryComponent<T> anyOf(Class<T> clazz) {
		return new SingleQueryComponent<T>(ObjectTemplateUtils.build(clazz).done(), Operator.any_of);
	}
	/** Returns a query component where any of the fields in t (together with other fields added using withAny/withAll) can match
	 *  Recommend using the clazz version unless you are generating lots of different queries from a single template
	 * @param t- the starting set of fields (can be empty generated from default c'tor)
	 * @return the query component "helper"
	 */
	public static <T> SingleQueryComponent<T> anyOf(T t) {
		return new SingleQueryComponent<T>(t, Operator.any_of);
	}
	
	/** Returns a "multi" query component where all of the QueryComponents in the list (and added via andAlso) must match (NOTE: each component *internally* can use ORs or ANDs)
	 * @param components - a list of query components
	 * @return the "multi" query component "helper"
	 */
	@SafeVarargs
	public static <T> MultiQueryComponent<T> allOf(SingleQueryComponent<T>... components) {
		return new MultiQueryComponent<T>(Operator.all_of, components);
	}
	
	/** Returns a "multi" query component where any of the QueryComponents in the list (and added via andAlso) can match (NOTE: each component *internally* can use ORs or ANDs)
	 * @param components - a list of query components
	 * @return the "multi" query component "helper"
	 */
	@SafeVarargs
	public static <T> MultiQueryComponent<T> anyOf(SingleQueryComponent<T>... components) {
		return new MultiQueryComponent<T>(Operator.any_of, components);
	}
	
	/** Encapsulates a very set of queries, all ANDed or ORed together
	 * @author acp
	 * @param <T> the bean type being queried
	 */
	public static class MultiQueryComponent<T> extends QueryComponent<T> {

		// Public interface - read
		// THIS IS FOR CRUD INTERFACE IMPLEMENTERS ONLY
		
		public List<SingleQueryComponent<T>> getElements() {
			return _elements;
		}
		
		public Operator getOp() {
			return _op;
		}
				
		public Long getLimit() {
			return _limit;
		}

		public List<Tuple2<String, Integer>> getOrderBy() {
			return _orderBy;
		}
		
		// Public interface - build
		
		/** More query components to match, using whichever of all/any that was first described
		 * @param components - more query components
		 * @return the "multi" query component "helper"
		 */
		@SafeVarargs
		public final MultiQueryComponent<T> also(@NonNull SingleQueryComponent<T>... components) {
			_elements.addAll(Arrays.asList(components)); 
			return this;
		}
		
		/** Limits the number of returned objects
		 * @param limit - the max number of objects to retrieve
		 * @return the "multi" query component "helper"
		 */
		public MultiQueryComponent<T> limit(long limit) {
			_limit = limit;
			return this;
		}
		/** Specifies the order in which objects will be returned
		 * @param orderList - a list of 2-tupes, first is the field string, second is +1 for ascending, -1 for descending
		 * @return the "multi" query component "helper"
		 */
		@SafeVarargs
		final public MultiQueryComponent<T> orderBy(@NonNull Tuple2<String, Integer>... orderList) {
			if (null == _orderBy) {
				_orderBy = new ArrayList<Tuple2<String, Integer>>(Arrays.asList(orderList));
			}
			else {
				_orderBy.addAll(Arrays.asList(orderList));
			}
			return this;
		}		
		
		// Implementation
		
		protected Long _limit;
		protected List<Tuple2<String, Integer>> _orderBy;
		
		List<SingleQueryComponent<T>> _elements;
		Operator _op;
		
		protected MultiQueryComponent(@NonNull Operator op, @SuppressWarnings("unchecked") SingleQueryComponent<T>... components) {
			_op = op;
			_elements = new ArrayList<SingleQueryComponent<T>>(Arrays.asList(components)); 
		}
	}
	/** Encapsulates a very simple query
	 * @author acp
	 * @param <T> the bean type being queried
	 */
	public static class  SingleQueryComponent<T> extends QueryComponent<T> {
		
		// Public interface - read
		// THIS IS FOR CRUD INTERFACE IMPLEMENTERS ONLY
		
		public T getElement() {
			return _element;
		}

		public Operator getOp() {
			return _op;
		}

		public LinkedHashMultimap<String, @NonNull Tuple2<Operator, Tuple2<Object, Object>>> getExtra() {
			return _extra;
		}

		public Long getLimit() {
			return _limit;
		}

		public List<Tuple2<String, Integer>> getOrderBy() {
			return _orderBy;
		}
		
		public LinkedHashMultimap<String, @NonNull Tuple2<Operator, Tuple2<Object, Object>>> getAll() {
			// Take all the non-null fields from the raw object and add them as op_equals
			
			final LinkedHashMultimap<String, @NonNull Tuple2<Operator, Tuple2<Object, Object>>> ret_val = LinkedHashMultimap.create();
			if (null != _extra) {
				ret_val.putAll(_extra);
			}			
			recursiveQueryBuilder_init(_element)
				.forEach(field_tuple -> ret_val.put(field_tuple._1(), Tuples._2T(Operator.equals, Tuples._2T(field_tuple._2(), null)))); 
			
			return ret_val;
		}
		
		// Public interface - build
		
		public <U> SingleQueryComponent<T> nested(@NonNull Function<T, ?> getter, @NonNull SingleQueryComponent<U> nested_query_component) {
			buildNamingHelper();
			return nested(_naming_helper.field(getter), nested_query_component);
		}
		/** Adds a collection field to the query - any of which can match
		 * @param getter - the Java8 getter for the field
		 * @param in - the collection of objects, any of which can match
		 * @return the Query Component helper
		 */
		public SingleQueryComponent<T> withAny(@NonNull Function<T, ?> getter, @NonNull Collection<?> in) {
			buildNamingHelper();
			return with(Operator.any_of, _naming_helper.field(getter), Tuples._2T(in, null));
		}
		/** Adds a collection field to the query - all of which must match
		 * @param getterthe Java8 getter for the field
		 * @param in - the collection of objects, all of which must match
		 * @return the Query Component helper
		 */
		public SingleQueryComponent<T> withAll(@NonNull Function<T, ?> getter, @NonNull Collection<?> in) {
			buildNamingHelper();
			return with(Operator.all_of, _naming_helper.field(getter), Tuples._2T(in, null));
		}
		/** Adds the requirement that the field be greater (or equal, if exclusive is false) than the specified lower bound
		 * @param getter the field name (dot notation supported)
		 * @param lower_bound - the lower bound - likely needs to be comparable, but not required by the API since that is up to the DB
		 * @param exclusive - if true, then the bound is _not_ included, if true then it is 
		 * @return the Query Component helper
		 */
		public <U> SingleQueryComponent<T> rangeAbove(@NonNull Function<T, ?> getter, @NonNull U lower_bound, boolean exclusive) {			
			buildNamingHelper();
			return rangeIn(_naming_helper.field(getter), lower_bound, exclusive, null, false);
		}
		/** Adds the requirement that the field be lesser (or equal, if exclusive is false) than the specified lower bound
		 * @param getter - the field name (dot notation supported)
		 * @param upper_bound - the upper bound - likely needs to be comparable, but not required by the API since that is up to the DB
		 * @param exclusive - if true, then the bound is _not_ included, if true then it is 
		 * @return the Query Component helper
		 */
		public <U> SingleQueryComponent<T> rangeBelow(@NonNull Function<T, ?> getter, @NonNull U upper_bound, boolean exclusive) {
			buildNamingHelper();
			return rangeIn(_naming_helper.field(getter), null, false, upper_bound, exclusive);
		}
		/** Adds the requirement that the field be within the two bounds (with exclusive/inclusive ie lower bound not included/included set by the 
		 * @param getter - the field name (dot notation supported)
		 * @param lower_bound - the lower bound - likely needs to be comparable, but not required by the API since that is up to the DB
		 * @param lower_exclusive - if true, then the bound is _not_ included, if true then it is
		 * @param upper_bound - the upper bound - likely needs to be comparable, but not required by the API since that is up to the DB
		 * @param upper_exclusive - if true, then the bound is _not_ included, if true then it is
		 * @return
		 */
		public <U> SingleQueryComponent<T> rangeIn(@NonNull Function<T, ?> getter, U lower_bound, boolean lower_exclusive, U upper_bound, boolean upper_exclusive) {
			buildNamingHelper();
			return rangeIn(_naming_helper.field(getter), lower_bound, lower_exclusive, upper_bound, upper_exclusive);
		}
		/** Adds the requirement that a field not be set to the given value 
		 * @param getter - the Java8 getter for the field
		 * @param value - the value to be negated
		 * @return the Query Component helper
		 */
		public <U> SingleQueryComponent<T> whenNot(@NonNull Function<T, ?> getter, U value) {
			buildNamingHelper();
			return with(Operator.equals, _naming_helper.field(getter), Tuples._2T(null, value));
		}
		/** Adds the requirement that a field be set to the given value 
		 * @param getter - the Java8 getter for the field
		 * @param value - the value to be negated
		 * @return the Query Component helper
		 */
		public <U> SingleQueryComponent<T> when(@NonNull Function<T, ?> getter, U value) {
			buildNamingHelper();
			return with(Operator.equals, _naming_helper.field(getter), Tuples._2T(value, null));
		}
		/** Adds the requirement that a field be present 
		 * @param getter - the Java8 getter for the field
		 * @return the Query Component helper
		 */
		public SingleQueryComponent<T> withPresent(@NonNull Function<T, ?> getter) {
			buildNamingHelper();
			return with(Operator.exists, _naming_helper.field(getter), Tuples._2T(true, null));
		}
		/** Adds the requirement that a field be missing 
		 * @param getter - the Java8 getter for the field
		 * @return the Query Component helper
		 */
		public SingleQueryComponent<T> withNotPresent(@NonNull Function<T, ?> getter) {
			buildNamingHelper();
			return with(Operator.exists, _naming_helper.field(getter), Tuples._2T(false, null));
		}
		/** Adds a collection field to the query - any of which can match
		 * @param getter - the field name (dot notation supported)
		 * @param in - the collection of objects, any of which can match
		 * @return the Query Component helper
		 */
		public SingleQueryComponent<T> withAny(@NonNull String field, @NonNull Collection<?> in) {
			return with(Operator.any_of, field, Tuples._2T(in, null));
		}
		/** Adds a collection field to the query - all of which must match
		 * @param getter - the field name (dot notation supported)
		 * @param in - the collection of objects, all of which must match
		 * @return the Query Component helper
		 */
		public SingleQueryComponent<T> withAll(@NonNull String field, @NonNull Collection<?> in) {
			return with(Operator.all_of, field, Tuples._2T(in, null));
		}
		
		/** Adds the requirement that the field be greater (or equal, if exclusive is false) than the specified lower bound
		 * @param getter - the field name (dot notation supported)
		 * @param lower_bound - the lower bound - likely needs to be comparable, but not required by the API since that is up to the DB
		 * @param exclusive - if true, then the bound is _not_ included, if true then it is 
		 * @return the Query Component helper
		 */
		public <U> SingleQueryComponent<T> rangeAbove(@NonNull String field, @NonNull U lower_bound, boolean exclusive) {			
			return rangeIn(field, lower_bound, exclusive, null, false);
		}
		/** Adds the requirement that the field be lesser (or equal, if exclusive is false) than the specified lower bound
		 * @param getter - the field name (dot notation supported)
		 * @param upper_bound - the upper bound - likely needs to be comparable, but not required by the API since that is up to the DB
		 * @param exclusive - if true, then the bound is _not_ included, if true then it is 
		 * @return the Query Component helper
		 */
		public <U> SingleQueryComponent<T> rangeBelow(@NonNull String field, @NonNull U upper_bound, boolean exclusive) {
			return rangeIn(field, null, false, upper_bound, exclusive);
		}
		/** Adds the requirement that the field be within the two bounds (with exclusive/inclusive ie lower bound not included/included set by the 
		 * @param getter - the field name (dot notation supported)
		 * @param lower_bound - the lower bound - likely needs to be comparable, but not required by the API since that is up to the DB
		 * @param lower_exclusive - if true, then the bound is _not_ included, if true then it is
		 * @param upper_bound - the upper bound - likely needs to be comparable, but not required by the API since that is up to the DB
		 * @param upper_exclusive - if true, then the bound is _not_ included, if true then it is
		 * @return
		 */
		public <U> SingleQueryComponent<T> rangeIn(@NonNull String field, U lower_bound, boolean lower_exclusive, U upper_bound, boolean upper_exclusive) {
			java.util.function.BiFunction<Boolean, Boolean, Operator> getRange =
				(lower, upper) -> {
					if (lower && upper) {
						return Operator.range_open_open;
					}
					else if (upper) {
						return Operator.range_closed_open;						
					}
					else if (lower) {
						return Operator.range_open_closed;						
					}
					else {
						return Operator.range_closed_closed;												
					}
				};
			return with(getRange.apply(lower_exclusive, upper_exclusive), field, Tuples._2T(lower_bound, upper_bound));
		}		
		
		/** Adds the requirement that a field be present 
		 * @param field - the field name (dot notation supported)
		 * @return
		 */
		public SingleQueryComponent<T> withPresent(@NonNull String field) {
			return with(Operator.exists, field, Tuples._2T(true, null));
		}
		/** Adds the requirement that a field must not be present 
		 * @param field - the field name (dot notation supported)
		 * @return
		 */
		public SingleQueryComponent<T> withNotPresent(@NonNull String field) {
			return with(Operator.exists, field, Tuples._2T(false, null));
		}
		/** Adds the requirement that a field not be set to the given value 
		 * @param field - the field name (dot notation supported)
		 * @param value - the value to be negated
		 * @return the Query Component helper
		 */
		public <U> SingleQueryComponent<T> whenNot(String field, U value) {
			return with(Operator.equals, field, Tuples._2T(null, value));
		}
		/** Adds the requirement that a field be set to the given value 
		 * @param field - the field name (dot notation supported)
		 * @param value - the value to be negated
		 * @return the Query Component helper
		 */
		public <U> SingleQueryComponent<T> when(String field, U value) {
			return with(Operator.equals, field, Tuples._2T(value, null));
		}
		/**
		 * @param field
		 * @param nested_query_component
		 * @return
		 */
		public <U> SingleQueryComponent<T> nested(@NonNull String field, @NonNull SingleQueryComponent<U> nested_query_component) {
			
			// Take all the non-null fields from the raw object and add them as op_equals
			
			recursiveQueryBuilder_init(nested_query_component._element)
				.forEach(field_tuple -> this.with(Operator.equals, field + "." + field_tuple._1(), Tuples._2T(field_tuple._2(), null))); 
			
			// Easy bit, add the extras
			
			Optionals.ofNullable(nested_query_component._extra.entries()).stream()
				.forEach(entry -> this._extra.put(field + "." + entry.getKey(), entry.getValue()));
			
			return this;
		}
				
		/** Limits the number of returned objects (ignored if the query component is used in a multi-query)
		 * @param limit the max number of objects to retrieve
		 * @return the query component "helper"
		 */
		public SingleQueryComponent<T> limit(long limit) {
			_limit = limit;
			return this;
		}
		/** Specifies the order in which objects will be returned (ignored if the query component is used in a multi-query)
		 * @param orderList a list of 2-tupes, first is the field string, second is +1 for ascending, -1 for descending
		 * @return the "multi" query component "helper"
		 */
		@SafeVarargs
		public final SingleQueryComponent<T> orderBy(@NonNull  Tuple2<String, Integer>... orderList) {
			if (null == _orderBy) {
				_orderBy = new ArrayList<Tuple2<String, Integer>>(Arrays.asList(orderList));
			}
			else {
				_orderBy.addAll(Arrays.asList(orderList));
			}
			return this;
		}
		
		// Implementation
		
		@SuppressWarnings("unchecked")
		protected void buildNamingHelper() {
			if (null == _naming_helper) {
				_naming_helper = ObjectTemplateUtils.from((Class<T>) _element.getClass());
			}
		}
		
		protected MethodNamingHelper<T> _naming_helper = null;
		protected T _element = null;
		protected Operator _op; 		
		LinkedHashMultimap<String, Tuple2<Operator, Tuple2<Object, Object>>> _extra = null;
		
		// Not supported when used in multi query
		protected Long _limit;
		protected List<Tuple2<String, Integer>> _orderBy;
		
		protected SingleQueryComponent(T t, Operator op) {
			_element = t;
			_op = op;
		}
		
		protected SingleQueryComponent<T> with(@NonNull Operator op, @NonNull String field, Tuple2<Object, Object> in) {
			if (null == _extra) {
				_extra = LinkedHashMultimap.create();
			}
			_extra.put(field, Tuples._2T(op, in));
			return this;
		}
		
		//Recursive helper:
		
		protected static Stream<Tuple2<String, Object>> recursiveQueryBuilder_init(Object bean) {
			return 	Arrays.stream(bean.getClass().getDeclaredFields())
					.filter(f -> !Modifier.isStatic(f.getModifiers())) // (ignore static fields)
					.flatMap(field_accessor -> {
						try { 
							field_accessor.setAccessible(true);
							Object val = field_accessor.get(bean);
							
							return Patterns.match(val)
									.<Stream<Tuple2<String, Object>>>andReturn()
									.when(v -> null == v, v -> Stream.empty())
									.when(String.class, v -> Stream.of(Tuples._2T(field_accessor.getName(), v)))
									.when(Number.class, v -> Stream.of(Tuples._2T(field_accessor.getName(), v)))
									.when(Boolean.class, v -> Stream.of(Tuples._2T(field_accessor.getName(), v)))
									.when(Collection.class, v -> Stream.of(Tuples._2T(field_accessor.getName(), v)))
									.when(Map.class, v -> Stream.of(Tuples._2T(field_accessor.getName(), v)))
									.when(Multimap.class, v -> Stream.of(Tuples._2T(field_accessor.getName(), v)))
									// OK if it's none of these supported types that we recognize, then assume it's a bean and recursively de-nest it
									.otherwise(v -> recursiveQueryBuilder_recurse(field_accessor.getName(), v));
						} 
						catch (Exception e) { return null; }
					});
		}
		
		protected static Stream<Tuple2<String, Object>> recursiveQueryBuilder_recurse(String parent_field, Object sub_bean) {
			final LinkedHashMultimap<String, @NonNull Tuple2<Operator, Tuple2<Object, Object>>> ret_val = CrudUtils.allOf(sub_bean).getAll();
				//(all vs and inherited from parent so ignored here)			
			
			return ret_val.entries().stream().map(e -> Tuples._2T(parent_field + "." + e.getKey(), e.getValue()._2()._1()));
		}
		

	}	
}
