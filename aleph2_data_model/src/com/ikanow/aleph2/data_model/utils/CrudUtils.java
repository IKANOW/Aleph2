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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import org.checkerframework.checker.nullness.qual.NonNull;

import scala.Tuple2;

import com.google.common.collect.LinkedHashMultimap;
import com.ikanow.aleph2.data_model.utils.ObjectUtils.MethodNamingHelper;

/** Very simple set of query builder utilties
 * @author acp
 */
public class CrudUtils {

	//TODO add nested field support (see my notebook notes)
	
	public enum Operator { all_of, any_of, exists, not_exists, range_open_open, range_closed_open, range_closed_closed, range_open_closed, equals };
	
	/** Returns a query component where all of the fields in t (together with other fields added using withAny/withAll) must match
	 * @param  the starting set of fields (can be empty generated from default c'tor)
	 * @return the query component "helper"
	 */
	public static <T> QueryComponent<T> allOf(T t) {
		return new QueryComponent<T>(t, Operator.all_of);
	}
	/** Returns a query component where any of the fields in t (together with other fields added using withAny/withAll) can match
	 * @param  the starting set of fields (can be empty generated from default c'tor)
	 * @return the query component "helper"
	 */
	public static <T> QueryComponent<T> anyOf(T t) {
		return new QueryComponent<T>(t, Operator.any_of);
	}
	
	/** Returns a "multi" query component where all of the QueryComponents in the list (and added via andAlso) must match (NOTE: each component *internally* can use ORs or ANDs)
	 * @param a list of query components
	 * @return the "multi" query component "helper"
	 */
	@SuppressWarnings("unchecked")
	public static <T> MultiQueryComponent<T> allOf(QueryComponent<T>... components) {
		return new MultiQueryComponent<T>(Operator.all_of, components);
	}
	
	/** Returns a "multi" query component where any of the QueryComponents in the list (and added via andAlso) can match (NOTE: each component *internally* can use ORs or ANDs)
	 * @param a list of query components
	 * @return the "multi" query component "helper"
	 */
	@SuppressWarnings("unchecked")
	public static <T> MultiQueryComponent<T> anyOf(QueryComponent<T>... components) {
		return new MultiQueryComponent<T>(Operator.any_of, components);
	}
	
	/** Encapsulates a very set of queries, all ANDed or ORed together
	 * @author acp
	 * @param <T> the bean type being queried
	 */
	public static class  MultiQueryComponent<T> {

		// Public interface - read
		// THIS IS FOR CRUD INTERFACE IMPLEMENTERS ONLY
		
		public List<QueryComponent<T>> getElements() {
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
		 * @param more query components
		 * @return the "multi" query component "helper"
		 */
		public MultiQueryComponent<T> also(@NonNull @SuppressWarnings("unchecked") QueryComponent<T>... components) {
			_elements.addAll(Arrays.asList(components)); 
			return this;
		}
		
		/** Limits the number of returned objects
		 * @param limit the max number of objects to retrieve
		 * @return the "multi" query component "helper"
		 */
		public MultiQueryComponent<T> limit(long limit) {
			_limit = limit;
			return this;
		}
		/** Specifies the order in which objects will be returned
		 * @param orderList a list of 2-tupes, first is the field string, second is +1 for ascending, -1 for descending
		 * @return the "multi" query component "helper"
		 */
		public MultiQueryComponent<T> orderBy(@SuppressWarnings("unchecked") @NonNull Tuple2<String, Integer>... orderList) {
			if (null == _orderBy) {
				_orderBy = Arrays.asList(orderList);
			}
			else {
				_orderBy.addAll(Arrays.asList(orderList));
			}
			return this;
		}		
		
		// Implementation
		
		protected Long _limit;
		protected List<Tuple2<String, Integer>> _orderBy;
		
		List<QueryComponent<T>> _elements;
		Operator _op;
		
		protected MultiQueryComponent(@NonNull Operator op, @SuppressWarnings("unchecked") QueryComponent<T>... components) {
			_op = op;
			_elements = Arrays.asList(components); 
		}
	}
	/** Encapsulates a very simple query
	 * @author acp
	 * @param <T> the bean type being queried
	 */
	public static class  QueryComponent<T> {
		
		// Public interface - read
		// THIS IS FOR CRUD INTERFACE IMPLEMENTERS ONLY
		
		public T getElement() {
			return _element;
		}

		public Operator getOp() {
			return _op;
		}

		public LinkedHashMultimap<String, @NonNull Tuple2<Operator, Collection<?>>> getExtra() {
			return _extra;
		}

		public Long getLimit() {
			return _limit;
		}

		public List<Tuple2<String, Integer>> getOrderBy() {
			return _orderBy;
		}
		
		// Public interface - build
		
		@SuppressWarnings("unchecked")
		public <U> QueryComponent<T> nested(@NonNull Function<T, ?> getter, @NonNull QueryComponent<U> nested_query_component) {
			if (null == _naming_helper) {
				_naming_helper = ObjectUtils.from((Class<T>) _element.getClass());
			}
			return nested(_naming_helper.field(getter), nested_query_component);
		}
		/** Adds a collection field to the query - any of which can match
		 * @param the Java8 getter for the field
		 * @param the collection of objects, any of which can match
		 * @return the Query Component helper
		 */
		@SuppressWarnings("unchecked")
		public QueryComponent<T> withAny(@NonNull Function<T, ?> getter, @NonNull Collection<?> in) {
			if (null == _naming_helper) {
				_naming_helper = ObjectUtils.from((Class<T>) _element.getClass());
			}
			return with(Operator.any_of, _naming_helper.field(getter), in);
		}
		/** Adds a collection field to the query - all of which must match
		 * @param the Java8 getter for the field
		 * @param the collection of objects, all of which must match
		 * @return the Query Component helper
		 */
		@SuppressWarnings("unchecked")
		public QueryComponent<T> withAll(@NonNull Function<T, ?> getter, @NonNull Collection<?> in) {
			if (null == _naming_helper) {
				_naming_helper = ObjectUtils.from((Class<T>) _element.getClass());
			}
			return with(Operator.all_of, _naming_helper.field(getter), in);
		}
		/** Adds the requirement that the field be greater (or equal, if open is false) than the specified lower bound
		 * @param the field name (dot notation supported)
		 * @param lower_bound the lower bound - likely needs to be comparable, but not required by the API since that is up to the DB
		 * @param open if true, then the bound is _not_ included, if true then it is 
		 * @return the Query Component helper
		 */
		@SuppressWarnings("unchecked")
		public <U> QueryComponent<T> rangeAbove(@NonNull Function<T, ?> getter, @NonNull U lower_bound, boolean open) {			
			if (null == _naming_helper) {
				_naming_helper = ObjectUtils.from((Class<T>) _element.getClass());
			}
			return rangeIn(_naming_helper.field(getter), lower_bound, open, null, false);
		}
		/** Adds the requirement that the field be lesser (or equal, if open is false) than the specified lower bound
		 * @param the field name (dot notation supported)
		 * @param upper_bound the upper bound - likely needs to be comparable, but not required by the API since that is up to the DB
		 * @param open if true, then the bound is _not_ included, if true then it is 
		 * @return the Query Component helper
		 */
		@SuppressWarnings("unchecked")
		public <U> QueryComponent<T> rangeBelow(@NonNull Function<T, ?> getter, @NonNull U upper_bound, boolean open) {
			if (null == _naming_helper) {
				_naming_helper = ObjectUtils.from((Class<T>) _element.getClass());
			}
			return rangeIn(_naming_helper.field(getter), null, false, upper_bound, open);
		}
		/** Adds the requirement that the field be within the two bounds (with open/closed ie lower bound not included/included set by the 
		 * @param the field name (dot notation supported)
		 * @param lower_bound the lower bound - likely needs to be comparable, but not required by the API since that is up to the DB
		 * @param lower_open if true, then the bound is _not_ included, if true then it is
		 * @param upper_bound the upper bound - likely needs to be comparable, but not required by the API since that is up to the DB
		 * @param upper_open if true, then the bound is _not_ included, if true then it is
		 * @return
		 */
		@SuppressWarnings("unchecked")
		public <U> QueryComponent<T> rangeIn(@NonNull Function<T, ?> getter, U lower_bound, boolean lower_open, U upper_bound, boolean upper_open) {
			if (null == _naming_helper) {
				_naming_helper = ObjectUtils.from((Class<T>) _element.getClass());
			}
			return rangeIn(_naming_helper.field(getter), lower_bound, lower_open, upper_bound, upper_open);
		}
		/** Adds the requirement that a field be present 
		 * @param the Java8 getter for the field
		 * @param the collection of objects, any of which can match
		 * @return the Query Component helper
		 */
		@SuppressWarnings("unchecked")
		public QueryComponent<T> withPresent(@NonNull Function<T, ?> getter) {
			if (null == _naming_helper) {
				_naming_helper = ObjectUtils.from((Class<T>) _element.getClass());
			}
			return with(Operator.exists, _naming_helper.field(getter), null);
		}
		/** Adds the requirement that a field be missing 
		 * @param the Java8 getter for the field
		 * @param the collection of objects, any of which can match
		 * @return the Query Component helper
		 */
		@SuppressWarnings("unchecked")
		public QueryComponent<T> withNotPresent(@NonNull Function<T, ?> getter) {
			if (null == _naming_helper) {
				_naming_helper = ObjectUtils.from((Class<T>) _element.getClass());
			}
			return with(Operator.not_exists, _naming_helper.field(getter), null);
		}
		/** Adds a collection field to the query - any of which can match
		 * @param the field name (dot notation supported)
		 * @param the collection of objects, any of which can match
		 * @return the Query Component helper
		 */
		public QueryComponent<T> withAny(@NonNull String field, @NonNull Collection<?> in) {
			return with(Operator.any_of, field, in);
		}
		/** Adds a collection field to the query - all of which must match
		 * @param the field name (dot notation supported)
		 * @param the collection of objects, all of which must match
		 * @return the Query Component helper
		 */
		public QueryComponent<T> withAll(@NonNull String field, @NonNull Collection<?> in) {
			return with(Operator.all_of, field, in);
		}
		
		/** Adds the requirement that the field be greater (or equal, if open is false) than the specified lower bound
		 * @param the field name (dot notation supported)
		 * @param lower_bound the lower bound - likely needs to be comparable, but not required by the API since that is up to the DB
		 * @param open if true, then the bound is _not_ included, if true then it is 
		 * @return the Query Component helper
		 */
		public <U> QueryComponent<T> rangeAbove(@NonNull String field, @NonNull U lower_bound, boolean open) {			
			return rangeIn(field, lower_bound, open, null, false);
		}
		/** Adds the requirement that the field be lesser (or equal, if open is false) than the specified lower bound
		 * @param the field name (dot notation supported)
		 * @param upper_bound the upper bound - likely needs to be comparable, but not required by the API since that is up to the DB
		 * @param open if true, then the bound is _not_ included, if true then it is 
		 * @return the Query Component helper
		 */
		public <U> QueryComponent<T> rangeBelow(@NonNull String field, @NonNull U upper_bound, boolean open) {
			return rangeIn(field, null, false, upper_bound, open);
		}
		/** Adds the requirement that the field be within the two bounds (with open/closed ie lower bound not included/included set by the 
		 * @param the field name (dot notation supported)
		 * @param lower_bound the lower bound - likely needs to be comparable, but not required by the API since that is up to the DB
		 * @param lower_open if true, then the bound is _not_ included, if true then it is
		 * @param upper_bound the upper bound - likely needs to be comparable, but not required by the API since that is up to the DB
		 * @param upper_open if true, then the bound is _not_ included, if true then it is
		 * @return
		 */
		public <U> QueryComponent<T> rangeIn(@NonNull String field, U lower_bound, boolean lower_open, U upper_bound, boolean upper_open) {
			java.util.function.BiFunction<Boolean, Boolean, Operator> getRange =
				(upper, lower) -> {
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
			return with(getRange.apply(lower_open, upper_open), field, Arrays.asList(lower_bound, upper_bound));
		}		
		
		/** Adds the requirement that a field be present 
		 * @param the field name (dot notation supported)
		 * @param the collection of objects, any of which can match
		 * @return
		 */
		public QueryComponent<T> withPresent(@NonNull String field) {
			return with(Operator.exists, field, null);
		}
		/** Adds the requirement that a field must not be present 
		 * @param the field name (dot notation supported)
		 * @param the collection of objects, any of which can match
		 * @return
		 */
		public QueryComponent<T> withNotPresent(@NonNull String field) {
			return with(Operator.not_exists, field, null);
		}
		/**
		 * @param field
		 * @param nested_query_component
		 * @return
		 */
		public <U> QueryComponent<T> nested(@NonNull String field, @NonNull QueryComponent<U> nested_query_component) {
			
			// Take all the non-null fields from the raw object and add them as op_equals
			
			Arrays.stream(nested_query_component._element.getClass().getDeclaredFields())
				.map(field_accessor -> { 
					try { 
						Object val = field_accessor.get(nested_query_component._element);
						return val == null ? null : Tuples._2T(field_accessor.getName(), val);
					} 
					catch (Exception e) { return null; }
				})
				.filter(field_tuple -> null != field_tuple) 
				.forEach(field_tuple -> this.with(Operator.equals, field + "." + field_tuple._1(), Arrays.asList(field_tuple._2(), null))); 
			
			// Easy bit, add the extras
			
			Optionals.ofNullable(nested_query_component._extra.entries()).stream()
				.forEach(entry -> this._extra.put(field + "." + entry.getKey(), entry.getValue()));
			
			return this;
		}
		
		/** Limits the number of returned objects (ignored if the query component is used in a multi-query)
		 * @param limit the max number of objects to retrieve
		 * @return the query component "helper"
		 */
		public QueryComponent<T> limit(long limit) {
			_limit = limit;
			return this;
		}
		/** Specifies the order in which objects will be returned (ignored if the query component is used in a multi-query)
		 * @param orderList a list of 2-tupes, first is the field string, second is +1 for ascending, -1 for descending
		 * @return the "multi" query component "helper"
		 */
		public QueryComponent<T> orderBy(@SuppressWarnings("unchecked") @NonNull  Tuple2<String, Integer>... orderList) {
			if (null == _orderBy) {
				_orderBy = Arrays.asList(orderList);
			}
			else {
				_orderBy.addAll(Arrays.asList(orderList));
			}
			return this;
		}
		
		// Implementation
		
		protected MethodNamingHelper<T> _naming_helper = null;
		protected T _element = null;
		protected Operator _op; 		
		LinkedHashMultimap<String, Tuple2<Operator, Collection<?>>> _extra = null;
		
		// Not supported when used in multi query
		protected Long _limit;
		protected List<Tuple2<String, Integer>> _orderBy;
		
		protected QueryComponent(T t, Operator op) {
			_element = t;
			_op = op;
		}
		
		protected QueryComponent<T> with(@NonNull Operator op, @NonNull String field, Collection<?> in) {
			if (null == _extra) {
				_extra = LinkedHashMultimap.create();
			}
			_extra.put(field, Tuples._2T(op, in));
			return this;
		}
	}	
}
