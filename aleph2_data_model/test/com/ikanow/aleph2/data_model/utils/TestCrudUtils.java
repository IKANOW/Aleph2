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
import java.util.Map;
import java.util.EnumSet;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import org.junit.Test;

import scala.Tuple2;

import com.google.common.collect.LinkedHashMultimap;
import com.ikanow.aleph2.data_model.utils.CrudUtils.Operator;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

public class TestCrudUtils {

	//TODO (ALEPH-22):

	// Some utility code (which actually end up being the basis for the mongodb crud operator...)
	
	private static BasicDBObject operatorToMongoKey(String field, Tuple2<Operator, Tuple2<Object, Object>> operator_args) {
		return Patterns.matchAndReturn(operator_args)
				.when(op_args -> Operator.exists == op_args._1(), op_args -> new BasicDBObject(field, new BasicDBObject("$exists", op_args._2()._1())) )
				
				.when(op_args -> (Operator.any_of == op_args._1()), op_args -> new BasicDBObject(field, new BasicDBObject("$in", op_args._2()._1())) )
				.when(op_args -> (Operator.all_of == op_args._1()), op_args -> new BasicDBObject(field, new BasicDBObject("$all", op_args._2()._1())) )
				
				.when(op_args -> (Operator.equals == op_args._1()) && (null != op_args._2()._2()), op_args -> new BasicDBObject(field, new BasicDBObject("$ne", op_args._2()._2())) )
				.when(op_args -> (Operator.equals == op_args._1()), op_args -> new BasicDBObject(field, op_args._2()._1()) )
				
				.when(op_args -> Operator.range_open_open == op_args._1(), op_args -> {
					QueryBuilder qb = QueryBuilder.start(field);
					if (null != op_args._2()._1()) qb = qb.greaterThan(op_args._2()._1());
					if (null != op_args._2()._2()) qb = qb.lessThan(op_args._2()._2());
					return qb.get(); 
				})
				.when(op_args -> Operator.range_open_closed == op_args._1(), op_args -> {
					QueryBuilder qb = QueryBuilder.start(field);
					if (null != op_args._2()._1()) qb = qb.greaterThan(op_args._2()._1());
					if (null != op_args._2()._2()) qb = qb.lessThanEquals(op_args._2()._2());
					return qb.get(); 
				})
				.when(op_args -> Operator.range_closed_closed == op_args._1(), op_args -> {
					QueryBuilder qb = QueryBuilder.start(field);
					if (null != op_args._2()._1()) qb = qb.greaterThanEquals(op_args._2()._1());
					if (null != op_args._2()._2()) qb = qb.lessThanEquals(op_args._2()._2());
					return qb.get(); 
				})
				.when(op_args -> Operator.range_closed_open == op_args._1(), op_args -> {
					QueryBuilder qb = QueryBuilder.start(field);
					if (null != op_args._2()._1()) qb = qb.greaterThanEquals(op_args._2()._1());
					if (null != op_args._2()._2()) qb = qb.lessThan(op_args._2()._2());
					return qb.get(); 
				})
				.otherwise(op_args -> new BasicDBObject());
	}
		
	private static <T> Tuple2<DBObject,DBObject> convertToMongoQuery(QueryComponent<T> query_in) {
		LinkedHashMultimap<String, Tuple2<Operator, Tuple2<Object, Object>>> fields = query_in.getAll();
		
		final String andVsOr = Patterns.matchAndReturn(query_in.getOp())
					.when(op -> Operator.any_of == op, op -> "$or")
					.when(op -> Operator.all_of == op, op -> "$and")
					.otherwise(op -> "$and");
		
		// The actual query:

		final DBObject query_out = Patterns.matchAndReturn(fields).when(f -> f.isEmpty(), f -> new BasicDBObject())
				.otherwise(f -> f.asMap().entrySet().stream()
					.<Tuple2<String, Tuple2<Operator, Tuple2<Object, Object>>>>
						flatMap(entry -> entry.getValue().stream().map( val -> Tuples._2T(entry.getKey(), val) ) )
					.collect(		
						new Collector<Tuple2<String, Tuple2<Operator, Tuple2<Object, Object>>>, BasicDBObject, DBObject>() {
							@Override
							public Supplier<BasicDBObject> supplier() {
								return BasicDBObject::new;
							}	
							@Override
							public BiConsumer<BasicDBObject, Tuple2<String, Tuple2<Operator, Tuple2<Object, Object>>>> accumulator() {
								return (acc, entry) -> {
									Patterns.matchAndAct(acc.get(andVsOr))
										.when(l -> (null == l), l -> {
											BasicDBList dbl = new BasicDBList();
											dbl.add(operatorToMongoKey(entry._1(), entry._2()));
											acc.put(andVsOr, dbl);
										})
										.when(BasicDBList.class, l -> l.add(operatorToMongoKey(entry._1(), entry._2())))
										.otherwise(l -> {});
								};
							}	
							// Boilerplate:
							@Override
							public BinaryOperator<BasicDBObject> combiner() { return (a, b) -> { a.putAll(b.toMap()); return a; } ; }	
							@Override
							public Function<BasicDBObject, DBObject> finisher() { return acc -> acc; }
							@Override
							public Set<java.util.stream.Collector.Characteristics> characteristics() { return EnumSet.of(Characteristics.UNORDERED); }
						} ) 
					);
		
		// Meta commands
		
		BasicDBObject meta = new BasicDBObject();
		
		if (null != query_in.getLimit()) meta.put("$limit", query_in.getLimit());
		BasicDBObject sort = (null == query_in.getOrderBy()) ? null : new BasicDBObject();
		Optionals.ofNullable(query_in.getOrderBy()).stream()
			.forEach(field_order -> sort.put(field_order._1(), field_order._2()));
		if (null != sort) meta.put("$sort", sort);
		
		return Tuples._2T(query_out, meta);
	}
	
	// Test objects
	
	public static class TestBean {
		public static class NestedNestedTestBean {
			public String nested_nested_string_field() { return nested_nested_string_field; }
			
			private String nested_nested_string_field;
		}
		public static class NestedTestBean {
			public String nested_string_field() { return nested_string_field; }
			public NestedNestedTestBean nested_object() { return nested_object; }
			
			private String nested_string_field;
			private NestedNestedTestBean nested_object;
		}		
		public String string_field() { return string_field; }
		public Boolean bool_field() { return bool_field; }
		public List<NestedTestBean> nested_list() { return nested_list; }
		public Map<String, String> map() { return map; }
		
		protected TestBean() {}
		private String string_field;
		private Boolean bool_field;
		private List<NestedTestBean> nested_list;
		private Map<String, String> map;
	}
	
	@Test
	public void emptyQuery() {
		
		// No meta:
		
		QueryComponent<TestBean> query_comp_1 = CrudUtils.allOf(new TestBean()); 
		
		Tuple2<DBObject, DBObject> query_meta_1 = convertToMongoQuery(query_comp_1);
		
		assertEquals("{ }", query_meta_1._1().toString());
		assertEquals("{ }", query_meta_1._2().toString());
		
		// Meta fields
		
		QueryComponent<TestBean> query_comp_2 = CrudUtils.anyOf(TestBean.class)
													.orderBy(Tuples._2T("test_field_1", 1), Tuples._2T("test_field_2", -1));		

		Tuple2<DBObject, DBObject> query_meta_2 = convertToMongoQuery(query_comp_2);
		
		assertEquals("{ }", query_meta_2._1().toString());
		BasicDBObject expected_meta_nested = new BasicDBObject("test_field_1", 1);
		expected_meta_nested.put("test_field_2", -1);
		BasicDBObject expected_meta = new BasicDBObject("$sort", expected_meta_nested);
		assertEquals(expected_meta.toString(), query_meta_2._2().toString());
	}
	
	@Test
	public void basicSingleTest() {
		
		// Queries starting with allOf
		
		// Very simple
		
		//QueryComponent<TestBean> query_comp_1 = CrudUtils.allOf(template1); 
		
		QueryComponent<TestBean> query_comp_1 = CrudUtils.allOf(TestBean.class)
													.when(TestBean::string_field, "string_field")
													.when(TestBean::bool_field, true);
		
		Tuple2<DBObject, DBObject> query_meta = convertToMongoQuery(query_comp_1);
		
		DBObject expected = QueryBuilder.start().and(
								QueryBuilder.start("string_field").is("string_field").get(),
								QueryBuilder.start("bool_field").is(true).get()
							).get();
		
		assertEquals(expected.toString(), query_meta._1().toString());
		assertEquals("{ }", query_meta._2().toString());
		
		// Includes extra
		
		//TODO
		
		// Multiple queries on the same field
		
	}
	
	//TODO top level allOf vs anyOf
	
	//TODO nestedSingleTest
	
	//TODO multipleTest
}
