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

import java.util.Arrays;
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

import com.google.common.collect.Sets;
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
		
	//TODO: I think this is a massive waste of time, can just delete...
	public static class MongoQueryTermCollector implements Collector<Tuple2<Operator, Tuple2<Object, Object>>, BasicDBObject, DBObject> {
		final protected String _key;
		final protected BasicDBObject _acc;
		public MongoQueryTermCollector(String key, BasicDBObject acc) {
			_key = key;
			_acc = acc;
		}		
		protected static void appendAndClause(BasicDBObject acc, String key, BasicDBObject old_clause, BasicDBObject new_clause) {
			BasicDBList current_and_list = (BasicDBList) acc.getOrDefault("$and", new BasicDBList());
			current_and_list.add(old_clause);
			current_and_list.add(new_clause);
			acc.put("$and", current_and_list);
			// Remove the original, now I've added it to the $and list
			old_clause.remove(key);
			if (old_clause.isEmpty()) {
				acc.remove(key);
			}
		}
		
		@Override
		public Supplier<BasicDBObject> supplier() {
			return BasicDBObject::new;
		}
		@Override
		public BiConsumer<BasicDBObject, Tuple2<Operator, Tuple2<Object, Object>>> accumulator() {
			//Note: combining { "$and": [ {a:1} ] } + { a:2 } -> { a:2, "$and": [ {a:1} ] } is fine
			
			return (acc2, op_arg1_arg2) -> { 
				final DBObject tmp = operatorToMongoKey(_key, op_arg1_arg2);
				if (_acc.isEmpty()) {
					_acc.putAll(tmp.toMap());
				}
				else { // check if we already have an operator
					Patterns.matchAndAct(Tuples._2T(_acc.get(_key), tmp.get(_key)))
						.when(old_new -> (old_new._1() instanceof BasicDBObject) && (old_new._2() instanceof BasicDBObject),
								// Both objects, need to check for overlap
								old_new -> {
									final BasicDBObject old_ops = (BasicDBObject)old_new._1();
									final BasicDBObject new_ops = (BasicDBObject)old_new._2();
									if (Sets.intersection(old_ops.keySet(), new_ops.keySet()).isEmpty()) {
										old_ops.putAll(new_ops.toMap()); //(NOTE: modifies acc2)
									}
									else {
										appendAndClause(_acc, _key, (BasicDBObject)acc2, (BasicDBObject)tmp);
									}
								})
						.when(old_new -> (old_new._1() instanceof BasicDBObject) && (null != old_new._2()),
								// One object and one value, need to create and $and clause
								old_new -> appendAndClause(_acc, _key, (BasicDBObject)acc2, (BasicDBObject)tmp))
						.when(old_new -> (old_new._2() instanceof BasicDBObject) && (null != old_new._1()), 
								// One object and one value, need to create and $and clause
								old_new -> appendAndClause(_acc, _key, (BasicDBObject)acc2, (BasicDBObject)tmp))
						.when(old_new -> (null != old_new._1()) && (null != old_new._2()), 
								// Both values, need to create and $and clause
								old_new -> appendAndClause(_acc, _key, (BasicDBObject)acc2, (BasicDBObject)tmp))
								// Other cases, can combine
						.otherwise(old_new -> acc2.putAll(tmp.toMap()))
						;
				}
			};
		}
		@Override
		public Function<BasicDBObject, DBObject> finisher() { 
			return acc2 -> { 
				if (!acc2.isEmpty()) _acc.put(_key, acc2); 
				return acc2;
			}; 
		}
		// Boilerplate:
		@Override
		public BinaryOperator<BasicDBObject> combiner() { return (a, b) -> { a.putAll(b.toMap()); return a; } ; }	
		@Override
		public Set<java.util.stream.Collector.Characteristics> characteristics() { return EnumSet.of(Characteristics.UNORDERED); }
	};
	
	private static <T> Tuple2<DBObject,DBObject> convertToMongoQuery(QueryComponent<T> query_in) {
		LinkedHashMultimap<String, Tuple2<Operator, Tuple2<Object, Object>>> fields = query_in.getAll();

		//TODO ... handle top-level allOf vs anyOf
		
		final String andVsOr = "$and";
		
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
									/**/
									System.out.println(acc + " .. " + entry);
									
									Patterns.matchAndAct(acc.get(andVsOr))
										.when(l -> (null == l), l -> acc.put(andVsOr, Arrays.asList(operatorToMongoKey(entry._1(), entry._2()))))
										.when(BasicDBList.class, l -> l.add(operatorToMongoKey(entry._1(), entry._2())))
										.otherwise(l -> {});
								};
								//TODO remove this and all the super complex code...
								//return (acc, entry) -> entry.getValue().stream().collect(new MongoQueryTermCollector(entry.getKey(), acc));
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
		//TODO handle skip/limit/order
		
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
		
		Tuple2<DBObject, DBObject> query_meta = convertToMongoQuery(query_comp_1);
		
		assertEquals("{ }", query_meta._1().toString());
		assertEquals("{ }", query_meta._2().toString());
		
		// Meta fields
		
		TestBean template1 = ObjectTemplateUtils.build(TestBean.class).done();
		
		//TODO (empty query, use anyOf)
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
		
		/**/
		//assertEquals(expected, query_meta._1().toString());
		assertEquals("{ }", query_meta._2().toString());
		
		// Includes extra
		
		//TODO
		
		// Multiple queries on the same field
		
	}
	
	//TODO top level allOf vs anyOf
	
	//TODO nestedSingleTest
	
	//TODO multipleTest
}
