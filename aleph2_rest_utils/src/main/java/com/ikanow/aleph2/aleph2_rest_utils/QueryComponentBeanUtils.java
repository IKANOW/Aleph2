/*******************************************************************************
 * Copyright 2016, The IKANOW Open Source Project.
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
package com.ikanow.aleph2.aleph2_rest_utils;

import java.util.Optional;
import java.util.stream.Collectors;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.BeanUpdateComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.SingleQueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent;

/**
 * @author Burch
 *
 */
public class QueryComponentBeanUtils {
	/**
	 * Converts our simple QueryComponentBean into a QueryComponent composed of SingleQueryComponents and MultiQueryComponents.
	 * Note: if an 'and' or 'or' are present in the current stage of the QueryComponentBean, we ignore anything in the operator
	 * fields (because a QueryComponent can only be a SingleQueryComponent of operations OR a MultiQueryComponent composed of ands and ors (no operations)).
	 * If 'and' and 'or' are both specified, we default to 'and'.
	 * 
	 * @param toConvert
	 * @param clazz
	 * @return
	 */
	public static <T> QueryComponent<T> convertQueryComponentBeanToComponent(final QueryComponentBean toConvert, Class<T> clazz, Optional<Long> limit) {
		if ( ( !toConvert.and().isEmpty()) ) {
			//is multi and query
			return CrudUtils.allOf(toConvert.and().stream().map(a->convertQueryComponentBeanToComponent(a, clazz, limit)).collect(Collectors.toList()));
		} else if ( !toConvert.or().isEmpty() ) {
			//is multi or query
			return CrudUtils.anyOf(toConvert.or().stream().map(a->convertQueryComponentBeanToComponent(a, clazz, limit)).collect(Collectors.toList()));			
		} else {
			//otherwise create a singleQuery
			return getSingleQueryComponent(toConvert, clazz, limit);
		}
	}
	
	public static <T> Tuple2<QueryComponent<T>,UpdateComponent<T>> convertUpdateComponentBeanToUpdateComponent(final UpdateComponentBean toConvert, Class<T> clazz) {
		@SuppressWarnings("unchecked")
		final BeanUpdateComponent<T> update = JsonNode.class.isAssignableFrom(clazz) ? (BeanUpdateComponent<T>) CrudUtils.update() : CrudUtils.update(clazz);		
		
		//add all possible ops
		toConvert.add().entrySet().stream().forEach(e->update.add(e.getKey(), e.getValue()._1, e.getValue()._2));
		toConvert.increment().entrySet().stream().forEach(e->update.increment(e.getKey(), e.getValue()));
		toConvert.remove().entrySet().stream().forEach(e->update.remove(e.getKey(), e.getValue()));
		toConvert.set().entrySet().stream().forEach(e->update.set(e.getKey(), e.getValue()));
		toConvert.unset().stream().forEach(e->update.unset(e));
		
		//return tuple<query, update>
		return new Tuple2<CrudUtils.QueryComponent<T>, CrudUtils.UpdateComponent<T>>(convertQueryComponentBeanToComponent(toConvert.query(), clazz, Optional.empty()), null);
	}
	
	/**
	 * Returns an instance of CrudUtils.allOf(clazz), if clazz is a JsonNode returns CrudUtils.allOf().
	 * 
	 * If the object is not a bean, this will exception out.
	 * @param clazz
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private static <T> SingleQueryComponent<T> getSingleQueryComponent(final QueryComponentBean toConvert, final Class<T> clazz, Optional<Long> limit ) {
		final SingleQueryComponent<T> query = JsonNode.class.isAssignableFrom(clazz) ? (SingleQueryComponent<T>) CrudUtils.allOf() : CrudUtils.allOf(clazz);	

		//add all the possible ops
		toConvert.when().entrySet().stream().forEach(e->query.when(e.getKey(), e.getValue()));	//equals
		toConvert.whenNot().entrySet().stream().forEach(e->query.whenNot(e.getKey(), e.getValue()));	//not equals
		
		toConvert.withAll().entrySet().stream().forEach(e->query.withAll(e.getKey(), e.getValue()));	//equals all
		toConvert.withAny().entrySet().stream().forEach(e->query.withAny(e.getKey(), e.getValue()));	//equals any		
		toConvert.withPresent().stream().forEach(e->query.withPresent(e)); //exists
		toConvert.withNotPresent().stream().forEach(e->query.withNotPresent(e));	//not exists
		
		toConvert.rangeAbove().entrySet().stream().forEach(e->query.rangeAbove(e.getKey(), e.getValue()._1, e.getValue()._2)); // >=
		toConvert.rangeBelow().entrySet().stream().forEach(e->query.rangeAbove(e.getKey(), e.getValue()._1, e.getValue()._2)); // <=
		toConvert.rangeIn().entrySet().stream().forEach(e->query.rangeIn(e.getKey(), e.getValue()._1._1, e.getValue()._1._2, e.getValue()._2._1, e.getValue()._2._2)); //>= AND <=
		
		limit.map(l->query.limit(l));
		
		return query;
	}
	
	//TODO do I need to convert the other way ever? System -> API -> Json -> user? I don't know why a user would need to be returned a query/update so I don't think so
}
