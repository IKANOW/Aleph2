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

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** Utility classes for managing JSON transforms
 * @author Alex
 */
public class JsonUtils {

	/** Takes a tuple expressed as LinkedHashMap<String, Object> (by convention the Objects are primitives, JsonNode, or POJO), and where one of the objects
	 *  is a JSON representation of the original object and creates an object by folding them all together
	 *  Note the other fields of the tuple take precedence over the JSON
	 *  TODO: needs testing + test code
	 * @param in - the tuple
	 * @param mapper - the Jackson object mapper
	 * @param json_field - optional (assumed to be "" if left alone) fieldname of the string representation of the JSON
	 * @return
	 */
	public static JsonNode foldTuple(final LinkedHashMap<String, Object> in, final ObjectMapper mapper, final Optional<String> json_field) {
		return in.entrySet().stream().reduce(
				mapper.createObjectNode(), 
				Lambdas.wrap_u((acc, kv) -> kv.getKey().equals(json_field.orElse(""))
					? (ObjectNode) ((ObjectNode) mapper.readTree(kv.getValue().toString())).setAll(acc)
					: Patterns.match(kv.getValue()).<ObjectNode>andReturn()
						.when(String.class, s -> acc.put(kv.getKey(), s))
						.when(Long.class, l -> acc.put(kv.getKey(), l))
						.when(Integer.class, l -> acc.put(kv.getKey(), l))
						.when(Boolean.class, b -> acc.put(kv.getKey(), b))
						.when(Double.class, d -> acc.put(kv.getKey(), d))
						.when(JsonNode.class, j -> (ObjectNode) acc.set(kv.getKey(), j))
						.when(Float.class, f -> acc.put(kv.getKey(), f))
						.when(BigDecimal.class, f -> acc.put(kv.getKey(), f))
						.otherwise(x -> acc.putPOJO(kv.getKey(), x))),
				(acc1, acc2) -> acc1);
	}
	
	//TODO (ALEPH-3): need a NestedAccessHelper for nested access to objects that can include maps
	//(see Joern's code for the harvester...)
	
}
