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

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
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
	 * @param in - the tuple
	 * @param mapper - the Jackson object mapper
	 * @param json_field - optional fieldname of the string representation of the JSON - if not present then the last field is used (set to eg "" if there is no base object)
	 * @return
	 */
	public static JsonNode foldTuple(final LinkedHashMap<String, Object> in, final ObjectMapper mapper, final Optional<String> json_field) {
		try {
			// (do this imperatively to handle the "last element can be the base object case"
			final Iterator<Map.Entry<String,Object>> it = in.entrySet().iterator();
			ObjectNode acc = mapper.createObjectNode();
			while (it.hasNext()) {
				final Map.Entry<String, Object> kv = it.next();
				if ((json_field.isPresent() && kv.getKey().equals(json_field.get()))
						|| !json_field.isPresent() && !it.hasNext()) {
					acc = (ObjectNode) ((ObjectNode) mapper.readTree(kv.getValue().toString())).setAll(acc);
				}
				else {
					final ObjectNode acc_tmp = acc;
					Patterns.match(kv.getValue()).andAct()
								.when(String.class, s -> acc_tmp.put(kv.getKey(), s))
								.when(Long.class, l -> acc_tmp.put(kv.getKey(), l))
								.when(Integer.class, l -> acc_tmp.put(kv.getKey(), l))
								.when(Boolean.class, b -> acc_tmp.put(kv.getKey(), b))
								.when(Double.class, d -> acc_tmp.put(kv.getKey(), d))
								.when(JsonNode.class, j -> acc_tmp.set(kv.getKey(), j))
								.when(Float.class, f -> acc_tmp.put(kv.getKey(), f))
								.when(BigDecimal.class, f -> acc_tmp.put(kv.getKey(), f))
								.otherwise(x -> acc_tmp.set(kv.getKey(), BeanTemplateUtils.toJson(x)));
				}
			}
			return acc;
		}
		catch (Exception e) { throw new RuntimeException(e); } // (convert to unchecked exception)
	}
	
	/** Returns a nested sub-element from a path in dot notation, else empty 
	 * @param path in dot notation
	 * @return
	 */
	public static Optional<JsonNode> getProperty(final String path, final JsonNode obj) {
		final String[] paths = path.split("[.]");
		final String last = paths[paths.length - 1];
		JsonNode mutable_curr = obj;
		for (String p: paths) {
			final JsonNode j = mutable_curr.get(p);
			if (last == p) { 
				return Optional.ofNullable(j).filter(__ -> !j.isNull());
			}
			else if (null == j) {
				return Optional.empty();
			}
			else if (j.isArray()) { // if it's an array get the first value - for anything more complicated need jpath
				mutable_curr = j.get(0);
			}
			else if (j.isObject()) {
				mutable_curr = j;
			}
			else return Optional.empty(); // not at the end of the chain and it's not something you can map through
		}
		return Optional.empty();
	}
	
}
