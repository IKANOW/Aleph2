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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import scala.Tuple2;

/**
 * @author Burch
 *
 */
public class UpdateComponentBean {
	public QueryComponentBean query() { return Optional.ofNullable(this.query).orElse(new QueryComponentBean()); }
	
	public Map<String, Tuple2<Object, Boolean>> add() { return Optional.ofNullable(this.add).orElse(Collections.emptyMap()); }
	public Map<String, Number> increment() { return Optional.ofNullable(this.increment).orElse(Collections.emptyMap()); }
	public Map<String, Object> remove() { return Optional.ofNullable(this.remove).orElse(Collections.emptyMap()); }
	public Map<String, Object> set() { return Optional.ofNullable(this.set).orElse(Collections.emptyMap()); }
	public Collection<String> unset() { return Optional.ofNullable(this.unset).orElse(Collections.emptyList()); }
	
	public QueryComponentBean query; 
	
	public Map<String, Tuple2<Object, Boolean>> add;
	public Map<String, Number> increment;
	public Map<String, Object> remove;
	public Map<String, Object> set;
	public Collection<String> unset;
}
