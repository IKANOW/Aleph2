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
import java.util.List;
import java.util.Map;
import java.util.Optional;

import scala.Tuple2;

/**
 * @author Burch
 *
 */
public class QueryComponentBean {
	public List<QueryComponentBean> and() { return Optional.ofNullable(this.and).orElse(Collections.emptyList());}
	public List<QueryComponentBean> or() { return Optional.ofNullable(this.or).orElse(Collections.emptyList());}
	
	public Map<String, Object> when() { return Optional.ofNullable(this.when).orElse(Collections.emptyMap());}
	public Map<String, Object> whenNot() { return Optional.ofNullable(this.whenNot).orElse(Collections.emptyMap());}
	
	public Map<String, Collection<Object>> withAny() { return Optional.ofNullable(this.withAny).orElse(Collections.emptyMap());}
	public Map<String, Collection<Object>> withAll() { return Optional.ofNullable(this.withAll).orElse(Collections.emptyMap());}
	public Collection<String> withPresent() { return Optional.ofNullable(this.withPresent).orElse(Collections.emptyList());}
	public Collection<String> withNotPresent() { return Optional.ofNullable(this.withNotPresent).orElse(Collections.emptyList());}
	
	public Map<String, Tuple2<Object, Boolean>> rangeAbove() { return Optional.ofNullable(this.rangeAbove).orElse(Collections.emptyMap());}
	public Map<String, Tuple2<Object, Boolean>> rangeBelow() { return Optional.ofNullable(this.rangeBelow).orElse(Collections.emptyMap());}
	public Map<String, Tuple2<Tuple2<Object, Boolean>, Tuple2<Object, Boolean>>> rangeIn() { return Optional.ofNullable(this.rangeIn).orElse(Collections.emptyMap());}
	
	//if either of these have values, need to convert to multiquerycomp
	//otherwise just use singlequerycomp
	public List<QueryComponentBean> and;
	public List<QueryComponentBean> or;
	
	//operators
	public Map<String, Object> when;
	public Map<String, Object> whenNot;
	
	public Map<String, Collection<Object>> withAny;
	public Map<String, Collection<Object>> withAll;	
	public Collection<String> withPresent;
	public Collection<String> withNotPresent;
	
	public Map<String, Tuple2<Object, Boolean>> rangeAbove;
	public Map<String, Tuple2<Object, Boolean>> rangeBelow;
	public Map<String, Tuple2<Tuple2<Object, Boolean>, Tuple2<Object, Boolean>>> rangeIn;
	
	//TODO don't have SingleQueryComponent.nested represented, do we need it?
}

