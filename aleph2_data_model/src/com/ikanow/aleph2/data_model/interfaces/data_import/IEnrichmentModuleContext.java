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
package com.ikanow.aleph2.data_model.interfaces.data_import;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public interface IEnrichmentModuleContext {

	////////////////////////////////////////////
	
	// harvest-time Configuration

	// TODO I'd like to avoid these if possible, ie put it in the JSON configuration
	// eg "dependency": "blah", (OPTIONAL) "type": "reduce" (different for streaming)
	
	//TODO specify grouping key fields - for batch (reducer), or grouping
	
	//TODO for streaming, set up grouping? hmm in theory could do exactly the same thing for batch, no?
	// ie per batch emit the aggregated data then have a follow on process that combines it
	
	////////////////////////////////////////////
	
	// run-time Initialization

	//TODO request batch size (batch only) .. hmm I'd rather this were another config param?
	
	////////////////////////////////////////////
	
	// Object transformations:

	// OK so I think the basic idea is:
	// In batch mode, you can convert an ObjectNode directly to a JsonNode
	// BUT if you're in parallel mode, which the context can calculate
	// then this results in a deep copy
	// or if you can get away with it, you can pass the object in with a 
	// set of mutations at which point it doesn't need to deep copy
	// Then if you have multiple mappers running in parallel then it 
	// will sync the objects using the ids (which are the input to the onBatchProcessing)

	// ie EITHER (or I guess also you can copy to a bean)
	
	ObjectNode mutate(JsonNode original);
	void emitObject(long id, ObjectNode mutated_json);
	
	// OR
	
	void emitObject(long id, JsonNode mutated_json, ObjectNode mutations);
	
	////////////////////////////////////////////
	
	// Object annotations:
	
	//TODO
	
	// this can be more straightforward since in most cases you're only adding
	
}
