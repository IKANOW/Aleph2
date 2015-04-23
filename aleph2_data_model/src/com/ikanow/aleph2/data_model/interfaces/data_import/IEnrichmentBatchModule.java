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

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Optional;

import scala.Tuple3;

import com.fasterxml.jackson.databind.JsonNode;
//TODO
//import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;

/** The interface enrichment developers need to implement this interface to use JARs as enrichment modules in batch mode
 * @author acp
 */
public interface IEnrichmentBatchModule {

	//TODO (would like to get rid of this to avoid classpath pollution?)
	void onConfigure(IEnrichmentModuleContext context, DataBucketBean bucket);
	
	//TODO when the actual processing starts up
	void onInitialize(IEnrichmentModuleContext context, DataBucketBean bucket);
	
	//TODO need an enrichment utils that enables annotation
	
	//TODO what about M/R?
	
	//TODO what about if my objects contain some binary component?!
	
	//TODO do I want this to be an Iterable? Stream?!
	//TODO stream does have the advantage that I can always make reducers batch...
	void onObjectBatch(List<Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>> batch);

	//TODO can't reduce if you have a byte stream, you need to map that away...
	//TODO have a boolean for "intermediate"? why doesn't this include the (Long) doc "handle"?
	void onReducedBatch(List<JsonNode> batch);
	
	void onComplete();
	
	//TODO delete object
	
	//TODO enrichment modules are always run from within core-defined technologies
	// so they have their own interface, unlike harvest

	//TODO add annotation
}
