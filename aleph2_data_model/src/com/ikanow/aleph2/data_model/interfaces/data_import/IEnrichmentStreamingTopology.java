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

import java.util.LinkedHashMap;
import java.util.Map;


import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;

import scala.Tuple2;

/** The interface that developers of streaming technology developers (currently in Storm) must implement
 * in order to deploy a complete end-end streaming enrichment topology. Compared to the IEnrichmentStreamingModule,
 * this provides much more flexibility
 * @author acp
 */
public interface IEnrichmentStreamingTopology {

	/** The developer should return a technology-specific topology in Tuple2._1() and a map of options in Tuple2._2() 
	 *  (currently only backtype.storm.topology.TopologyBuilder is supported)
	 * @param bucket The bucket 
	 * @param context The enrichment context passed in by the core
	 * @return the topology and configuration for this streaming enrichment process
	 */
	Tuple2<Object, Map<String, String>> getTopologyAndConfiguration(final DataBucketBean bean, final IEnrichmentModuleContext context);
	
	/** For streaming technologies that don't inherently support JsonNode (eg Storm) - This generates the JsonNode that is the final output from the enrichment process
	 * Normally the final element will be a string representation of the entire object, which you'll convert, amend with mutations from the other fields, and then output 
	 * We have provided JsonUtils.foldTuple as a utility to enable easy use of this method
	 * @param outgoing_object - the final object output via either the success or error endpoint
	 * @return the final JsonNode to store
	 */
	JsonNode rebuildObject(final LinkedHashMap<String, Object> outgoing_object);
}
