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
import java.util.Optional;


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
	
	/** For streaming technologies that don't inherently support JsonNode (eg Storm) - For every object submitted to the streaming topology, this function is applied to generate a simpler object more amenable to being passed around
	 * It is recommended to make the final element inserted be the entire object in string format (see incoming_object_string)
	 * @param incoming_object - the incoming "raw" object to decompose into simpler format
	 * @param incoming_object_string - it will often be the case that one of the tuple elements should be the entire message. If the framework has the object in string form, it will pass it in here to avoid a spurious serialization. If it does not, the client will have to deserialize the object if he wants it in the stream object.
	 * @return an object representing the simplified object (eg in Storm the keys will be used to declare the fields)
	 */
	LinkedHashMap<String, Object> decomposeIncomingObject(final JsonNode incoming_object, final Optional<String> incoming_object_string);
	
	/** For streaming technologies that don't inherently support JsonNode (eg Storm) - This does the opposite of decomposeIncomingObject - it generates the JsonNode that is the final output from the enrichment process
	 * Normally the final element will be a string representation of the entire object, which you'll convert, amend with mutations from the other fields, and then output 
	 * TODO (ALEPH-4): build a util that lets you fold all the fields into the last one (converted from String)
	 * @param outgoing_object - the final object output via either the success or error endpoint
	 * @return the final JsonNode to store
	 */
	JsonNode rebuildObject(final LinkedHashMap<String, Object> outgoing_object);
}
