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

import java.util.Map;

import scala.Tuple2;

public interface IEnrichmentStreamingTopology {

	//TODO should this be an IEnrichmentStreamingModuleContext? or in fact an IEnrichmentStreamingTopologyContext
	// with just the stuff needed to _build_ the topology, and then an accessor to get a IEnrichmentStreamingModuleContext?
	
	/** The developer should return a technology-specific topology in Tuple2._1() and a map of options in Tuple2._2() 
	 *  (currently only backtype.storm.topology.TopologyBuilder is supported)
	 * @param context The enrichment context passed in by the core
	 * @return the topology and configuration for this streaming enrichment process
	 */
	Tuple2<Object, Map<String, String>> getTopologyAndConfiguration(IEnrichmentModuleContext context);
}
