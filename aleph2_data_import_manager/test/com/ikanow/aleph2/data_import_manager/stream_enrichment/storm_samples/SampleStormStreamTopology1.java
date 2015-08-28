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
package com.ikanow.aleph2.data_import_manager.stream_enrichment.storm_samples;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import scala.Tuple2;
import backtype.storm.topology.TopologyBuilder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentStreamingTopology;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.JsonUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;

public class SampleStormStreamTopology1 implements IEnrichmentStreamingTopology {

	protected static ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentStreamingTopology#getTopologyAndConfiguration(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext)
	 */
	@Override
	public Tuple2<Object, Map<String, String>> getTopologyAndConfiguration(final DataBucketBean bucket, final IEnrichmentModuleContext context) {				
		final TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout1", new SampleWebReaderSpout("https://raw.githubusercontent.com/IKANOW/Aleph2/master/README.md"));
		builder.setBolt("bolt1", new SampleWordParserBolt()).shuffleGrouping("spout1");
		return Tuples._2T(builder.createTopology(), Collections.emptyMap());
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentStreamingTopology#rebuildObject(java.lang.Object, java.util.function.Function)
	 */
	@Override
	public <O> JsonNode rebuildObject(final O raw_outgoing_object, final Function<O, LinkedHashMap<String, Object>> generic_outgoing_object_builder) {
		return JsonUtils.foldTuple(generic_outgoing_object_builder.apply(raw_outgoing_object), _mapper, Optional.empty());
	}

}
