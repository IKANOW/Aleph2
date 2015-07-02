/*******************************************************************************
* Copyright 2015, The IKANOW Open Source Project.
* 
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License, version 3,
* as published by the Free Software Foundation.
* 
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Affero General Public License for more details.
* 
* You should have received a copy of the GNU Affero General Public License
* along with this program. If not, see <http://www.gnu.org/licenses/>.
******************************************************************************/
package com.ikanow.aleph2.data_import.stream_enrichment.storm;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import scala.Tuple2;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentStreamingTopology;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.JsonUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;

/** A topology that just goes straight from the default spout to the default output bolt
 * @author Alex
 */
public class PassthroughTopology implements IEnrichmentStreamingTopology {

	protected static ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentStreamingTopology#getTopologyAndConfiguration(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext)
	 */
	@Override
	public Tuple2<Object, Map<String, String>> getTopologyAndConfiguration(final DataBucketBean bucket, final IEnrichmentModuleContext context) {		
		final TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("1", context.getTopologyEntryPoint(BaseRichSpout.class, Optional.of(bucket)));
		builder.setBolt("2", context.getTopologyStorageEndpoint(BaseRichBolt.class, Optional.of(bucket))).localOrShuffleGrouping("1");
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
