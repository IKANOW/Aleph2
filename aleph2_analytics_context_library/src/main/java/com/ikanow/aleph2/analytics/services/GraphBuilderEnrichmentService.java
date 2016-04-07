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

package com.ikanow.aleph2.analytics.services;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.analytics.data_model.GraphConfigBean;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.interfaces.data_services.IGraphService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.GraphSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.SetOnce;

/** Gets the actual GraphBuilderEnrichmentService from the Graph DB then wraps all its calls
 *  Users inserting an enrichment service to handle graph insertion/merges should reference this, not the underlying technology's service
 * @author Alex
 *
 */
public class GraphBuilderEnrichmentService implements IEnrichmentBatchModule {

	protected final SetOnce<IEnrichmentBatchModule> _delegate = new SetOnce<>();
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule#onStageInitialize(com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext, com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean, scala.Tuple2, java.util.Optional)
	 */
	@Override
	public void onStageInitialize(IEnrichmentModuleContext context,
			DataBucketBean bucket, EnrichmentControlMetadataBean control,
			Tuple2<ProcessingStage, ProcessingStage> previous_next,
			Optional<List<String>> next_grouping_fields) {
		
		// Get the configured graph db service's delegate and store it
		
		final GraphConfigBean dedup_config = BeanTemplateUtils.from(Optional.ofNullable(control.config()).orElse(Collections.emptyMap()), GraphConfigBean.class).get();
		
		final GraphSchemaBean graph_schema = Optional.ofNullable(dedup_config.graph_schema_override()).orElse(bucket.data_schema().graph_schema()); //(exists by construction)
		
		context.getServiceContext()
			.getService(IGraphService.class, Optional.ofNullable(graph_schema.service_name()))
			.flatMap(graph_service -> graph_service.getUnderlyingPlatformDriver(IEnrichmentBatchModule.class, Optional.of(this.getClass().getName())))
			.ifPresent(delegate -> _delegate.set(delegate));
			;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule#onObjectBatch(java.util.stream.Stream, java.util.Optional, java.util.Optional)
	 */
	@Override
	public void onObjectBatch(Stream<Tuple2<Long, IBatchRecord>> batch,
			Optional<Integer> batch_size, Optional<JsonNode> grouping_key) {		
		_delegate.optional().ifPresent(delegate -> delegate.onObjectBatch(batch, batch_size, grouping_key));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule#onStageComplete(boolean)
	 */
	@Override
	public void onStageComplete(boolean is_original) {
		_delegate.optional().ifPresent(delegate -> delegate.onStageComplete(is_original));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule#cloneForNewGrouping()
	 */
	public IEnrichmentBatchModule cloneForNewGrouping() {
		return _delegate.optional().map(delegate -> delegate.cloneForNewGrouping()).orElse(this);
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule#validateModule(com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext, com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean)
	 */
	public Collection<BasicMessageBean> validateModule(final IEnrichmentModuleContext context, final  DataBucketBean bucket, final EnrichmentControlMetadataBean control)
	{
		return _delegate.optional().map(delegate -> delegate.validateModule(context, bucket, control)).orElse(Collections.emptyList());
	}
	
}
