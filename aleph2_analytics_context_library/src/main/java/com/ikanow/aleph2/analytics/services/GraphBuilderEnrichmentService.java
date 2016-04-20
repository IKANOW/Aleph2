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
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.SetOnce;

/** Gets the actual GraphBuilderEnrichmentService from the Graph DB then wraps all its calls
 *  Users inserting an enrichment service to handle graph insertion/merges should reference this, not the underlying technology's service
 * @author Alex
 *
 */
public class GraphBuilderEnrichmentService implements IEnrichmentBatchModule {

	protected final SetOnce<Boolean> _enabled = new SetOnce<>();
	protected final SetOnce<IEnrichmentBatchModule> _delegate = new SetOnce<>();
	protected final SetOnce<IEnrichmentModuleContext> _context = new SetOnce<>();
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule#onStageInitialize(com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext, com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean, scala.Tuple2, java.util.Optional)
	 */
	@Override
	public void onStageInitialize(IEnrichmentModuleContext context,
			DataBucketBean bucket, EnrichmentControlMetadataBean control,
			Tuple2<ProcessingStage, ProcessingStage> previous_next,
			Optional<List<String>> next_grouping_fields) {
		
		_context.set(context);
		
		final GraphConfigBean dedup_config = BeanTemplateUtils.from(Optional.ofNullable(control.config()).orElse(Collections.emptyMap()), GraphConfigBean.class).get();
		
		// Check if enabled
		final Optional<GraphSchemaBean> maybe_graph_schema = Optional.ofNullable(dedup_config.graph_schema_override()).map(Optional::of)
																.orElse(Optionals.of(() -> bucket.data_schema().graph_schema())); //(exists by construction)
		
		_enabled.set(maybe_graph_schema.map(gs -> Optional.ofNullable(gs.enabled()).orElse(true)).orElse(false));
		
		if (_enabled.get()) {
			// Get the configured graph db service's delegate and store it
						
			final GraphSchemaBean graph_schema = maybe_graph_schema.get(); //(exists by construction)
			
			context.getServiceContext()
				.getService(IGraphService.class, Optional.ofNullable(graph_schema.service_name()))
				.flatMap(graph_service -> graph_service.getUnderlyingPlatformDriver(IEnrichmentBatchModule.class, Optional.of(this.getClass().getName())))
				.ifPresent(delegate -> _delegate.set(delegate));
				;
				
			_delegate.optional().ifPresent(delegate -> delegate.onStageInitialize(context, bucket, control, previous_next, next_grouping_fields));
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule#onObjectBatch(java.util.stream.Stream, java.util.Optional, java.util.Optional)
	 */
	@Override
	public void onObjectBatch(Stream<Tuple2<Long, IBatchRecord>> batch,
			Optional<Integer> batch_size, Optional<JsonNode> grouping_key)
	{
		if (_enabled.get()) { // Also process +annoying hack to ensure the stream is also emitted normally
			
			_delegate.optional().ifPresent(delegate -> delegate.onObjectBatch(
					batch.peek(t2 -> _context.get().emitImmutableObject(t2._1(), t2._2().getJson(), Optional.empty(), Optional.empty(), grouping_key)), 
					batch_size, grouping_key));
		}
		try { // Passthrough if the stream hasn't been processed (ie not enabled), else harmless error
			batch.forEach(t2 -> _context.get().emitImmutableObject(t2._1(), t2._2().getJson(), Optional.empty(), Optional.empty(), grouping_key));			
		}
		catch (IllegalStateException e) {} // just means the 
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
