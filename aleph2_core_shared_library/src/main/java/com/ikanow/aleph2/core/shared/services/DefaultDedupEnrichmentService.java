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
package com.ikanow.aleph2.core.shared.services;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableSet;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.DocumentSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.SetOnce;

/** An enrichment module that will perform deduplication using the provided document_schema
 * @author Alex
 */
public class DefaultDedupEnrichmentService implements IEnrichmentBatchModule {

	protected final SetOnce<ICrudService<JsonNode>> _dedup_context = new SetOnce<>();
	protected final SetOnce<IEnrichmentModuleContext> _context = new SetOnce<>();
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule#onStageInitialize(com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext, com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean, scala.Tuple2, java.util.Optional)
	 */
	@Override
	public void onStageInitialize(final IEnrichmentModuleContext context,
			final DataBucketBean bucket, 
			final EnrichmentControlMetadataBean control,
			final Tuple2<ProcessingStage, ProcessingStage> previous_next,
			final Optional<List<String>> next_grouping_fields)
	{
		_context.set(context);

		final DocumentSchemaBean doc_schema = bucket.data_schema().document_schema(); //(exists by construction)

		final DataBucketBean context_holder =
				Optional.ofNullable(doc_schema.deduplication_contexts())
					.filter(l -> !l.isEmpty()) // (if empty or null then fall back to...)				
					.map(contexts -> 
						BeanTemplateUtils.build(DataBucketBean.class)
								.with(DataBucketBean::multi_bucket_children, ImmutableSet.<String>builder().addAll(contexts))
						.done().get()
					)
					.orElse(bucket);
		
		final Optional<ICrudService<JsonNode>> maybe_read_crud = 
			context.getServiceContext().getDocumentService()
					.flatMap(ds -> ds.getDataService())
					.flatMap(ds -> ds.getReadableCrudService(JsonNode.class, Arrays.asList(context_holder), Optional.empty()))
			;
		
		maybe_read_crud.ifPresent(read_crud -> _dedup_context.set(read_crud));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule#onObjectBatch(java.util.stream.Stream, java.util.Optional, java.util.Optional)
	 */
	@Override
	public void onObjectBatch(final Stream<Tuple2<Long, IBatchRecord>> batch,
			final Optional<Integer> batch_size, 
			final Optional<JsonNode> grouping_key)
	{
		// TODO Auto-generated method stub
		
		//TODO: extract the dedup params for all the records
		
		//TODO: launch a big batch request
		
		//TODO: wait for it to finish
		
		//TODO: correlate vs incoming batch and handle collisions
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule#onStageComplete(boolean)
	 */
	@Override
	public void onStageComplete(final boolean is_original) {
		//(Nothing to do)
	}

}
