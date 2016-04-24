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
package com.ikanow.aleph2.analytics.services;

import java.util.Optional;
import java.util.stream.Stream;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.JsonUtils;

import fj.data.Either;


/** Default Batch enrichment module.
 * @author jfreydank
 *
 */
public class PassthroughService implements IEnrichmentBatchModule {
	private static final Logger logger = LogManager.getLogger(PassthroughService.class);
	public static final String OUTPUT_TO_FIELDNAME = "output";
	public static final String OUTPUT_TO_INTERNAL = "$$internal";
	public static final String OUTPUT_TO_STOP = "$$stop";
	
	protected IEnrichmentModuleContext _context;
	protected DataBucketBean _bucket;
	protected Tuple2<ProcessingStage, ProcessingStage> _previous_next;
	protected String _output;
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule#onStageInitialize(com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext, com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, boolean)
	 */
	@Override
	public void onStageInitialize(IEnrichmentModuleContext context, DataBucketBean bucket, EnrichmentControlMetadataBean control, 
			final Tuple2<ProcessingStage, ProcessingStage> previous_next, final Optional<List<String>> next_grouping_fields)
	{			
		logger.debug("BatchEnrichmentModule.onStageInitialize:"+ context+", DataBucketBean:"+ bucket+", prev_next:"+previous_next);
		this._context = context;
		this._bucket = bucket;
		this._previous_next = previous_next;
		
		_output = Optional.ofNullable(control.config()).map(cfg -> cfg.get(OUTPUT_TO_FIELDNAME)).map(s -> s.toString()).filter(s -> !s.isEmpty()).orElse(OUTPUT_TO_INTERNAL);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule#onObjectBatch(java.util.stream.Stream, java.util.Optional, java.util.Optional)
	 */
	@Override
	public void onObjectBatch(final Stream<Tuple2<Long, IBatchRecord>> batch, Optional<Integer> batch_size, Optional<JsonNode> grouping_key) {
		if (logger.isDebugEnabled()) logger.debug("BatchEnrichmentModule.onObjectBatch:" + batch_size);
		batch.forEach(t2 -> {
			handleObject(t2, grouping_key, _output, _context);
		}); // for 
	}

	/** Handles an individual object
	 * @param object
	 * @param grouping_key
	 * @param output
	 */
	protected static void handleObject(final Tuple2<Long, IBatchRecord> t2, final Optional<JsonNode> grouping_key, final String output, final IEnrichmentModuleContext context) {
		// not sure what to do with streaming (probably binary) data - probably will have to just ignore it in default mode?
		// (the alternative is to build Tika directly in? or maybe dump it directly in .. not sure how Jackson manages raw data?)
		if (OUTPUT_TO_INTERNAL.equals(output)) {
			context.emitImmutableObject(t2._1(), t2._2().getJson(), Optional.empty(), Optional.empty(), grouping_key);				
		}
		else if (OUTPUT_TO_STOP.equals(output)) {
			//(do nothing)
		}
		else if (output.startsWith("$")) {
			JsonUtils.getProperty(output.substring(1), t2._2().getJson()).filter(j -> j.isTextual()).map(j -> j.asText()).filter(p -> !p.startsWith("$")).ifPresent(p -> {
				handleObject(t2, grouping_key, p, context);
			});
		}
		else { // external emit...
			final DataBucketBean bucket = BeanTemplateUtils.build(DataBucketBean.class).with(DataBucketBean::full_name, output).done().get();
			context.externalEmit(bucket, Either.left(t2._2().getJson()), Optional.empty());
		}
		
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule#onStageComplete(boolean)
	 */
	@Override
	public void onStageComplete(boolean is_original) {
		logger.debug("BatchEnrichmentModule.onStageComplete()");
	}
}
