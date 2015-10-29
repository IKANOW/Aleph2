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

import java.util.Optional;
import java.util.stream.Stream;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;

/** The interface enrichment developers need to implement this interface to use JARs as enrichment modules in batch mode
 * @author acp
 */
public interface IEnrichmentBatchModule {

	/** The different types of processing stage
	 * @author Alex
	 */
	public enum ProcessingStage { input, batch, grouping, output, unknown };
	
	/** Called when the stage (eg map or reduce) is starting
	 *  Note that this is only called once per set of onObjectBatches - if multiple instances of the batch module are spawned (eg when grouping)
	 *  then clone(IEnrichmentBatchModule) is called.
	 * @param context - a context 
	 * @param bucket - the bucket for which this enrichment is taking place
	 * @param previous_next - the previous and next stages of the processing (note input/input is pre-deduplication/merge, output/output is post-deduplication/merge), note that prev only appears as grouping if the grouping key was the same 
	 */
	void onStageInitialize(final IEnrichmentModuleContext context, final  DataBucketBean bucket, final EnrichmentControlMetadataBean control, 
								final Tuple2<ProcessingStage, ProcessingStage> previous_next);
	
	/** A batch of objects is ready for processing (unless one of the context.emitObjects is called, the object will be discarded)
	 * @param batch a stream of (id, object, lazy binary stream) for processing 
	 * @param batch_size - if this is present then the stream corresponds to a set of records of known size, if not then it's a stream of unknown size (this normally only occurs
	 * @param grouping_key - if this stage has records grouped by a grouping key (represented as a JsonNode) - note that if prev stage is also grouping this indicates an intermediate grouping took place (Eg combine)
	 */
	void onObjectBatch(final Stream<Tuple2<Long, IBatchRecord>> batch, Optional<Integer> batch_size, Optional<JsonNode> grouping_key);

	/** Called when a stage is complete - enables tidying up and similar (flushing)
	 *  Is called on every instance of the IEnrichmentBatchModule spawned
	 * @param is_original - the instance on which onStageInitialize (always called last, ie can clean up any shared resources)
	 */
	void onStageComplete(boolean is_original);	
	
	/** This is called once per key in reduce type cases
	 * Can be left blank if no state is required, otherwise can copy across any initializations from onStageInitialize
	 * @param original - the instance of IEnrichmentBatchModule on which onStageInitialize was called
	 * @return the instance of IEnrichmentBatchModule on which onStageComplete is called
	 */
	default IEnrichmentBatchModule cloneForNewGrouping(IEnrichmentBatchModule original) {
		try {
			return this.getClass().newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
