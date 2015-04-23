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

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Optional;

import org.checkerframework.checker.nullness.qual.NonNull;

import scala.Tuple3;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;

/** The interface enrichment developers need to implement this interface to use JARs as enrichment modules in batch mode
 * @author acp
 */
public interface IEnrichmentBatchModule {

	/** Called when the stage (eg map or reduce) is starting
	 * @param context - a context 
	 * @param bucket - the bucket for which this enrichment is taking place
	 * @param final_stage - this is true if this is the final step before the object is stored
	 */
	void onStageInitialize(@NonNull IEnrichmentModuleContext context, DataBucketBean bucket, boolean final_stage);
	
	/** A batch of objects is ready for processing (unless one of the context.emitObjects is called, the object will be discarded)
	 * @param batch a list of (id, object, lazy binary stream) for processing 
	 */
	void onObjectBatch(@NonNull List<Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>> batch);

	/** Called when a stage is complete - enables tidying up and similar
	 */
	void onStageComplete();	
}
