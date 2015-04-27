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

//import java.io.ByteArrayOutputStream;
//import java.util.Optional;
//import com.fasterxml.jackson.databind.JsonNode;
//import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;

/** A single element in a streaming real-time pipeline
 *  Prefer to the IEnrichmentStreamingTopology when generating highly re-usable components that can be used 
 *  in very simple processing pipelines 
 *  (NOT YET IMPLEMENTED)
 * @author acp
 */
public interface IEnrichmentStreamingModule {

	//TODO: something like
	//void onConfigure(IEnrichmentModuleContext context, DataBucketBean bucket);
	//void onInitialize(IEnrichmentModuleContext context, DataBucketBean bucket);
	//void onObject(long id, JsonNode json, Optional<ByteArrayOutputStream> binary);
	
}
