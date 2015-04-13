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
import java.util.Optional;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.utils.Tuples;

public interface IHarvestContext {

	/** For each library defined by the bucket.harvest_configs, returns a FileSystem path 
	 * Note that the behavior of the context if called on another bucket than the one
	 * currently being processed is undefined
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed
	 * @return A map of filesystem paths with key both the name and id of the library 
	 */
	public Map<String, String> getHarvestLibraries(Optional<DataBucketBean> bucket);
	
	//TODO store harvest application state (a CRUD interface - need to tidy up )
	//TODO store harvest generic state (diff between this and status??)	
		
	public void logStatusForBucketOwner(String message, boolean roll_up_duplicates);
	
	//TODO get location to which to write data (what about temporal? - it'll have to be a template?)
	public String getTempOutputLocation(Optional<DataBucketBean> bucket);	
	public Tuples._2T<String, Boolean> getFinalOutputLocation(Optional<DataBucketBean> bucket);
	
	public void emergencyDisableBucket(Optional<DataBucketBean> bucket);
	public void emergencyQuarantineBucket(Optional<DataBucketBean> bucket, String quarantineDuration);

	public void sendObjectToEnrichmentAndStoragePipeline(Optional<DataBucketBean> bucket, @NonNull JsonNode object);
	public <T> void sendObjectToEnrichmentAndStoragePipeline(Optional<DataBucketBean> bucket, @NonNull T object);
	public void sendObjectToEnrichmentAndStoragePipeline(Optional<DataBucketBean> bucket, @NonNull Map<String, Object> object);
	
	//TODO a raw pipeline call
}
