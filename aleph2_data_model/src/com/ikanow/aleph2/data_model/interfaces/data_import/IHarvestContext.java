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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;

/** A context library that is always passed to the IHarvestTechnology module and can also be 
 *  passed to the harvest library processing (TODO: need to document how, ie copy JARs into external classpath and call ContextUtils.getHarvestContext)
 * @author acp
 */
public interface IHarvestContext {

	//////////////////////////////////////////////////////
	
	// HARVESTER MODULE ONLY
	
	/** (HarvesterModule only) Returns a service - for external clients, the corresponding library JAR must have been copied into the class file (path given by getHarvestContextLibraries)
	 * (NOTE: harvester technology modules do not need this, they can access the required service directly via the @Inject annotation)    
	 * @param service_clazz
	 * @return
	 */
	<I> I getService(Class<I> service_clazz);
	
	/** (HarvestModule only) For (near) real time harvests emit the object to the enrichment/alerting pipeline
	 * If no streaming enrichment pipeline is set up this will broadcast the object to listening streaming analytics/access - if not picked up, it will be dropped
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined)
	 * @param object the object to emit represented by Jackson JsonNode
	 */
	void sendObjectToStreamingPipeline(Optional<DataBucketBean> bucket, @NonNull JsonNode object);
	
	/** (HarvestModule only) For (near) real time harvests emit the object to the enrichment/alerting pipeline
	 * If no streaming enrichment pipeline is set up this will broadcast the object to listening streaming analytics/access - if not picked up, it will be dropped
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined)
	 * @param object the object to emit in "pojo" format
	 */
	<T> void sendObjectToStreamingPipeline(Optional<DataBucketBean> bucket, @NonNull T object);
	
	/** (HarvestModule only) For (near) real time harvests emit the object to the enrichment/alerting pipeline
	 * If no streaming enrichment pipeline is set up this will broadcast the object to listening streaming analytics/access - if not picked up, it will be dropped
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined)
	 * @param object the object to emit in (possibly nested) Map<String, Object> format
	 */
	void sendObjectToStreamingPipeline(Optional<DataBucketBean> bucket, @NonNull Map<String, Object> object);

	//////////////////////////////////////////////////////
	
	// HARVESTER TECHNOLOGY ONLY 
	
	/** (HarvesterTechnology only) A list of JARs that can be copied to an external process (eg Hadoop job) to provide the result client access to this context (and other services)
	 * @services an optional set of service classes that are needed (only the libraries needed for the context is provided otherwise)
	 * @return the path (in a format that makes sense to IStorageService)
	 */
	List<String> getHarvestContextLibraries(Optional<Set<Class<?>>> services);
	
	/** (HarvesterTechnology only) This string should be passed into ContextUtils.getHarvestContext to retrieve this class from within external clients
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined) 
	 * @return an opaque string that can be passed into ContextUtils.getHarvestContext
	 */
	String getHarvestContextSignature(Optional<DataBucketBean> bucket);

	/** (HarvesterTechnology only) Get the global (ie harvest technology-specific _not_ bucket-specific) configuration
	 * @return A Future containing a JsonNode representing the "harvest technology specific configuration"
	 */
	Future<JsonNode> getGlobalHarvestTecnologyConfiguration();
	
	/** (HarvesterTechnology only) For each library defined by the bucket.harvest_configs, returns a FileSystem path 
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined) 
	 * @return A Future containing a map of filesystem paths with key both the name and id of the library 
	 */
	Future<Map<String, String>> getHarvestLibraries(Optional<DataBucketBean> bucket);
	
	//////////////////////////////////////////////////////
	
	// BOTH HARVEST TECHNOLOGY AND HARVEST MODULE 
	
	/** (HarvestTechnology/HarvestModule) Returns an object repository that the harvester/module can use to store arbitrary internal state
	 * @param clazz The class of the bean or object type desired (needed so the repo can reason about the type when deciding on optimizations etc)
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined) 
	 * @param sub_collection - arbitrary string, enables the user to split the per library state into multiple independent collections
	 * @return a generic object repository
	 */
	<S> ICrudService<S> getHarvestBucketObjectStore(@NonNull Class<S> clazz, Optional<DataBucketBean> bucket, Optional<String> sub_collection);
	
	/** (HarvestTechnology/HarvestModule) Returns the state/status bean for the specified bucket
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined) 
	 * @return A Future containing a bean containing the harvests state and status
	 */
	Future<DataBucketStatusBean> getHarvestStatus(Optional<DataBucketBean> bucket);
	
	/** (HarvestTechnology/HarvestModule) Calling this function logs a status message into he HarvestStateBean that is visible to the user
	 * Note that the behavior of the context if called on another bucket than the one
	 * currently being processed is undefined
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined) 
	 * @param The message to log
	 * @param roll_up_duplicates if set (default: true) then identical messages are not logged separately 
	 */
	void logStatusForBucketOwner(Optional<DataBucketBean> bucket, @NonNull String message, boolean roll_up_duplicates);
	
	/** (HarvestTechnology/HarvestModule) Calling this function logs a status message into he HarvestStateBean that is visible to the user
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined) 
	 * @param The message to log (duplicates are "rolled up")
	 */
	void logStatusForBucketOwner(Optional<DataBucketBean> bucket, @NonNull String message);
	
	/** (HarvestTechnology/HarvestModule) A safe location into which temp data can be written without being accessed by the data import manager 
	 *  It is the responsibility of the harvest technology module to keep this area clean however.
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined) 
	 * @return the location in a string format that makes sense to the IAccessService
	 */
	String getTempOutputLocation(Optional<DataBucketBean> bucket);
	
	/** (HarvestTechnology/HarvestModule) Once files are moved/written (preferably atomically) into this path, they become owned by the import manager
	 *  and should no longer be modified by the harvest module 
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined) 
	 * @return the location in a string format that makes sense to the IStorageService
	 */
	String getFinalOutputLocation(Optional<DataBucketBean> bucket);	
	
	/** (HarvestTechnology/HarvestModule) Requests that the bucket be suspended - in addition to changing the bucket state, this will result in a call to IHarvestTechnologyModule.onSuspend
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined) 
	 */
	void emergencyDisableBucket(Optional<DataBucketBean> bucket);
	
	/** (HarvestTechnology/HarvestModule) Requests that the bucket be suspended for the specified duration - in addition to changing the bucket state, this will result in a call to IHarvestTechnologyModule.onSuspend
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined)
	 * @param  quarantineDuration A string representing the duration for which to quarantine the data (eg "1 hour", "2 days", "3600")
	 */
	void emergencyQuarantineBucket(Optional<DataBucketBean> bucket, @NonNull String quarantine_duration);

	//////////////////////////////////////////////////////
	
	// NEITHER 
	
	/** (Should never be called by clients) this is used by the infrastructure to set up external contexts
	 * @param signature the string returned from getHarvestContextSignature
	 */
	void initializeNewContext(String signature);	
}
