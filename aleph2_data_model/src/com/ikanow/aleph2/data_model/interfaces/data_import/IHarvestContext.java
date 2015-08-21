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
import java.util.concurrent.CompletableFuture;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.shared.AssetStateDirectoryBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;

/** A context library that is always passed to the IHarvestTechnology module and can also be 
 *  passed to the harvest library processing (TODO (ALEPH-4): need to document how, ie copy JARs into external classpath and call ContextUtils.getHarvestContext)
 * @author acp
 */
public interface IHarvestContext extends IUnderlyingService {

	//////////////////////////////////////////////////////
	
	// HARVESTER MODULE ONLY
	
	/** (HarvesterModule only) Returns context to return a service - for external clients, the corresponding library JAR must have been copied into the class file (path given by getHarvestContextLibraries)
	 * (NOTE: harvester technology modules do not need this, they can access the service context directly via the @Inject annotation)    
	 * @return the service context
	 */
	IServiceContext getServiceContext();
	
	/////////////////////////////////////////////////////////////
	
	/** (HarvestModule only) For (near) real time harvests emit the object to the enrichment/alerting pipeline
	 * If no streaming enrichment pipeline is set up this will broadcast the object to listening streaming analytics/access - if not picked up, it will be dropped
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined)
	 * @param object the object to emit represented by Jackson JsonNode
	 */
	void sendObjectToStreamingPipeline(final Optional<DataBucketBean> bucket, final JsonNode object);
	
	/** (HarvestModule only) For (near) real time harvests emit the object to the enrichment/alerting pipeline
	 * If no streaming enrichment pipeline is set up this will broadcast the object to listening streaming analytics/access - if not picked up, it will be dropped
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined)
	 * @param object the object to emit in (possibly nested) Map<String, Object> format
	 */
	void sendObjectToStreamingPipeline(final Optional<DataBucketBean> bucket, final Map<String, Object> object);

	//////////////////////////////////////////////////////
	
	// BOTH HARVEST TECHNOLOGY AND HARVEST MODULE - DATA INPUT/OUPUT
	
	/** (HarvestTechnology/HarvestModule) A safe location into which temp data can be written without being accessed by the data import manager 
	 *  It is the responsibility of the harvest technology module to keep this area clean however.
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined) 
	 * @return the location in a string format that makes sense to the IAccessService
	 */
	String getTempOutputLocation(final Optional<DataBucketBean> bucket);
	
	/** (HarvestTechnology/HarvestModule) Once files are moved/written (preferably atomically) into this path, they become owned by the import manager
	 *  and should no longer be modified by the harvest module 
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined) 
	 * @return the location in a string format that makes sense to the IStorageService
	 */
	String getFinalOutputLocation(final Optional<DataBucketBean> bucket);	
	
	//////////////////////////////////////////////////////
	
	// HARVESTER TECHNOLOGY ONLY 
	
	/** (HarvesterTechnology only) A list of JARs that can be copied to an external process (eg Hadoop job) to provide the result client access to this context (and other services)
	 * @services an optional set of service classes (with optionally service name - not needed unless a non-default service is needed) that are needed (only the libraries needed for the context is provided otherwise)
	 * @return the path (in a format that makes sense to IStorageService)
	 */
	List<String> getHarvestContextLibraries(final Optional<Set<Tuple2<Class<? extends IUnderlyingService>, Optional<String>>>> services);
	
	/** (HarvesterTechnology only) This string should be passed into ContextUtils.getHarvestContext to retrieve this class from within external clients
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined) 
	 * @services an optional set of service classes (with optionally service name - not needed unless a non-default service is needed) that are needed (only the libraries needed for the context is provided otherwise)
	 * @return an opaque string that can be passed into ContextUtils.getHarvestContext
	 */
	String getHarvestContextSignature(final Optional<DataBucketBean> bucket, final Optional<Set<Tuple2<Class<? extends IUnderlyingService>, Optional<String>>>> services);

	/** (HarvesterTechnology only) For each library defined by the bucket.harvest_configs, returns a FileSystem path 
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined) 
	 * @return A Future containing a map of filesystem paths with key both the name and id of the library 
	 */
	CompletableFuture<Map<String, String>> getHarvestLibraries(final Optional<DataBucketBean> bucket);
	
	//////////////////////////////////////////////////////
	
	// BOTH HARVEST TECHNOLOGY AND HARVEST MODULE - ADMIN
	
	/** (HarvestTechnology/HarvestModule) Get the global (ie harvest technology-specific _not_ bucket-specific) object data store
	 * @param clazz The class of the bean or object type desired (needed so the repo can reason about the type when deciding on optimizations etc)
	 * @param collection - arbitrary string, enables the user to split the per bucket state into multiple independent collections. If left empty then returns a directory of existing collections (clazz has to be AssetStateDirectoryBean.class)
	 * @return a generic object repository visible to all users of this harvest technology
	 */
	<S> ICrudService<S> getGlobalHarvestTechnologyObjectStore(final Class<S> clazz, final Optional<String> collection);
	
	/** (HarvestTechnology/HarvestModule) Returns an object repository that the harvester/module can use to store arbitrary internal state. 
	 * @param clazz The class of the bean or object type desired (needed so the repo can reason about the type when deciding on optimizations etc)
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined) 
	 * @param collection - arbitrary string, enables the user to split the per bucket state into multiple independent collections. If left empty then returns a directory of existing collections (clazz has to be AssetStateDirectoryBean.class)
	 * @param type - if left blank then points to the harvest type, else can retrieve other types of object store (shared_library - bucket is then ignored, enrichment, or analytics)
	 * @return a generic object repository
	 */
	<S> ICrudService<S> getBucketObjectStore(final Class<S> clazz, final Optional<DataBucketBean> bucket, final Optional<String> collection, final Optional<AssetStateDirectoryBean.StateDirectoryType> type);
	
	/** (HarvestTechnology/HarvestModule) Returns the specified bucket
	 * @return The bucket that this job is running for, or Optional.empty() if that is ambiguous
	 */
	Optional<DataBucketBean> getBucket();
	
	/** (HarvestTechnology/HarvestModule) Returns the library bean that provided the user callback currently being executed
	 *  This library bean can be used together with the CoreManagementDb (getPerLibraryState) to store/retrieve state
	 *  To convert the library_config field to a bean, just use Optional.ofNullable(_context.getLibraryConfig().library_config()).map(j -> BeanTemplateUtils.from(j).get()) 
	 * @return the library bean that provided the user callback currently being executed
	 */
	SharedLibraryBean getLibraryConfig();
	
	/** (HarvestTechnology/HarvestModule) Returns the status bean for the specified bucket
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined) 
	 * @return A Future containing a bean containing the harvests state and status
	 */
	CompletableFuture<DataBucketStatusBean> getBucketStatus(final Optional<DataBucketBean> bucket);
	
	/** (HarvestTechnology/HarvestModule) Calling this function logs a status message into he DataBucketStatusBean that is visible to the user
	 * Note that the behavior of the context if called on another bucket than the one
	 * currently being processed is undefined
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined) 
	 * @param message The message to log
	 * @param roll_up_duplicates if set (default: true) then identical messages are not logged separately 
	 */
	void logStatusForBucketOwner(final Optional<DataBucketBean> bucket, final BasicMessageBean message, final boolean roll_up_duplicates);
	
	/** (HarvestTechnology/HarvestModule) Calling this function logs a status message into he DataBucketStatusBean that is visible to the user
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined) 
	 * @param The message to log (duplicates are "rolled up")
	 */
	void logStatusForBucketOwner(final Optional<DataBucketBean> bucket, final BasicMessageBean message);
	
	/** (HarvestTechnology/HarvestModule) Requests that the bucket be suspended - in addition to changing the bucket state, this will result in a call to IHarvestTechnologyModule.onSuspend
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined) 
	 */
	void emergencyDisableBucket(final Optional<DataBucketBean> bucket);
	
	/** (HarvestTechnology/HarvestModule) Requests that the bucket be suspended for the specified duration - in addition to changing the bucket state, this will result in a call to IHarvestTechnologyModule.onSuspend
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined)
	 * @param  quarantineDuration A string representing the duration for which to quarantine the data (eg "1 hour", "2 days", "3600")
	 */
	void emergencyQuarantineBucket(final Optional<DataBucketBean> bucket, final String quarantine_duration);

	//////////////////////////////////////////////////////
	
	// NEITHER 
	
	/** (Should never be called by clients) this is used by the infrastructure to set up external contexts
	 * @param signature the string returned from getHarvestContextSignature
	 */
	void initializeNewContext(final String signature);	
}
