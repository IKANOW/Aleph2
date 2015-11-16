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
package com.ikanow.aleph2.data_model.interfaces.data_import;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService;
import com.ikanow.aleph2.data_model.objects.data_import.AnnotationBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.shared.AssetStateDirectoryBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;

import fj.data.Either;
import fj.data.Validation;

public interface IEnrichmentModuleContext extends IUnderlyingService {

	////////////////////////////////////////////
	
	// Batch/streaming topology
	
	/** (Topology only) This string should be passed into ContextUtils.getEnrichmentContext to retrieve this class from within external clients
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined) 
	 * @return an opaque string that can be passed into ContextUtils.getEnrichmentContext
	 */
	String getEnrichmentContextSignature(final Optional<DataBucketBean> bucket, final Optional<Set<Tuple2<Class<? extends IUnderlyingService>, Optional<String>>>> services);
	
	////////////////////////////////////////////
	
	// Batch topology context

	//TODO (ALEPH-4): not yet supported
	
	////////////////////////////////////////////
	
	// Streaming topology context
	
	/** For use in the topology generated by IEnrichmentStreamingTopology.getTopologyAndConfiguration - this stage outputs objects from the harvest stage
	 * @param clazz - the requested class of the topology entry point (eg BaseRichSpout for Storm) 
	 * @param bucket - the bucket associated with this topology (if clear from the context this can be set to Optional.empty)
	 * @return a collection of end points of the specified type (will normally only be 1, but can be >1 for cases where an analytic technology is using an enrichment engine)
	 */
	<T> Collection<Tuple2<T, String>> getTopologyEntryPoints(final Class<T> clazz, final Optional<DataBucketBean> bucket);	
	
	/** For use in the topology generated by IEnrichmentStreamingTopology.getTopologyAndConfiguration - this stage stores objects coming out of the enrichment stage
	 * @param clazz - the requested class of the topology success end point (eg BaseRichBolt for Storm) - ie objects to be stored in the platform 
	 * @param bucket - the bucket associated with this topology (if clear from the context this can be set to Optional.empty)
	 * @return an instance of the requested class
	 */
	<T> T getTopologyStorageEndpoint(final Class<T> clazz, final Optional<DataBucketBean> bucket);
	
	/** For use in the topology generated by IEnrichmentStreamingTopology.getTopologyAndConfiguration - this stage temporarily stores failed objects for further inspection
	 * @param clazz - the requested class of the topology success end point (eg BaseRichBolt for Storm) - ie objects to be stored temporarily for further inspection
	 * @param bucket - the bucket associated with this topology (if clear from the context this can be set to Optional.empty)
	 * @return an instance of the requested class
	 */
	<T> T getTopologyErrorEndpoint(final Class<T> clazz, final Optional<DataBucketBean> bucket);
	
	////////////////////////////////////////////
	
	// Batch module context
	
	// The API is somewhat restrictive in order to allow multi-threaded operations efficiently
	// - you can make an immutable JsonNode mutable and then emit it
	//   BUT if you're in parallel mode, then this results in a (potentially deep) copy 
	// - or you emit the object with a set of mutations (if this is possible) and the context will take care of syncing 
	//   multiple changes (replace values, merge maps and sets, append collections; null value unsets)
	// - the latter is always how annotations work 

	// (See also under common object output)
	
	/** Returns the next unused id, enabling new objects to be added to the enrichment processing
	 * @return the next id that safely creates a new object
	 */
	long getNextUnusedId();
	
	////////////////////////////////////////////
	
	// Streaming module context
	
	//TODO (ALEPH-4): not yet supported
	
	////////////////////////////////////////////
	
	// Common - Object Output
	// These are normally only used for batch enrichment, but can be used for streaming topology also, to avoid an additional bolt
	
	/** The first stage of the nicer-looking but less efficient emit process - make the input node mutable so you can edit it
	 * @param original - a JsonNode that will spawn a mutable object
	 * @return the mutable copy or reference of the original
	 */
	ObjectNode convertToMutable(final JsonNode original);
	
	/** The second stage of the nicer-looking but less efficient emit process - emit the mutated object
	 * @param id - the id received with the object from the EnrichmentBatchModule call (NOT USED IN STREAMING ENRICHMENT)
	 * @param mutated_json - the mutated object to emit
	 * @param annotations - a set of annotations to include in the emitted object
	 * @param grouping_key - where the grouping fields are set to unknown ("?"), any JsonNode can be used as the grouping field and can be passed in here. In other cases the behavior of setting this is undefined.
	 * @returns a validation containing and error, or the emitted JsonNode if successful
	 */
	Validation<BasicMessageBean, JsonNode> emitMutableObject(final long id, final ObjectNode mutated_json, final Optional<AnnotationBean> annotations, final Optional<JsonNode> grouping_key);
	
	/** The most efficient (slightly uglier) emit process - emit the original object with a list of applied mutations
	 * @param id - the id received with the object from the IEnrichmentBatchModule onObjectBatch or onAggregatedObjectBatch call (NOT USED IN STREAMING ENRICHMENT)
	 * @param original_json - the json doc received
	 * @param mutations - a list of "mutations" that are applied to the original_json (replace values, merge maps and sets, append collections; null value unsets)
	 * @param annotation - a set of annotations to include in the emitted object
	 * @param grouping_key - where the grouping fields are set to root ("?"), any JsonNode can be used as the grouping field and can be passed in here. In other cases the behavior of setting this is undefined.
	 * @returns a validation containing and error, or the emitted JsonNode if successful
	 */
	Validation<BasicMessageBean, JsonNode> emitImmutableObject(final long id, final JsonNode original_json, final Optional<ObjectNode> mutations, final Optional<AnnotationBean> annotations, final Optional<JsonNode> grouping_key);
	
	/** Enables the batch process to store an object that failed for future analysis
	 * @param id the id of the object received from the IEnrichmentBatchModule onObjectBatch or onAggregatedObjectBatch call (NOT USED IN STREAMING ENRICHMENT)
	 * @param original_json
	 */
	void storeErroredObject(final long id, final JsonNode original_json);
	
	/**
	 *  Supports: a) writing objects to sub-buckets (use the sub-bucket full_name - the system will fill the rest in) b) external buckets (again - only the full name is required, the system will authorize and fill in)
	 * @param bucket - the external or sub-bucket (if it's the principal bucket then will immediately output regardless of where in the enrichment pipeline this is) 
	 * @param json - the object to emit
	 * @param mutations - a list of "mutations" that are applied to the json (replace values, merge maps and sets, append collections; null value unsets)
	 * @param annotation - a set of annotations to include in the emitted object
	 * @return
	 */
	Validation<BasicMessageBean, JsonNode> externalEmit(final DataBucketBean bucket, final Either<JsonNode, Map<String, Object>> object, final Optional<AnnotationBean> annotations);
	
	/** Flushes any pending batch output, eg before a process exits
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined)
	 * @return a undefined future that completes when the batch output completes
	 */
	CompletableFuture<?> flushBatchOutput(final Optional<DataBucketBean> bucket); 
		
	////////////////////////////////////////////
	
	// Common - Management
	
	/** (HarvesterModule only) Returns context to return a service - for external clients, the corresponding library JAR must have been copied into the class file (path given by getHarvestContextLibraries)
	 * (NOTE: harvester technology modules do not need this, they can access the service context directly via the @Inject annotation)    
	 * @return the service context
	 */
	IServiceContext getServiceContext();
	
	/** (All Enrichment Types) Get the global (ie enrichment-specific _not_ bucket-specific) object data store
	 * @param clazz The class of the bean or object type desired (needed so the repo can reason about the type when deciding on optimizations etc)
	 * @param collection - arbitrary string, enables the user to split the per bucket state into multiple independent collections. If left empty then returns a directory of existing collections (clazz has to be AssetStateDirectoryBean.class)
	 * @return a generic object repository visible to all users of this harvest technology
	 */
	<S> Optional<ICrudService<S>> getGlobalEnrichmentModuleObjectStore(final Class<S> clazz, final Optional<String> collection);

	/** (All Enrichment Types) Returns an object repository that the harvester/module can use to store arbitrary internal state
	 * @param clazz The class of the bean or object type desired (needed so the repo can reason about the type when deciding on optimizations etc)
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined) 
	 * @param collection - arbitrary string, enables the user to split the per bucket state into multiple independent collections. If left empty then returns a directory of existing collections (clazz has to be AssetStateDirectoryBean.class)
	 * @param type - if left blank then points to the enrichment type, else can retrieve other types of object store (shared_library - bucket is then ignored, harvest, or analytics)
	 * @return a generic object repository
	 */
	<S> ICrudService<S> getBucketObjectStore(final Class<S> clazz, final Optional<DataBucketBean> bucket, final Optional<String> collection, final Optional<AssetStateDirectoryBean.StateDirectoryType> type);
	
	/** (All Enrichment Types) Returns the status bean for the specified bucket
	 * @return The bucket that this job is running for, or Optional.empty() if that is ambiguous
	 */
	Optional<DataBucketBean> getBucket();
	
	/** (All Enrichment Types) Returns the library bean that provided the user callback currently being executed
	 *  This library bean can be used together with the CoreManagementDb (getPerLibraryState) to store/retrieve state
	 *  To convert the library_config field to a bean, just use Optional.ofNullable(_context.getLibraryConfig().library_config()).map(j -> BeanTemplateUtils.from(j).get()) 
	 * @return the library bean that provided the user callback currently being executed - not present if no module_id_or_name is specified for the enrichment job
	 */
	Optional<SharedLibraryBean> getModuleConfig();
	
	/** (All Enrichment Types) Returns the status bean for the specified bucket
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined) 
	 * @return A Future containing a bean containing the harvests state and status
	 */
	Future<DataBucketStatusBean> getBucketStatus(final Optional<DataBucketBean> bucket);
	
	/** (All Enrichment Types) Calling this function logs a status message into he DataBucketStatusBean that is visible to the user
	 * Note that the behavior of the context if called on another bucket than the one
	 * currently being processed is undefined
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined) 
	 * @param The message to log
	 * @param roll_up_duplicates if set (default: true) then identical messages are not logged separately 
	 */
	void logStatusForBucketOwner(final Optional<DataBucketBean> bucket, final BasicMessageBean message, boolean roll_up_duplicates);
	
	/** (All Enrichment Types) Calling this function logs a status message into he DataBucketStatusBean that is visible to the user
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined) 
	 * @param The message to log (duplicates are "rolled up")
	 */
	void logStatusForBucketOwner(final Optional<DataBucketBean> bucket, final BasicMessageBean message);
	
	/** (All Enrichment Types) Requests that the bucket be suspended - in addition to changing the bucket state, this will result in a call to IHarvestTechnologyModule.onSuspend
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined) 
	 */
	void emergencyDisableBucket(final Optional<DataBucketBean> bucket);
	
	/** (All Enrichment Types) Requests that the bucket be suspended for the specified duration - in addition to changing the bucket state, this will result in a call to IHarvestTechnologyModule.onSuspend
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined)
	 * @param  quarantineDuration A string representing the duration for which to quarantine the data (eg "1 hour", "2 days", "3600")
	 */
	void emergencyQuarantineBucket(final Optional<DataBucketBean> bucket, final String quarantine_duration);

	////////////////////////////////////////////
	
	/** (Should never be called by clients) this is used by the infrastructure to set up external contexts
	 * @param signature the string returned from getEnrichmentContextSignature
	 */
	void initializeNewContext(final String signature);	

}
