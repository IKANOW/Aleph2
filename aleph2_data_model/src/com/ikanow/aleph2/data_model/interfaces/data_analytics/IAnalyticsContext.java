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
package com.ikanow.aleph2.data_model.interfaces.data_analytics;

import java.util.Collection;
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
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_import.AnnotationBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.shared.AssetStateDirectoryBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;

import fj.data.Either;
import fj.data.Validation;

/** A context library that is always passed to the IAnalyticsTechnology module and can also be 
 *  passed to the analytics library processing 
 * @author acp
 */
public interface IAnalyticsContext extends IUnderlyingService {

	//////////////////////////////////////////////////////
	
	// ANALYTICS MODULE ONLY
	
	/** (Analytic Module only) Returns context to return a service - for external clients, the corresponding library JAR must have been copied into the class file (path given by getAnalyticsContextLibraries)
	 * (NOTE: analytic technology modules do not need this, they can access the service context directly via the @Inject annotation)    
	 * @return the service context
	 */
	IServiceContext getServiceContext();	
	
	/////////////////////////////////////////////////////////////
	
	/** (Analytic Module only) If another component is requesting streaming access to the output (use checkForListeners to find out) then this utility function will output the objects
	 *  Note emitObject automatically checks for this, so this only need be called if emitObject is not being used. 
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined)
	 * @param stage - if set to Optionals.empty() or "$end" then occurs post enrichment. If set to "" or "$start" then occurs pre-enrichment. Otherwise should be the name of a module - will listen immediately after that. 
	 * @param object the object to emit represented by either Jackson JsonNode or a generic map-of-objects
	 * @returns a validation containing and error, or the emitted JsonNode if successful
	 */
	Validation<BasicMessageBean, JsonNode> sendObjectToStreamingPipeline(final Optional<DataBucketBean> bucket, final AnalyticThreadJobBean job, final Either<JsonNode, Map<String, Object>> object, final Optional<AnnotationBean> annotations);
	
	/** (Analytic Module only) For output modules for the particular technology to output objects reasonably efficient, if an output service is not available
	 *  Also supports: a) writing objects to sub-buckets (use the sub-bucket full_name - the system will fill the rest in) b) external buckets (again - only the full name is required, the system will authorize and fill in)
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined)
	 * @param job - the job for which data is being output
	 * @param object the object to emit represented by either Jackson JsonNode or a generic map-of-objects
	 * @param annotation - the generic annotation parameters can either be copied directly into the object, or appended via this bean (merging if the object also has annotatiosn)
	 * @returns a validation containing and error, or the emitted JsonNode if successful
	 */
	Validation<BasicMessageBean, JsonNode> emitObject(final Optional<DataBucketBean> bucket, final AnalyticThreadJobBean job, final Either<JsonNode, Map<String, Object>> object, final Optional<AnnotationBean> annotations);
	
	/**(Analytic Module only) Flushes any pending batch output, eg before a process exits
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined)
	 * @param job the job for which data is being output
	 * @return a undefined future that completes when the batch output completes
	 */
	CompletableFuture<?> flushBatchOutput(final Optional<DataBucketBean> bucket, final AnalyticThreadJobBean job); 
	
	//////////////////////////////////////////////////////
	
	// BOTH ANALYTICS TECHNOLOGY AND ANALYTICS MODULE - DATA INPUT/OUTPUT 
	
	/** (AnalyticsTechnology/Analytics Module) Gets the path to which this (possibly) intermediate job should write data
	 *  For non-temporary jobs that write to file, or permanent jobs with no temporal settings - a single string is returned, which is the path to which to write
	 *  For bucket output with temporal settings, 2 strings are returned - the first is the base directory, the second is a DateFormat string that is the time-based sub-directory to which files should be written
	 *  (If the bucket has no file output then no strings are returned)
	 *  Note that for any specific technology it is possible to request an outputter of the right format via getServiceOutput instead - if it exists then it will likely contain an efficient pre-built module
	 *  Or just write your own outputter calling emitOutput
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined)
	 * @param job - the job being run
	 * @return - optionally (if the job requires file-based output) the base path and the temporal suffix that can be passed to eg SimpleDateFormat 
	 */
	Optional<Tuple2<String, Optional<String>>> getOutputPath(final Optional<DataBucketBean> bucket, final AnalyticThreadJobBean job);

	/** (AnalyticsTechnology/Analytics Module) Gets the topic to which this (possibly) intermediate job should stream real-time data
	 *  This is for passing the topic into an external library - alternatively sendObjectToStreamingPipeline can be called
	 * @param bucket
	 * @param job
	 * @return
	 */
	Optional<String> getOutputTopic(final Optional<DataBucketBean> bucket, final AnalyticThreadJobBean job);
	
	
	/** (AnalyticsTechnology/Analytics Module) For jobs that read from file (either temporary or persistent), this provides a set of file paths from which to read
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined)
	 * @param job - the job being run
	 * @param job_input - the specific input to check against for the job being run (each job can have multiple inputs)
	 * @return - A collection of paths (directories) to check  
	 */
	List<String> getInputPaths(final Optional<DataBucketBean> bucket, final AnalyticThreadJobBean job, final AnalyticThreadJobBean.AnalyticThreadJobInputBean job_input);

	/** (AnalyticsTechnology/Analytics Module) For jobs that read from real-time queues, this provides a set of topics from which to read
	 *  (Will also subscribe to the bucket so that the related bucket will start streaming if it is not already)
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined)
	 * @param job - the job being run
	 * @param job_input - the specific input to check against for the job being run (each job can have multiple inputs)
	 * @return - A collection of topics (for a real-time queue) to which to subscribe (eg using ICoreDistributed.consumeAs, or a Storm spout if using Storm, or etc) 
	 */
	List<String> getInputTopics(final Optional<DataBucketBean> bucket, final AnalyticThreadJobBean job, final AnalyticThreadJobBean.AnalyticThreadJobInputBean job_input);

	/** For data services, you can either request an underlying technology (eg InputFormat for Hadoop/RichSpout for Storm/etc), or an ICrudService (of JsonNode) as a backup 
	 * @param clazz - the requested class, note all CRUD services will always return an ICrudService<JsonNode>, but more useful technology-specific higher-level constructs may also be possible
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined)
	 * @param job - the job being run
	 * @param job_input - the specific input to check against for the job being run (each job can have multiple inputs)
	 * @return an instances of the requested class if available
	 */
	<T extends IAnalyticsAccessContext<?>> Optional<T> getServiceInput(final Class<T> clazz, final Optional<DataBucketBean> bucket, final AnalyticThreadJobBean job, final AnalyticThreadJobBean.AnalyticThreadJobInputBean job_input);

	/** Requests a technology-specific output for the given data service (eg OutputFormat for Hadoop/Bolt for Storm/etc), or an IDataWriteService (of JsonNode) as a backup
	 * @param clazz - the requested class, note all CRUD services will always return an ICrudService<JsonNode>, but more useful technology-specific higher-level constructs may also be possible
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined)
	 * @param job - the job being run
	 * @param data_service - the data service to which to write (ie to which the request is being sent) 
	 * @return an instance of the requested class if available, else empty
	 */
	<T extends IAnalyticsAccessContext<?>> Optional<T> getServiceOutput(final Class<T> clazz, final Optional<DataBucketBean> bucket, final AnalyticThreadJobBean job, final String data_service);
	
	/** This checks whether another analytic job has requested a stream of objects - if so then sendObjectToStreamingPipeline can be used to forward them
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined)
	 * @param job - the job being run 
	 * @return whether another bucket is listening for this bucket
	 */
	boolean checkForListeners(final Optional<DataBucketBean> bucket, final AnalyticThreadJobBean job);
	
	//////////////////////////////////////////////////////
	
	// ANALYTICS TECHNOLOGY ONLY 
	
	/** (AnalyticsTechnology only) A list of JARs that can be copied to an external process (eg Hadoop job) to provide the result client access to this context (and other services)
	 * @services an optional set of service classes (with optionally service name - not needed unless a non-default service is needed) that are needed (only the libraries needed for the context is provided otherwise)
	 * @return the path (in a format that makes sense to IStorageService)
	 */
	List<String> getAnalyticsContextLibraries(final Optional<Set<Tuple2<Class<? extends IUnderlyingService>, Optional<String>>>> services);
	
	/** (AnalyticsTechnology only) This string should be passed into ContextUtils.getAnalyticsContext to retrieve this class from within external clients
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined) 
	 * @services an optional set of service classes (with optionally service name - not needed unless a non-default service is needed) that are needed (only the libraries needed for the context is provided otherwise)
	 * @return an opaque string that can be passed into ContextUtils.getAnalyticsContext
	 */
	String getAnalyticsContextSignature(final Optional<DataBucketBean> bucket, final Optional<Set<Tuple2<Class<? extends IUnderlyingService>, Optional<String>>>> services);

	/** (AnalyticsTechnology only) For each library defined by the thread.module_names_or_ids/thread.analytic_name_or_id, returns a FileSystem path 
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined) 
	 * @return A Future containing a map of filesystem paths with key both the name and id of the library 
	 */
	CompletableFuture<Map<String, String>> getAnalyticsLibraries(final Optional<DataBucketBean> bucket, final Collection<AnalyticThreadJobBean> jobs);
		
	//////////////////////////////////////////////////////
	
	// BOTH ANALYTICS TECHNOLOGY AND ANALYTICS MODULE - ADMIN 
	
	/** (AnalyticsTechnology/Analytics Module) Get the global (ie analytic technology-specific _not_ bucket-specific) object data store
	 * @param clazz The class of the bean or object type desired (needed so the repo can reason about the type when deciding on optimizations etc)
	 * @param collection - arbitrary string, enables the user to split the per bucket state into multiple independent collections. If left empty then returns a directory of existing collections (clazz has to be AssetStateDirectoryBean.class)
	 * @return a generic object repository visible to all users of this analytic technology
	 */
	<S> ICrudService<S> getGlobalAnalyticTechnologyObjectStore(final Class<S> clazz, final Optional<String> collection);
	
	/** (AnalyticsTechnology/Analytics Module) Get the global (ie module library-specific _not_ bucket-specific) object data store, if a module has been specified
	 * @param clazz The class of the bean or object type desired (needed so the repo can reason about the type when deciding on optimizations etc)
	 * @param collection - arbitrary string, enables the user to split the per bucket state into multiple independent collections. If left empty then returns a directory of existing collections (clazz has to be AssetStateDirectoryBean.class)
	 * @return a generic object repository visible to all users of this module
	 */
	<S> Optional<ICrudService<S>> getLibraryObjectStore(final Class<S> clazz, final String name_or_id, final Optional<String> collection);
	
	/** (AnalyticsTechnology/Analytics Module) Returns an object repository that the harvester/module can use to store arbitrary internal state. 
	 * @param clazz The class of the bean or object type desired (needed so the repo can reason about the type when deciding on optimizations etc)
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined) 
	 * @param collection - arbitrary string, enables the user to split the per bucket state into multiple independent collections. If left empty then returns a directory of existing collections (clazz has to be AssetStateDirectoryBean.class)
	 * @param type - if left blank then points to the analytics type, else can retrieve other types of object store (shared_library - bucket is then ignored, enrichment, or harvest)
	 * @return a generic object repository
	 */
	<S> ICrudService<S> getBucketObjectStore(final Class<S> clazz, final Optional<DataBucketBean> bucket, final Optional<String> collection, final Optional<AssetStateDirectoryBean.StateDirectoryType> type);
		
	/** (AnalyticsTechnology/Analytics Module) Returns the specified bucket
	 * @return The bucket that this job is running for, or Optional.empty() if that is ambiguous
	 */
	Optional<DataBucketBean> getBucket();
	
	/** (AnalyticsTechnology/Analytics Module) Returns the library bean that provided the user callback currently being executed
	 *  This library bean can be used together with the CoreManagementDb (getPerLibraryState) to store/retrieve state
	 *  To convert the library_config field to a bean, just use Optional.ofNullable(_context.getLibraryConfig().library_config()).map(j -> BeanTemplateUtils.from(j).get()) 
	 * @return the library bean that provided the user callback currently being executed
	 */
	SharedLibraryBean getTechnologyConfig();
	
	/** (AnalyticsTechnology/Analytics Module) Returns the library beans optionally specified in the analytic job's module_name_or_id/library_name_or_ids
	 *  The libraries bean can be used together with the CoreManagementDb (getPerLibraryState) to store/retrieve state, and to get entry points into modular functionality
	 *  To convert the library_config field to a bean, just use Optional.ofNullable(_context.getLibraryConfig().library_config()).map(j -> BeanTemplateUtils.from(j).get()) 
	 *  This can also be used to obtain BucketUtils.getStreamingEntryPoint or BucketUtils.getBatchEntryPoint or BucketUtils.getEntryPoint
	 * @return the library bean that provided the user callback currently being executed
	 */
	Map<String, SharedLibraryBean> getLibraryConfigs();
	
	/** (AnalyticsTechnology/Analytics Module) Returns the status bean for the specified bucket (whhch contains AnalyticThreadStatusBean) 
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined) 
	 * @return A Future containing a bean containing the harvests state and status
	 */
	CompletableFuture<DataBucketStatusBean> getBucketStatus(final Optional<DataBucketBean> bucket);
	
	/** (AnalyticsTechnology/Analytics Module) Calling this function logs a status message into he AnalyticThreadStatusBean (within the DataBucketStatusBean) that is visible to the user
	 * Note that the behavior of the context if called on another bucket than the one
	 * currently being processed is undefined
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined) 
	 * @param message The message to log
	 * @param roll_up_duplicates if set (default: true) then identical messages are not logged separately 
	 */
	void logStatusForThreadOwner(final Optional<DataBucketBean> bucket, final BasicMessageBean message, final boolean roll_up_duplicates);
	
	/** (AnalyticsTechnology/Analytics Module) Calling this function logs a status message into he AnalyticThreadStatusBean (within the DataBucketStatusBean) that is visible to the user
	 * @param bucket An optional bucket - if there is no ambiguity in the bucket then Optional.empty() can be passed (Note that the behavior of the context if called on another bucket than the one currently being processed is undefined) 
	 * @param The message to log (duplicates are "rolled up")
	 */
	void logStatusForThreadOwner(final Optional<DataBucketBean> bucket, final BasicMessageBean message);
		
	/** (AnalyticsTechnology/Analytics Module) Requests that the analytic thread be suspended - in addition to changing the analytic thread state, this will result in a call to IAnalyticsTechnologyModule.onSuspend
	 * @param bucket - An optional bucket containing the analytic thread - if there is no ambiguity in the analytic thread then Optional.empty() can be passed (Note that the behavior of the context if called on another analytic thread than the one currently being processed is undefined) 
	 */
	void emergencyDisableBucket(final Optional<DataBucketBean> bucket);
	
	/** (AnalyticsTechnology/Analytics Module) Requests that the analytic thread be suspended for the specified duration - in addition to changing the analytic thread state, this will result in a call to IAnalyticsTechnologyModule.onSuspend
	 * @param bucket - An optional bucket containing the analytic thread - if there is no ambiguity in the analytic thread then Optional.empty() can be passed (Note that the behavior of the context if called on another analytic thread than the one currently being processed is undefined)
	 * @param  quarantineDuration A string representing the duration for which to quarantine the data (eg "1 hour", "2 days", "3600")
	 */
	void emergencyQuarantineBucket(final Optional<DataBucketBean> bucket, final String quarantine_duration);	
	
	//////////////////////////////////////////////////////
	
	// NEITHER 
	
	/** (Should never be called by clients) this is used by the infrastructure to set up external contexts
	 * @param signature the string returned from getAnalyticsContextSignature
	 */
	void initializeNewContext(final String signature);	
}
