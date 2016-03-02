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
package com.ikanow.aleph2.data_model.interfaces.shared_services;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;

/** Provides an optional generic data service interface
 * @author Alex
 */
public interface IDataServiceProvider {

	/** A collection of method calls that represents a generic data service in Aleph2
	 *  Not all of the methods make sense for all data services - the optional is used to indicate whether a particular method can be invoked (for a particular bucket)
	 * @author Alex
	 */
	public static interface IGenericDataService {
		
		/** Special secondary buffer case, very useful for batch analytics
		 */
		public static final String SECONDARY_PING = "__ping";
		/** Special secondary buffer case, very useful for batch analytics
		 */
		public static final String SECONDARY_PONG = "__pong";
		
		/** Returns an IDataWriteService that can be written to. For more complex write operations (eg updates) on data services that support it, IDataWriteService.getCrudService can be used
		 * @param clazz - the class of the bean to use, or JsonNode.class for schema-less writes
		 * @param bucket - the bucket for which to write data (cannot be a multi bucket or alias)
		 * @param options - an arbitrary string whose function depends on the particular data service (eg "entity" or "association" to get a CRUD store from a graph DB)
		 * @param secondary_buffer - for "ping pong" type buckets, where the data is replaced atomically each run (or for modifying the contents of any bucket atomically) - use a secondary buffer (any string)
		 * @return optionally, a data write service that can be used to store and delete data
		 */
		<O> Optional<IDataWriteService<O>> getWritableDataService(final Class<O> clazz, final DataBucketBean bucket, final Optional<String> options, final Optional<String> secondary_buffer);
		
		/** Returns a full CRUD service (though may be functionally read-only, or maybe update-only) for the specified buckets 
		 * @param clazz - the class of the bean to use, or JsonNode.class for schema-less writes
		 * @param buckets - the buckets across which the CRUD operations will be applied
		 * @param options - an arbitrary string whose function depends on the particular data service (eg "entity" or "association" to get a CRUD store from a graph DB)
		 * @return optionally, a CRUD service pointing at the collection of buckets 
		 */
		<O> Optional<ICrudService<O>> getReadableCrudService(final Class<O> clazz, final Collection<DataBucketBean> buckets, final Optional<String> options);
		
		/** Returns the list of secondary buffers used with this bucket (so that they can be deleted)
		 *  By default does list the secdonary for transient analytic jobs, only that of the final output buffer (other jobs are locked together)  
		 * @param bucket
		 * @param intermediate_step - if specified then returns the secondary buffers of the specified analytic job (normally one with transient output), otherwise for the bucket (other jobs are locked). Does nothing if the service does not support intermediate jobs. 
		 * @return
		 */
		Set<String> getSecondaryBuffers(final DataBucketBean bucket, Optional<String> intermediate_step);
		
		/** Provides the primary name if it can be deduced, else Optional.empty (ie if symlinks aren't supported ... in this case you can infer what the primary buffer is
		 *  based on the fact it will be missing from the secondary buffer)
		 *  By default does list the primary for transient analytic jobs, only that of the final output buffer (other jobs are locked together)  
		 * @param bucket
		 * @param intermediate_step - if specified then returns the primary buffer of the specified analytic job (normally one with transient output), otherwise for the bucket (other jobs are locked). Does nothing if the service does not support intermediate jobs. 
		 * @return
		 */
		Optional<String> getPrimaryBufferName(final DataBucketBean bucket, Optional<String> intermediate_step);
		
		/** For "ping pong" buffers (or when atomically modifying the contents of any bucket), switches the "active" read buffer to the designated secondary buffer
		 *  The current primary bucket is returned to its previous name if possible, else uses the designated string, else deletes it 
		 *  By default does not switch transient buffers for transient analytic jobs, only the final output buffer		 *  
		 * @param bucket - the bucket to switch
		 * @param secondary_buffer - the name of the buffer (if not present then moves back to the default secondary)
		 * @param new_name_for_ex_primary - if empty then uses a technology specific term (can be referenced subsequently by using Optional.empty() in secondary_buffer) 
		 *        if "" then deletes the old index (this can be unsafe if a long running MR job is still running on the older collection), else renames to this
		 * @param intermediate_step - if specified then switches the primary buffer of the specified analytic job (normally one with transient output), otherwise for the bucket (other jobs are locked). Returns an error if the service does not support intermediate jobs. 
		 * @return a future containing the success/failure of the operation and associated status
		 */
		CompletableFuture<BasicMessageBean> switchCrudServiceToPrimaryBuffer(final DataBucketBean bucket, Optional<String> secondary_buffer, final Optional<String> new_name_for_ex_primary, Optional<String> intermediate_step);
				
		/** Indicates that this service should examine the data in this bucket and delete any old data
		 * @param bucket - the bucket to be checked
		 * @return a future containing the success/failure of the operation and associated status (if success==true, create a detail called "loggable" in order to log the result)
		 */
		CompletableFuture<BasicMessageBean> handleAgeOutRequest(final DataBucketBean bucket);
		
		/** Deletes or purges the bucket
		 * @param bucket - the bucket to be deleted/purged
		 * @param secondary_buffer - if this is specified then only the designated secondary buffer for the bucket is deleted (if bucket getting deleted then they all do)
		 * @param bucket_or_buffer_getting_deleted - whether this operation is part of a full deletion of the bucket (if it's a secondary buffer then refers just to the buffer)
		 * @return a future containing the success/failure of the operation and associated status
		 */
		CompletableFuture<BasicMessageBean> handleBucketDeletionRequest(final DataBucketBean bucket, final Optional<String> secondary_buffer, final boolean bucket_or_buffer_getting_deleted);
	};
	
	/** If this service instance has a data service associated with it, then return an interface that enables its use 
	 * @return the generic data service interface
	 */
	default Optional<IGenericDataService> getDataService() {
		return Optional.empty();
	}
	
	/** Callback to all registered data services whenever a bucket is modified
	 * @param bucket - the bucket in question
	 * @param old_bucket - if the bucket has been updated, then this is the old version
	 * @param suspended - if the bucket is currently suspended
	 * @param data_services -  the set of data services the implementing class instance handles (eg ES can handle data_warehouse, search_index_service, etc)
	 * @param previous_data_services - the set of data services the previous bucket's implementing class instance handles (eg ES can handle data_warehouse, search_index_service, etc)
	 * @return a future containing an optional return message
	 */
	default CompletableFuture<Collection<BasicMessageBean>> onPublishOrUpdate(final DataBucketBean bucket, final Optional<DataBucketBean> old_bucket, final boolean suspended, final Set<String> data_services, final Set<String> previous_data_services) {
		return CompletableFuture.completedFuture(Collections.emptyList());
	}
	
	/** Returns a secured version of the data provider service. 
	 * @param service_context - the system service context
	 * @param auth_bean - the authorization context of the calling user
	 * @return the secured version of the CRUD service
	 */
	default IDataServiceProvider secured(IServiceContext service_context, AuthorizationBean auth_bean) {		
		return service_context.getSecurityService().secured(this, auth_bean);
	}	
}
