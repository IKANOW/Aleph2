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
package com.ikanow.aleph2.data_model.interfaces.data_services;

import java.time.Duration;
import java.util.Optional;

import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.shared.AssetStateDirectoryBean;
import com.ikanow.aleph2.data_model.objects.shared.AssetStateDirectoryBean.StateDirectoryType;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.ProcessingTestSpecBean;
import com.ikanow.aleph2.data_model.objects.shared.ProjectBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.FutureUtils.ManagementFuture;

/** The interface to the management database
 * @author acp
 */
public interface IManagementDbService extends IUnderlyingService {
	public static Optional<String> CORE_MANAGEMENT_DB = Optional.of("CoreManagementDbService");
	
	/** Returns a version of the DB that (if supported) cannot be written to. This enables the underlying infrastructure
	 *  to be more lightweight in terms of the infrastructure set up, so is desirable to use whenever possible
	 * @return A possibly read only version of the DB (guaranteed read only for the core management service)
	 */
	IManagementDbService readOnlyVersion();
	
	////////////////////////////////////
	
	// 1] Access
	
	// 1.1] JAR Library, includes:
	//      - Harvest Technologies
	//		- Harvest modules (no interface)
	//		- Enrichment modules
	//		- Analytics technologies
	//		- Analytics modules
	//		- Analytics utilities (no interface)
	//		- Access modules
	//		- Acccess utilities (no interface)
	
	/** Gets the store of shared JVM JAR libraries
	 * @return the CRUD service for the shared libraries store
	 */
	IManagementCrudService<SharedLibraryBean> getSharedLibraryStore();
	
	//TODO (ALEPH-19): shared schemas
	
	//TODO (ALEPH-19): other "shares" - including "bucket artefacts" like "documents", "spreadsheets", "knowledge graphs" 
	
	//TODO (ALEPH-19, ALEPH-2): security and other things that we'll initially handle from IKANOW v1
	
	/** Gets or (lazily) creates a repository shared by all users of the specified library (eg Harvest Technology)
	 * @param clazz The class of the bean or object type desired (needed so the repo can reason about the type when deciding on optimizations etc)
	 * @param library a partial bean with the name or _id set
	 * @param collection - arbitrary string, enables the user to split the per library state into multiple independent collections. If empty returns a read-only CRUD service of type AssetStateDirectoryBean.
	 * @return the CRUD service for the per library generic object store
	 */
	<T> ICrudService<T> getPerLibraryState(final Class<T> clazz, final SharedLibraryBean library, final Optional<String> collection);
	
	////////////////////////////////////
	
	// 2] Importing
	
	// 2.1] Buckets	
	
	/** Gets the store of data buckets
	 * @return  the CRUD service for the bucket store
	 */
	IManagementCrudService<DataBucketBean> getDataBucketStore();
	
	/** Gets the store of data bucket statuses
	 * @return  the CRUD service for the bucket status store
	 */
	IManagementCrudService<DataBucketStatusBean> getDataBucketStatusStore();
	
	/** Gets or (lazily) creates a repository accessible from processing that occurs in the context of the specified bucket (harvest stage)
	 * @param clazz The class of the bean or object type desired (needed so the repo can reason about the type when deciding on optimizations etc)
	 * @param bucket a partial bean with the name or _id set
	 * @param collection - arbitrary string, enables the user to split the per library state into multiple independent collections. If empty returns a read-only CRUD service of type AssetStateDirectoryBean.
	 * @return the CRUD service for the per bucket generic object store
	 */
	<T> ICrudService<T> getBucketHarvestState(final Class<T> clazz, final DataBucketBean bucket, final Optional<String> collection);
	
	// 2.2] Enrichment
	
	/** Gets or (lazily) creates a repository accessible from processing that occurs in the context of the specified bucket (enrichment stage)
	 * @param clazz The class of the bean or object type desired (needed so the repo can reason about the type when deciding on optimizations etc)
	 * @param bucket a partial bean with the name or _id set
	 * @param sub_collection - arbitrary string, enables the user to split the per library state into multiple independent collections. If empty returns a read-only CRUD service of type AssetStateDirectoryBean.
	 * @return the CRUD service for the per bucket generic object store
	 */
	<T> ICrudService<T> getBucketEnrichmentState(final Class<T> clazz, final DataBucketBean bucket, final Optional<String> sub_collection);
	
	// 2.3] Misc bucket operations
	
	/** Purges the specified bucket of all of its data objects (and ditto for all of its sub-buckets)
	 * @param to_purge - the bucket whose data objects should be deleted
	 * @param in - an optional duration to pause before purging the bucket
	 * @return - whether the purge was scheduled/successful, with a management side channel
	 */
	ManagementFuture<Boolean> purgeBucket(final DataBucketBean to_purge, final Optional<Duration> in);
	
	/** Launches a test of the specified bucket using the provided test spec
	 * @param to_test - the bucket to test
	 * @param test_spec - the details of the test
	 * @return whether the test was started successfully, with a management side channel
	 */
	ManagementFuture<Boolean> testBucket(final DataBucketBean to_test, final ProcessingTestSpecBean test_spec);
	
	////////////////////////////////////
	
	// 3] Analytics

	/** Gets or (lazily) creates a repository accessible from processing that occurs in the context of the specified analytic thread (that is part of a bucket)
	 * @param clazz The class of the bean or object type desired (needed so the repo can reason about the type when deciding on optimizations etc)
	 * @param bucket a partial bean with the name or _id set
	 * @param collection - arbitrary string, enables the user to split the per library state into multiple independent collections. If empty returns a read-only CRUD service of type AssetStateDirectoryBean.
	 * @return the CRUD service for the per analytic thread generic object store
	 */
	<T> ICrudService<T> getBucketAnalyticThreadState(final Class<T> clazz, final DataBucketBean bucket, final Optional<String> collection);	
	
	////////////////////////////////////

	// 3] Security
	
	/** Returns a copy of the management DB that is filtered based on the client (user) and project rights
	 * @param authorization_fieldname the fieldname in the bean that determines where the per-bean authorization is held
	 * @param client_auth Optional specification of the user's access rights
	 * @param project_auth Optional specification of the projects's access rights
	 * @return The filtered CRUD repo
	 */
	IManagementDbService getFilteredDb(final Optional<AuthorizationBean> client_auth, final Optional<ProjectBean> project_auth);
		
	IManagementDbService getSecureddDb(final AuthorizationBean client_auth, final Optional<ProjectBean> project_auth);

	////////////////////////////////////

	// X] Misc/Internals

	/** When an internal message gets lost, it will usually end up in the retry store - regular re-attempts will then be made
	 * @param retry_message_clazz - the class of the (internal) bean containing the lost message and some metadata
	 * @return a CRUD service from the underlying technology
	 */
	<T> ICrudService<T> getRetryStore(final Class<T> retry_message_clazz);
	
	/** When a bucket is deleted (or purged), it is added to this queue, which is responsible for actual deletion of data 
	 * @param deletion_queue_clazz - the class of the (internal) bean containing the bucket to be deleted and some metadata
	 * @return a CRUD service from the underlying technology
	 */
	<T> ICrudService<T> getBucketDeletionQueue(final Class<T> deletion_queue_clazz);
	
	/** This queue contains all buckets currently being tested 
	 * @param test_queue_clazz - the class of the (internal) bean containing the bucket to be tested and some metadata
	 * @return a CRUD service from the underlying technology
	 */
	<T> ICrudService<T> getBucketTestQueue(final Class<T> test_queue_clazz);
	
	/** Returns a list of all the "user" state objects
	 * @param bucket_filter - if enabled then only returns CRUD datastores for the designated buckets
	 * @param type_filter - if enabled then only returns CRUD datastores for the designated type (harvest/analytic_thread/enrichment/library)
	 * @return the implementation of the CRUD service 
	 */
	ICrudService<AssetStateDirectoryBean> getStateDirectory(final Optional<DataBucketBean> bucket_filter, Optional<StateDirectoryType> type_filter);
	
	

}
