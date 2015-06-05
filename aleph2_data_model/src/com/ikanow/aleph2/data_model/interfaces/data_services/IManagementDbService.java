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

import java.util.Optional;


import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.ProjectBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;

/** The interface to the management database
 * @author acp
 */
public interface IManagementDbService {

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
	 * @param sub_collection - arbitrary string, enables the user to split the per library state into multiple independent collections
	 * @return the CRUD service for the per library generic object store
	 */
	<T> ICrudService<T> getPerLibraryState(final Class<T> clazz, final SharedLibraryBean library, final Optional<String> sub_collection);
	
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
	
	/** Gets or (lazily) creates a repository accessible from processing that occurs in the context of the specified bucket
	 * @param clazz The class of the bean or object type desired (needed so the repo can reason about the type when deciding on optimizations etc)
	 * @param bucket a partial bean with the name or _id set
	 * @param sub_collection - arbitrary string, enables the user to split the per library state into multiple independent collections
	 * @return the CRUD service for the per bucket generic object store
	 */
	<T> ICrudService<T> getPerBucketState(final Class<T> clazz, final DataBucketBean bucket, final Optional<String> sub_collection);
	
	////////////////////////////////////
	
	// 3] Analytics

	/** Gets the store of analytic threads
	 * @return the CRUD service for the analytic thread store
	 */
	IManagementCrudService<AnalyticThreadBean> getAnalyticThreadStore();
	
	/** Gets or (lazily) creates a repository accessible from processing that occurs in the context of the specified analytic thread
	 * @param clazz The class of the bean or object type desired (needed so the repo can reason about the type when deciding on optimizations etc)
	 * @param analytic_thread a partial bean with the name or _id set
	 * @param sub_collection - arbitrary string, enables the user to split the per library state into multiple independent collections
	 * @return the CRUD service for the per analytic thread generic object store
	 */
	<T> ICrudService<T> getPerAnalyticThreadState(final Class<T> clazz, final AnalyticThreadBean analytic_thread, final Optional<String> sub_collection);	
	
	////////////////////////////////////

	// 3] Security
	
	/** Returns a copy of the management DB that is filtered based on the client (user) and project rights
	 * @param authorization_fieldname the fieldname in the bean that determines where the per-bean authorization is held
	 * @param client_auth Optional specification of the user's access rights
	 * @param project_auth Optional specification of the projects's access rights
	 * @return The filtered CRUD repo
	 */
	IManagementDbService getFilteredDb(final Optional<AuthorizationBean> client_auth, final Optional<ProjectBean> project_auth);
		
	////////////////////////////////////

	// X] Misc

	/** When an internal message gets lost, it will usually end up in the retry store - regular re-attempts will then be made
	 * @param retry_message_clazz - the class of the (internal) bean containing the lost message and some metadata
	 * @return a CRUD service intended to support
	 */
	<T> ICrudService<T> getRetryStore(final Class<T> retry_message_clazz);
	
	/** USE WITH CARE: this returns the driver to the underlying technology
	 *  shouldn't be used unless absolutely necessary!
	 * @param driver_class the class of the driver
	 * @param a string containing options in some technology-specific format
	 * @return a driver to the underlying technology. Will exception if you pick the wrong one!
	 */
	<T> Optional<T> getUnderlyingPlatformDriver(final Class<T> driver_class, final Optional<String> driver_options);
}
