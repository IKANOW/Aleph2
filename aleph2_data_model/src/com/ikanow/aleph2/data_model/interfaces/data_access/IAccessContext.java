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
package com.ikanow.aleph2.data_model.interfaces.data_access;

import java.util.Optional;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;

import com.google.inject.Injector;
import com.ikanow.aleph2.data_model.interfaces.data_services.IColumnarService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IDocumentService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IGeospatialService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IGraphService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.data_services.ITemporalService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;

/**
 * Gives an app access to all the currently configured data services.
 * 
 * @author Burch
 *
 */
public interface IAccessContext {	
	/////////////////////////////////////////////////////////////////////
	
	//generic get service interface
	/**
	 * Enables access to the data services available in the system.  The currently
	 * configured instance of the class passed in will be returned if it exists, null otherwise.
	 * 
	 * @param service_clazz The class of the resource you want to access e.g. ISecurityService.class
	 * @return the data service requested or null if it does not exist
	 */
	public <I> I getDataService(@NonNull Class<I> service_clazz);

	/////////////////////////////////////////////////////////////////////
	
	//utility getters for common services
	/**
	 * Returns an instance of the currently configured columnar service.
	 * 
	 * This is a helper function that just calls {@link getDataService(Class<I>)}
	 * 
	 * @return
	 */
	public IColumnarService getColumnarService();
	
	/**
	 * Returns an instance of the currently configured document service.
	 * 
	 * This is a helper function that just calls {@link getDataService(Class<I>)}
	 * 
	 * @return
	 */
	public IDocumentService getDocumentService();
	
	/**
	 * Returns an instance of the currently configured geospatial service.
	 * 
	 * This is a helper function that just calls {@link getDataService(Class<I>)}
	 * 
	 * @return
	 */
	public IGeospatialService getGeospatialService();
	
	/**
	 * Returns an instance of the currently configured graph service.
	 * 
	 * This is a helper function that just calls {@link getDataService(Class<I>)}
	 * 
	 * @return
	 */
	public IGraphService getGraphService();
	
	/**
	 * Returns an instance of the currently configured mangement db service.
	 * 
	 * This is a helper function that just calls {@link getDataService(Class<I>)}
	 * 
	 * @return
	 */
	public IManagementDbService getManagementDbService();
	
	/**
	 * Returns an instance of the currently configured search index service.
	 * 
	 * This is a helper function that just calls {@link getDataService(Class<I>)}
	 * 
	 * @return
	 */
	public ISearchIndexService getSearchIndexService();
	
	/**
	 * Returns an instance of the currently configured storage index service.
	 * 
	 * This is a helper function that just calls {@link getDataService(Class<I>)}
	 * 
	 * @return
	 */
	public IStorageService getStorageIndexService();
	
	/**
	 * Returns an instance of the currently configured temporal service.
	 * 
	 * This is a helper function that just calls {@link getDataService(Class<I>)}
	 * 
	 * @return
	 */
	public ITemporalService getTemporalService();
	
	/////////////////////////////////////////////////////////////////////
	
	//security service is related to data services
	/**
	 * Returns an instance of the currently configured security service.
	 * 
	 * This is a helper function that just calls {@link getDataService(Class<I>)}
	 * 
	 * @return
	 */
	public ISecurityService getSecurityService();
	
	/////////////////////////////////////////////////////////////////////
	
	// Real-time access to system components
	
	/** Enables access modules to subscribe to objects being generated by other components in the system
	 * @param bucket - the bucket to monitor
	 * @param stage - if set to Optionals.empty() then occurs post enrichment. If set to "" then occurs pre-enrichment. Otherwise should be the name of a module - will listen immediately after that. 
	 * @param on_new_object_callback - a void function taking a JsonNode (the object) 
	 * @return a future that completes when the subscription has occurred, describing its success or failure
	 */
	Future<BasicMessageBean> subscribeToBucket(@NonNull DataBucketBean bucket, @NonNull Optional<String> stage, Consumer<JsonNode> on_new_object_callback);
	
	/** Enables access modules to subscribe to objects being generated by other components in the system
	 * @param analytic_thread - the thread to monitor
	 * @param stage - if set to Optionals.empty() then occurs post processing. If set to "" then occurs pre-processing. Otherwise should be the name of a module - will listen immediately after that. 
	 * @param on_new_object_callback - a void function taking a JsonNode (the object) 
	 * @return a future that completes when the subscription has occurred, describing its success or failure
	 */
	Future<BasicMessageBean> subscribeToAnalyticThread(@NonNull AnalyticThreadBean analytic_thread, @NonNull Optional<String> stage, Consumer<JsonNode> on_new_object_callback);

	/** Enables access modules to subscribe to objects being generated by other components in the system
	 * @param bucket - the bucket to monitor
	 * @param stage - if set to Optionals.empty() then occurs post enrichment. If set to "" then occurs pre-enrichment. Otherwise should be the name of a module - will listen immediately after that. 
	 * @return a future returning stream of Json nodes as soon as the connection is established
	 */
	Future<Stream<JsonNode>> getObjectStreamFromBucket(@NonNull DataBucketBean bucket, @NonNull Optional<String> stage);
	
	/** Enables access modules to subscribe to objects being generated by other components in the system
	 * @param analytic_thread - the analytic thread to monitor
	 * @param stage - if set to Optionals.empty() then occurs post processing. If set to "" then occurs pre-processing. Otherwise should be the name of a module - will listen immediately after that. 
	 * @return a future returning stream of Json nodes as soon as the connection is established
	 */
	Stream<JsonNode> getObjectStreamFromAnalyticThread(@NonNull AnalyticThreadBean analytic_thread, @NonNull Optional<String> stage);
	
}
