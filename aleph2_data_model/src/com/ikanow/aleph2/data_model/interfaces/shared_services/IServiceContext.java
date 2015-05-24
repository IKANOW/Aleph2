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
package com.ikanow.aleph2.data_model.interfaces.shared_services;

import java.util.Optional;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.ikanow.aleph2.data_model.interfaces.data_services.IColumnarService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IDocumentService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IGeospatialService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IGraphService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.data_services.ITemporalService;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;

/**
 * Helper class to give access to all the configured services available
 * in the application.  This should be used as a passthrough to
 * {@link com.ikanow.aleph2.data_model.utils.ModuleUtils#getService(Class, Optional)}
 * 
 * @author Burch
 *
 */
public interface IServiceContext {

	/////////////////////////////////////////////////////////////////////
		
	//generic get service interface
	/**
	* Enables access to the data services available in the system.  The currently
	* configured instance of the class passed in will be returned if it exists, null otherwise.
	* 
	* @param serviceClazz The class of the resource you want to access e.g. ISecurityService.class
	* @return the data service requested or null if it does not exist
	*/
	public <I> I getService(@NonNull Class<I> serviceClazz, @NonNull Optional<String> serviceName);
	
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
	* Returns an instance of the currently configured _core_ mangement db service (which sits above the actual technology configured for the management db service via service.ManagementDbService.*).
	* 
	* This is a helper function that just calls {@link getDataService(Class<I>)}
	* 
	* @return
	*/
	public IManagementDbService getCoreManagementDbService();
	
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
	public IStorageService getStorageService();
	
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
	
	//security service is related to data services
	/**
	* Returns an instance of the global configuration parameters
	* 
	* This is a helper function that just calls {@link getDataService(Class<I>)}
	* 
	* @return
	*/
	public GlobalPropertiesBean getGlobalProperties();
	

}
