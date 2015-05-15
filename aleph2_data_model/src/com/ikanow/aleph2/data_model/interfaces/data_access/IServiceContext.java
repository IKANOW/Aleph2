package com.ikanow.aleph2.data_model.interfaces.data_access;

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
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;

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

}
