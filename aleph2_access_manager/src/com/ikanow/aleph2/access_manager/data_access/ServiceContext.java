package com.ikanow.aleph2.access_manager.data_access;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import com.ikanow.aleph2.data_model.interfaces.data_access.IServiceContext;
import com.ikanow.aleph2.data_model.interfaces.data_services.IColumnarService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IDocumentService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IGeospatialService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IGraphService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.data_services.ITemporalService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;

public class ServiceContext implements IServiceContext {

	public Map<Key, Injector> serviceInjectors = new HashMap<Key, Injector>();
	
	public void addDataService(@NonNull Class serviceClazz, @NonNull Optional<String> serviceName, @NonNull Injector injector) {
		serviceInjectors.put(getKey(serviceClazz, serviceName), injector);
	}
	
	private Key getKey(@NonNull Class serviceClazz, @NonNull Optional<String> serviceName) {		
		if ( serviceName.isPresent() )
			return Key.get(serviceClazz, Names.named(serviceName.get()));
		else
			return Key.get(serviceClazz);
	}
	
	@Override
	public <I> I getService(@NonNull Class<I> serviceClazz, @NonNull Optional<String> serviceName) {
		Key key;
		if ( serviceName.isPresent() )
			key = Key.get(serviceClazz, Names.named(serviceName.get()));
		else
			key = Key.get(serviceClazz);
		
		return (I) serviceInjectors.get(key).getInstance(key);
	}

	@Override
	public IColumnarService getColumnarService() {
		return getService(IColumnarService.class, Optional.empty());
	}

	@Override
	public IDocumentService getDocumentService() {
		return getService(IDocumentService.class, Optional.empty());
	}

	@Override
	public IGeospatialService getGeospatialService() {
		return getService(IGeospatialService.class, Optional.empty());
	}

	@Override
	public IGraphService getGraphService() {
		return getService(IGraphService.class, Optional.empty());
	}

	@Override
	public IManagementDbService getManagementDbService() {
		return getService(IManagementDbService.class, Optional.empty());
	}

	@Override
	public ISearchIndexService getSearchIndexService() {
		return getService(ISearchIndexService.class, Optional.empty());
	}

	@Override
	public IStorageService getStorageIndexService() {
		return getService(IStorageService.class, Optional.empty());
	}

	@Override
	public ITemporalService getTemporalService() {
		return getService(ITemporalService.class, Optional.empty());
	}

	@Override
	public ISecurityService getSecurityService() {
		return getService(ISecurityService.class, Optional.empty());
	}
}
