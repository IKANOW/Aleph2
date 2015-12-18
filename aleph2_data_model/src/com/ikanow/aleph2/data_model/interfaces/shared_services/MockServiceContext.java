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

import java.util.HashMap;
import java.util.Optional;

import com.google.inject.Provider;
import com.ikanow.aleph2.data_model.interfaces.data_services.IColumnarService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IDocumentService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IGeospatialService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IGraphService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.data_services.ITemporalService;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;

/** A useful mock object for unit testing, enables devs to create their own IServiceContexts without using guice
 * @author acp
 *
 */
public class MockServiceContext implements IServiceContext {

	protected final HashMap<String, HashMap<String, IUnderlyingService>> _mocks;
	protected GlobalPropertiesBean _globals = null;
	
	/** Provider implementation
	 * @author Alex
	 *
	 * @param <I>
	 */
	public static class MockProvider<I> implements Provider<I> {

		/** User c'tor
		 * @param i
		 */
		public MockProvider(final I i) {
			_i = i;
		}
		
		protected final I _i;
		
		/* (non-Javadoc)
		 * @see com.google.inject.Provider#get()
		 */
		@Override
		public I get() {
			return _i;
		}
		
	}
	
	/** User constructor 
	 */
	public MockServiceContext() {
		_mocks = new HashMap<String, HashMap<String, IUnderlyingService>>();
	}
	
	/** Enables users to insert their own services into the service context
	 * @param clazz - the class of the insertion type
	 * @param service_name - optional service name
	 * @param instance - the instance to insert
	 */
	public <I extends IUnderlyingService> void addService(Class<I> clazz, Optional<String> service_name, I instance) {
		HashMap<String, IUnderlyingService> submock = _mocks.get(clazz.getName());
		if (null == submock) {
			submock = new HashMap<String, IUnderlyingService>();
			_mocks.put(clazz.getName(), submock);
		}
		submock.put(service_name.orElse(""), instance);
	}
	public void addGlobals(final GlobalPropertiesBean globals) {
		_globals = globals;
	}
	
	@Override
	public <I extends IUnderlyingService> Optional<I> getService(Class<I> serviceClazz,
			Optional<String> serviceName) {
		return getServiceProvider(serviceClazz, serviceName).map(s -> s.get());
	}

	@SuppressWarnings("unchecked")
	@Override
	public <I extends IUnderlyingService> Optional<Provider<I>> getServiceProvider(Class<I> serviceClazz,
			Optional<String> serviceName) {
		return (Optional<Provider<I>>) Optional.ofNullable(Optional.ofNullable(_mocks.get(serviceClazz.getName()))
								.orElse(new HashMap<String, IUnderlyingService>())
								.get(serviceName.orElse("")))
								.map(i -> (Provider<I>)new MockProvider<I>((I)i))
								;
	}
	
	
	@Override
	public Optional<IColumnarService> getColumnarService() {
		return getColumnarServiceProvider().map(s -> s.get());
	}
	@Override
	public Optional<Provider<IColumnarService>> getColumnarServiceProvider() {
		return getServiceProvider(IColumnarService.class, Optional.empty());
	}


	@Override
	public Optional<IDocumentService> getDocumentService() {
		return getDocumentServiceProvider().map(s -> s.get());
	}
	@Override
	public Optional<Provider<IDocumentService>> getDocumentServiceProvider() {
		return getServiceProvider(IDocumentService.class, Optional.empty());
	}

	@Override
	public Optional<IGeospatialService> getGeospatialService() {
		return getGeospatialServiceProvider().map(s -> s.get());
	}
	@Override
	public Optional<Provider<IGeospatialService>> getGeospatialServiceProvider() {
		return getServiceProvider(IGeospatialService.class, Optional.empty());
	}


	@Override
	public Optional<IGraphService> getGraphService() {
		return getGraphServiceProvider().map(s -> s.get());
	}
	@Override
	public Optional<Provider<IGraphService>> getGraphServiceProvider() {
		return getServiceProvider(IGraphService.class, Optional.empty());
	}


	@Override
	public IManagementDbService getCoreManagementDbService() {
		return getCoreManagementDbServiceProvider().get();
	}
	@Override
	public Provider<IManagementDbService> getCoreManagementDbServiceProvider() {
		return getServiceProvider(IManagementDbService.class, IManagementDbService.CORE_MANAGEMENT_DB).get();
	}

	@Override
	public Optional<ISearchIndexService> getSearchIndexService() {
		return getSearchIndexServiceProvider().map(s -> s.get());
	}
	@Override
	public Optional<Provider<ISearchIndexService>> getSearchIndexServiceProvider() {
		return getServiceProvider(ISearchIndexService.class, Optional.empty());
	}

	@Override
	public IStorageService getStorageService() {
		return getStorageServiceProvider().get();
	}
	@Override
	public Provider<IStorageService> getStorageServiceProvider() {
		return getServiceProvider(IStorageService.class, Optional.empty()).get();
	}


	@Override
	public Optional<ITemporalService> getTemporalService() {
		return getTemporalServiceProvider().map(s -> s.get());
	}
	@Override
	public Optional<Provider<ITemporalService>> getTemporalServiceProvider() {
		return getServiceProvider(ITemporalService.class, Optional.empty());
	}

	@Override
	public ISecurityService getSecurityService() {
		return getSecurityServiceProvider().get();
	}
	@Override
	public Provider<ISecurityService> getSecurityServiceProvider() {
		return getServiceProvider(ISecurityService.class, Optional.empty()).get();
	}

	@Override
	public GlobalPropertiesBean getGlobalProperties() {
		return _globals;
	}



}
