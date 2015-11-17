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
	
	@SuppressWarnings("unchecked")
	@Override
	public <I extends IUnderlyingService> Optional<I> getService(Class<I> serviceClazz,
			Optional<String> serviceName) {
		return (Optional<I>) Optional.ofNullable(Optional.ofNullable(_mocks.get(serviceClazz.getName()))
								.orElse(new HashMap<String, IUnderlyingService>())
								.get(serviceName.orElse("")));
	}

	@Override
	public Optional<IColumnarService> getColumnarService() {
		return getService(IColumnarService.class, Optional.empty());
	}

	@Override
	public Optional<IDocumentService> getDocumentService() {
		return getService(IDocumentService.class, Optional.empty());
	}

	@Override
	public Optional<IGeospatialService> getGeospatialService() {
		return getService(IGeospatialService.class, Optional.empty());
	}

	@Override
	public Optional<IGraphService> getGraphService() {
		return getService(IGraphService.class, Optional.empty());
	}

	@Override
	public IManagementDbService getCoreManagementDbService() {
		return getService(IManagementDbService.class, IManagementDbService.CORE_MANAGEMENT_DB).get();
	}

	@Override
	public Optional<ISearchIndexService> getSearchIndexService() {
		return getService(ISearchIndexService.class, Optional.empty());
	}

	@Override
	public IStorageService getStorageService() {
		return getService(IStorageService.class, Optional.empty()).get();
	}

	@Override
	public Optional<ITemporalService> getTemporalService() {
		return getService(ITemporalService.class, Optional.empty());
	}

	@Override
	public ISecurityService getSecurityService() {
		return getService(ISecurityService.class, Optional.empty()).get();
	}

	@Override
	public GlobalPropertiesBean getGlobalProperties() {
		return _globals;
	}

}
