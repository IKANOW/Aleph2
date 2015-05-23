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

import java.util.HashMap;
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

/** A useful mock object for unit testing, enables devs to create their own IServiceContexts without using guice
 * @author acp
 *
 */
public class MockServiceContext implements IServiceContext {

	protected final HashMap<String, HashMap<String, Object>> _mocks;
	
	/** User constructor 
	 */
	public MockServiceContext() {
		_mocks = new HashMap<String, HashMap<String, Object>>();
	}
	
	/** Enables users to insert their own services into the service context
	 * @param clazz - the class of the insertion type
	 * @param service_name - optional service name
	 * @param instance - the instance to insert
	 */
	public <I> void addService(Class<I> clazz, Optional<String> service_name, I instance) {
		HashMap<String, Object> submock = _mocks.get(clazz.getName());
		if (null == submock) {
			submock = new HashMap<String, Object>();
			_mocks.put(clazz.getName(), submock);
		}
		submock.put(service_name.orElse(""), instance);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public <I> I getService(@NonNull Class<I> serviceClazz,
			@NonNull Optional<String> serviceName) {
		return (I)Optional.ofNullable(_mocks.get(serviceClazz.getName()))
						.orElse(new HashMap<String, Object>())
						.get(serviceName.orElse(""));
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
	public IManagementDbService getCoreManagementDbService() {
		return getService(IManagementDbService.class, Optional.of("CoreManagementDbService"));
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

	@Override
	public GlobalPropertiesBean getGlobalProperties() {
		return getService(GlobalPropertiesBean.class, Optional.empty());
	}

}
