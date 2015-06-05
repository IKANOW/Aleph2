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

import static org.junit.Assert.*;

import java.util.Optional;

import org.junit.Test;

import com.ikanow.aleph2.data_model.interfaces.data_services.IColumnarService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IDataWarehouseService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IDocumentService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IGeospatialService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IGraphService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.data_services.ITemporalService;
import com.ikanow.aleph2.data_model.interfaces.data_services.samples.SampleColumnarService;
import com.ikanow.aleph2.data_model.interfaces.data_services.samples.SampleDataWarehouseService;
import com.ikanow.aleph2.data_model.interfaces.data_services.samples.SampleDocumentService;
import com.ikanow.aleph2.data_model.interfaces.data_services.samples.SampleGeospatialService;
import com.ikanow.aleph2.data_model.interfaces.data_services.samples.SampleGraphService;
import com.ikanow.aleph2.data_model.interfaces.data_services.samples.SampleManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.data_services.samples.SampleSearchIndexService;
import com.ikanow.aleph2.data_model.interfaces.data_services.samples.SampleSecurityService;
import com.ikanow.aleph2.data_model.interfaces.data_services.samples.SampleStorageService;
import com.ikanow.aleph2.data_model.interfaces.data_services.samples.SampleTemporalService;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;

public class TestMockServiceContext {

	@Test
	public void testAll() {
		MockServiceContext context = new MockServiceContext();
		
//		configMap.put("service.TemporalService.interface", ITemporalService.class.getCanonicalName());
//		configMap.put("service.TemporalService.service", SampleTemporalService.class.getCanonicalName());
	
		context.addService(ISecurityService.class, Optional.empty(), new SampleSecurityService());
		context.addService(IColumnarService.class, Optional.empty(), new SampleColumnarService());
		context.addService(IDataWarehouseService.class, Optional.empty(), new SampleDataWarehouseService());
		context.addService(IDocumentService.class, Optional.empty(), new SampleDocumentService());
		context.addService(IGeospatialService.class, Optional.empty(), new SampleGeospatialService());
		context.addService(IGraphService.class, Optional.empty(), new SampleGraphService());
		context.addService(IManagementDbService.class, Optional.of("CoreManagementDbService"), new SampleManagementDbService());
		context.addService(IManagementDbService.class, Optional.empty(), new SampleManagementDbService());
		context.addService(ISearchIndexService.class, Optional.empty(), new SampleSearchIndexService());
		context.addService(IStorageService.class, Optional.empty(), new SampleStorageService());
		context.addService(ITemporalService.class, Optional.empty(), new SampleTemporalService());
		context.addService(GlobalPropertiesBean.class, Optional.empty(), new GlobalPropertiesBean(null, null, null, null));
		
		assertNotNull(context.getColumnarService());
		assertNotNull(context.getDocumentService());
		assertNotNull(context.getGeospatialService());
		assertNotNull(context.getGraphService());
		assertNotNull(context.getCoreManagementDbService());
		assertNotNull(context.getSearchIndexService());
		assertNotNull(context.getStorageService());
		assertNotNull(context.getTemporalService());
		assertNotNull(context.getSecurityService());
		assertNotNull(context.getGlobalProperties());
		
		assertNull(context.getService(TestMockServiceContext.class, Optional.empty()));
		assertNull(context.getService(ISearchIndexService.class, Optional.of("fail")));
		
		
		assertFalse("Shouldn't be equal", context.getCoreManagementDbService() == context.getService(IManagementDbService.class, Optional.empty()).get());
	}
	
}
