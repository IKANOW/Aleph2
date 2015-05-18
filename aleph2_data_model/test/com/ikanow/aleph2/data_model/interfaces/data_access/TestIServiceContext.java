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

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ikanow.aleph2.data_model.interfaces.data_access.samples.ICustomService;
import com.ikanow.aleph2.data_model.interfaces.data_access.samples.SampleCustomServiceOne;
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
import com.ikanow.aleph2.data_model.interfaces.data_services.samples.SampleCoreDistributedServices;
import com.ikanow.aleph2.data_model.interfaces.data_services.samples.SampleDataWarehouseService;
import com.ikanow.aleph2.data_model.interfaces.data_services.samples.SampleDocumentService;
import com.ikanow.aleph2.data_model.interfaces.data_services.samples.SampleGeospatialService;
import com.ikanow.aleph2.data_model.interfaces.data_services.samples.SampleGraphService;
import com.ikanow.aleph2.data_model.interfaces.data_services.samples.SampleManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.data_services.samples.SampleSearchIndexService;
import com.ikanow.aleph2.data_model.interfaces.data_services.samples.SampleSecurityService;
import com.ikanow.aleph2.data_model.interfaces.data_services.samples.SampleStorageService;
import com.ikanow.aleph2.data_model.interfaces.data_services.samples.SampleTemporalService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICoreDistributedServices;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.data_model.utils.ModuleUtils.ServiceContext;
import com.typesafe.config.ConfigFactory;

public class TestIServiceContext {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}


	@Test
	public void testGetDefaultServices() throws Exception {
		Map<String, Object> configMap = new HashMap<String, Object>();
		configMap.put("service.SecurityService.interface", ISecurityService.class.getCanonicalName());
		configMap.put("service.SecurityService.service", SampleSecurityService.class.getCanonicalName());	
		configMap.put("service.ColumnarService.interface", IColumnarService.class.getCanonicalName());
		configMap.put("service.ColumnarService.service", SampleColumnarService.class.getCanonicalName());
		configMap.put("service.DataWarehouseService.interface", IDataWarehouseService.class.getCanonicalName());
		configMap.put("service.DataWarehouseService.service", SampleDataWarehouseService.class.getCanonicalName());
		configMap.put("service.DocumentService.interface", IDocumentService.class.getCanonicalName());
		configMap.put("service.DocumentService.service", SampleDocumentService.class.getCanonicalName());
		configMap.put("service.GeospatialService.interface", IGeospatialService.class.getCanonicalName());
		configMap.put("service.GeospatialService.service", SampleGeospatialService.class.getCanonicalName());
		configMap.put("service.GraphService.interface", IGraphService.class.getCanonicalName());
		configMap.put("service.GraphService.service", SampleGraphService.class.getCanonicalName());
		configMap.put("service.ManagementDbService.interface", IManagementDbService.class.getCanonicalName());
		configMap.put("service.ManagementDbService.service", SampleManagementDbService.class.getCanonicalName());
		configMap.put("service.SearchIndexService.interface", ISearchIndexService.class.getCanonicalName());
		configMap.put("service.SearchIndexService.service", SampleSearchIndexService.class.getCanonicalName());
		configMap.put("service.StorageService.interface", IStorageService.class.getCanonicalName());
		configMap.put("service.StorageService.service", SampleStorageService.class.getCanonicalName());
		configMap.put("service.TemporalService.interface", ITemporalService.class.getCanonicalName());
		configMap.put("service.TemporalService.service", SampleTemporalService.class.getCanonicalName());
		configMap.put("service.CoreDistributedServices.interface", ICoreDistributedServices.class.getCanonicalName());
		configMap.put("service.CoreDistributedServices.service", SampleCoreDistributedServices.class.getCanonicalName());
		ModuleUtils.loadModulesFromConfig(ConfigFactory.parseMap(configMap));		
		IServiceContext context = new ServiceContext();

		assertNotNull(context.getColumnarService());
		assertNotNull(context.getDocumentService());
		assertNotNull(context.getGeospatialService());
		assertNotNull(context.getGraphService());
		assertNotNull(context.getManagementDbService());
		assertNotNull(context.getSearchIndexService());
		assertNotNull(context.getStorageIndexService());
		assertNotNull(context.getTemporalService());
		assertNotNull(context.getSecurityService());
		assertNotNull(context.getCoreDistributedServices());
	}
	
	@Test
	public void testGetCustomServices() throws Exception {
		Map<String, Object> configMap = new HashMap<String, Object>();
		configMap.put("service.SampleCustomServiceOne.interface", ICustomService.class.getCanonicalName());
		configMap.put("service.SampleCustomServiceOne.service", SampleCustomServiceOne.class.getCanonicalName());
		ModuleUtils.loadModulesFromConfig(ConfigFactory.parseMap(configMap));		
		IServiceContext context = new ServiceContext();
		assertNotNull(context.getService(ICustomService.class, Optional.of(SampleCustomServiceOne.class.getSimpleName())));
	}
	
	@Test
	public void testGetCustomServiceDNE() {
		Map<String, Object> configMap = new HashMap<String, Object>();
		boolean threwError = false;
		try {
			ModuleUtils.loadModulesFromConfig(ConfigFactory.parseMap(configMap));
		} catch (Exception e) {
			threwError = true;
		}	
		assertTrue(threwError);		
	}

}
