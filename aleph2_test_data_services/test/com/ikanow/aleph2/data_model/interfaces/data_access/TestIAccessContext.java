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

import java.util.Optional;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ikanow.aleph2.access_manager.data_access.AccessContext;
import com.ikanow.aleph2.access_manager.data_access.AccessMananger;
import com.ikanow.aleph2.access_manager.data_access.sample_services.SampleCustomService;
import com.ikanow.aleph2.data_model.utils.ContextUtils;
import com.typesafe.config.ConfigFactory;


public class TestIAccessContext {

	@BeforeClass
	public static void setupBeforeClass() throws Exception {
		AccessMananger.initialize(ConfigFactory.load());	
	}
	
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Test
	public void testGetDefaultServices() {
		IAccessContext context = ContextUtils.getAccessContext();
		assertNotNull(context.getColumnarService());
		assertNotNull(context.getDocumentService());
		assertNotNull(context.getGeospatialService());
		assertNotNull(context.getGraphService());
		assertNotNull(context.getManagementDbService());
		assertNotNull(context.getSearchIndexService());
		assertNotNull(context.getSecurityService());
		assertNotNull(context.getStorageIndexService());
		assertNotNull(context.getTemporalService());
	}
	
	@Test
	public void testGetCustomServices() {
		IAccessContext context = ContextUtils.getAccessContext();
		Optional<String> opt = Optional.empty();
		//TODO getDataService needs a null for custom services
		assertNotNull(context.getDataService(SampleCustomService.class, opt));
	}
	
	@Test
	public void testServicesAreSingletons() {
		IAccessContext context = ContextUtils.getAccessContext();
		assertEquals(context.getSecurityService().hashCode(), context.getSecurityService().hashCode());
	}

}
