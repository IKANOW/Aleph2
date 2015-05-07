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
package com.ikanow.aleph2.access_manager.data_access;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ikanow.aleph2.access_manager.data_access.AccessContext;
import com.ikanow.aleph2.access_manager.data_access.sample_services.SampleCustomService;
import com.ikanow.aleph2.access_manager.data_access.sample_services.SampleUnboundService;

public class TestAccessManager {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		//TODO load up what accessManager to test somehow (e.g. via config)
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
	
	//TODO I should probably be loading up a different config just for tests
	
	@Test
	public void testGetDefaultServices() {
		AccessContext context = new AccessContext(); //todo don't hardcode this access context
		assertNotNull(context.getSecurityService());
		
		assertNotNull(context.getColumnarService());
		assertNotNull(context.getDocumentService());
		assertNotNull(context.getGeospatialService());
		assertNotNull(context.getGraphService());
		assertNotNull(context.getManagementDbService());
		assertNotNull(context.getSearchIndexService());
		assertNotNull(context.getStorageIndexService());
		assertNotNull(context.getTemporalService());
	}
	
	@Test
	public void testGetCustomServices() {
		AccessContext context = new AccessContext();
		assertNotNull(context.getDataService(SampleCustomService.class));
	}
	
	@Test
	public void testGetCustomServiceDNE() {
		AccessContext context = new AccessContext();
		assertNull(context.getDataService(SampleUnboundService.class)); //this class should not have a binding
	}

}
