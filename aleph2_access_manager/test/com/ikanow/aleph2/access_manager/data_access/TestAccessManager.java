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
