package com.ikanow.aleph2.data_model.interfaces.data_access;

import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.Test;

import com.ikanow.aleph2.access_manager.data_access.AccessDriver;

public class TestIAccessContext {

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Test
	public void testGetDefaultServices() {
		IAccessContext context = AccessDriver.getAccessContext();
		assertNotNull(context.getColumnarDbService());
		assertNotNull(context.getDocumentDbService());
		assertNotNull(context.getGeospatialService());
		assertNotNull(context.getGraphDbService());
		assertNotNull(context.getManagementDbService());
		assertNotNull(context.getSearchIndexService());
		assertNotNull(context.getSecurityService());
		assertNotNull(context.getStorageIndexService());
		assertNotNull(context.getTemporalService());
	}
	
	@Test
	public void testGetCustomServices() {
		IAccessContext context = AccessDriver.getAccessContext();
		assertNotNull(context.getDataService("SampleCustomService"));
	}
	
	@Test
	public void test() {
		IAccessContext context = com.ikanow.aleph2.data_model.interfaces.data_access.AccessDriver.getAccessContext();
		System.out.println(context.getClass().getName()); //this should be our AccessContext
		context.getSecurityService();
		
	}

}
