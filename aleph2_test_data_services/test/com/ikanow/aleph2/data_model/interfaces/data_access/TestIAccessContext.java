package com.ikanow.aleph2.data_model.interfaces.data_access;

import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.Test;

import com.ikanow.aleph2.access_manager.data_access.sample_services.SampleCustomService;
import com.ikanow.aleph2.data_model.utils.ContextUtils;


public class TestIAccessContext {

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
		assertNotNull(context.getDataService(SampleCustomService.class));
	}
	
	@Test
	public void testServicesAreSingletons() {
		IAccessContext context = ContextUtils.getAccessContext();
		assertEquals(context.getSecurityService().hashCode(), context.getSecurityService().hashCode());
	}

}
