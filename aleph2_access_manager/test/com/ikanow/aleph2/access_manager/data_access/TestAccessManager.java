package com.ikanow.aleph2.access_manager.data_access;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ikanow.aleph2.access_manager.data_access.AccessContext;
import com.ikanow.aleph2.data_model.interfaces.data_layers.IDataService;

public class TestAccessManager {

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
	public void test() {
		AccessContext context = new AccessContext();
		IDataService custom_service = context.getDataService("SampleCustomService");
		assertNotNull(custom_service);
	}
	
	@Test
	public void testGetDefaultServices() {
		AccessContext context = new AccessContext();
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
		AccessContext context = new AccessContext();
		assertNotNull(context.getDataService("SampleCustomService"));
	}

}
