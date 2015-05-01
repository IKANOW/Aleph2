package com.ikanow.aleph2.access_manager.data_access;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ikanow.aleph2.access_manager.data_access.AccessContext;
import com.ikanow.aleph2.access_manager.data_access.util.ConfigUtil;
import com.ikanow.aleph2.data_model.interfaces.data_layers.IDataService;
import com.typesafe.config.Config;

public class TestAccessManager {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		//TODO load the config
		Config test_config = ConfigUtil.loadTestConfig();
		
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
	
	@Test
	public void testGetCustomServiceDNE() {
		AccessContext context = new AccessContext();
		assertNull(context.getDataService("SampleCustomServiceDNE"));
	}

}
