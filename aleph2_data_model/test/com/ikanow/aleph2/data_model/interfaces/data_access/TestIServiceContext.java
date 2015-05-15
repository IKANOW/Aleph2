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

import com.ikanow.aleph2.data_model.interfaces.data_access.samples.SampleCustomServiceOne;
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
		configMap.put("service.SecurityService.interface", "com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService");
		configMap.put("service.SecurityService.service", "com.ikanow.aleph2.data_model.interfaces.data_access.samples.SampleSecurityService");	
		ModuleUtils.loadModulesFromConfig(ConfigFactory.parseMap(configMap));		
		IServiceContext context = new ServiceContext();
		
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
	public void testGetCustomServices() throws Exception {
		Map<String, Object> configMap = new HashMap<String, Object>();
		configMap.put("service.SampleCustomServiceOne.interface", "com.ikanow.aleph2.data_model.interfaces.data_access.samples.ICustomService");
		configMap.put("service.SampleCustomServiceOne.service", "com.ikanow.aleph2.data_model.interfaces.data_access.samples.SampleCustomServiceOne");
		ModuleUtils.loadModulesFromConfig(ConfigFactory.parseMap(configMap));		
		IServiceContext context = new ServiceContext();
		assertNotNull(context.getService(SampleCustomServiceOne.class, Optional.empty()));
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
