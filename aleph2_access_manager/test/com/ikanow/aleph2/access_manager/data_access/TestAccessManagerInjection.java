package com.ikanow.aleph2.access_manager.data_access;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.inject.CreationException;
import com.ikanow.aleph2.access_manager.data_access.sample_services.SampleCustomNestedServiceOne;
import com.ikanow.aleph2.access_manager.data_access.sample_services.SampleICustomNestedService;
import com.ikanow.aleph2.access_manager.data_access.util.ConfigUtil;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class TestAccessManagerInjection {

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
	public void testNestedInjection() {
		//TODO setup the injection I want, need to test some nested injection
		Map<String, Object> configMap = new HashMap<String, Object>();
		configMap.put("data_service.SampleNestedCustomServiceOne.interface", "com.ikanow.aleph2.access_manager.data_access.sample_services.SampleICustomNestedService");
		configMap.put("data_service.SampleNestedCustomServiceOne.service", "com.ikanow.aleph2.access_manager.data_access.sample_services.SampleCustomNestedServiceOne");
		configMap.put("data_service.SampleNestedCustomServiceOne.default", true);		
		AccessContext context = ConfigUtil.loadTestConfig(configMap);
		
		SampleICustomNestedService service_one = context.getDataService(SampleICustomNestedService.class, Optional.of("SampleNestedCustomServiceOne"));
		SampleICustomNestedService service_two = context.getDataService(SampleICustomNestedService.class, Optional.of("SampleNestedCustomServiceTwo"));
		assertNotEquals(service_one.hashCode(), service_two.hashCode());
		
	}
	
	/**
	 * Checks that having 2 items set to default for the same interface will fail
	 */
	@Test
	public void testTwoDefaultsFail() {
		Map<String, Object> configMap = new HashMap<String, Object>();
		configMap.put("data_service.SampleNestedCustomServiceOne.interface", "com.ikanow.aleph2.access_manager.data_access.sample_services.SampleICustomNestedService");
		configMap.put("data_service.SampleNestedCustomServiceOne.service", "com.ikanow.aleph2.access_manager.data_access.sample_services.SampleCustomNestedServiceOne");
		configMap.put("data_service.SampleNestedCustomServiceOne.default", true);	
		configMap.put("data_service.SampleNestedCustomServiceUnderlying.interface", "com.ikanow.aleph2.access_manager.data_access.sample_services.SampleICustomNestedService");
		configMap.put("data_service.SampleNestedCustomServiceUnderlying.service", "com.ikanow.aleph2.access_manager.data_access.sample_services.SampleCustomNestedServiceUnderlying");
		configMap.put("data_service.SampleNestedCustomServiceUnderlying.default", true);
		boolean threwException = false;
		try {
			ConfigUtil.loadTestConfig(configMap);
		} catch (CreationException ex ){
			threwException = true;
		}
		assertTrue(threwException);
	}
	
	@Test
	public void testNoDefaultAutoSet() {
		//test that the default services names are set as default unless they get overrode
		Map<String, Object> configMap = new HashMap<String, Object>();
		configMap.put("data_service.SecurityService.interface", "com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService");
		configMap.put("data_service.SecurityService.service", "com.ikanow.aleph2.access_manager.data_access.sample_services.SampleISecurityService");				
		AccessContext context = ConfigUtil.loadTestConfig(configMap);
		
		assertNotNull( context.getDataService(ISecurityService.class, Optional.empty()));
	}
	
	@Test
	public void testDefaultAndNamed() {
		Map<String, Object> configMap = new HashMap<String, Object>();
		configMap.put("data_service.SampleNestedCustomServiceOne.interface", "com.ikanow.aleph2.access_manager.data_access.sample_services.SampleICustomNestedService");
		configMap.put("data_service.SampleNestedCustomServiceOne.service", "com.ikanow.aleph2.access_manager.data_access.sample_services.SampleCustomNestedServiceOne");
		configMap.put("data_service.SampleNestedCustomServiceOne.default", true);		
		AccessContext context = ConfigUtil.loadTestConfig(configMap);
		
		SampleICustomNestedService service_one = context.getDataService(SampleICustomNestedService.class, Optional.of("SampleNestedCustomServiceOne"));
		SampleICustomNestedService service_two = context.getDataService(SampleICustomNestedService.class, Optional.empty());
		//They will be separate instances so they will not hash to the same code, but should be the same parent class
		assertEquals(service_one.getClass(), service_two.getClass());
		
		//this is making the same check as above, just being explicit for brevity
		SampleCustomNestedServiceOne service_one_casted = (SampleCustomNestedServiceOne) service_one;
		assertEquals(service_one_casted.getClass(), service_two.getClass());
	}

}
