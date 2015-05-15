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

import com.ikanow.aleph2.access_manager.data_access.sample_services.ICustomNestedService;
import com.ikanow.aleph2.access_manager.data_access.sample_services.ICustomService;
import com.ikanow.aleph2.access_manager.data_access.sample_services.SampleCustomServiceNestedOne;
import com.ikanow.aleph2.access_manager.data_access.sample_services.SampleCustomServiceOne;
import com.ikanow.aleph2.access_manager.data_access.sample_services.SampleCustomServiceTwo;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
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
	public void testNestedInjection() throws Exception {
		//TODO setup the injection I want, need to test some nested injection
		Map<String, Object> configMap = new HashMap<String, Object>();
		configMap.put("service.SampleNestedCustomServiceOne.interface", "com.ikanow.aleph2.access_manager.data_access.sample_services.SampleICustomNestedService");
		configMap.put("service.SampleNestedCustomServiceOne.service", "com.ikanow.aleph2.access_manager.data_access.sample_services.SampleCustomNestedServiceOne");
		configMap.put("service.SampleNestedCustomServiceOne.default", true);
		ModuleUtils.loadModulesFromConfig(ConfigFactory.parseMap(configMap));
		AccessContext context = new AccessContext();
		
		ICustomNestedService service_one = context.getService(ICustomNestedService.class, Optional.of("SampleNestedCustomServiceOne"));
		ICustomNestedService service_two = context.getService(ICustomNestedService.class, Optional.of("SampleNestedCustomServiceTwo"));
		assertNotEquals(service_one.hashCode(), service_two.hashCode());
		
	}
	
	/**
	 * Checks that having 2 items set to default for the same interface will fail
	 */
	@Test
	public void testTwoDefaultsFail() {
		Map<String, Object> configMap = new HashMap<String, Object>();
		configMap.put("service.SampleNestedCustomServiceOne.interface", "com.ikanow.aleph2.access_manager.data_access.sample_services.SampleICustomNestedService");
		configMap.put("service.SampleNestedCustomServiceOne.service", "com.ikanow.aleph2.access_manager.data_access.sample_services.SampleCustomNestedServiceOne");
		configMap.put("service.SampleNestedCustomServiceOne.default", true);	
		configMap.put("service.SampleNestedCustomServiceUnderlying.interface", "com.ikanow.aleph2.access_manager.data_access.sample_services.SampleICustomNestedService");
		configMap.put("service.SampleNestedCustomServiceUnderlying.service", "com.ikanow.aleph2.access_manager.data_access.sample_services.SampleCustomNestedServiceUnderlying");
		configMap.put("service.SampleNestedCustomServiceUnderlying.default", true);
		boolean threwException = false;
		try {
			ModuleUtils.loadModulesFromConfig(ConfigFactory.parseMap(configMap));
			AccessContext context = new AccessContext();
		} catch (Exception ex ){
			threwException = true;
		}
		assertTrue(threwException);
	}
	
	@Test
	public void testNoDefaultAutoSet() throws Exception {
		//test that the default services names are set as default unless they get overrode
		Map<String, Object> configMap = new HashMap<String, Object>();
		configMap.put("service.SecurityService.interface", "com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService");
		configMap.put("service.SecurityService.service", "com.ikanow.aleph2.access_manager.data_access.sample_services.SampleISecurityService");				
		ModuleUtils.loadModulesFromConfig(ConfigFactory.parseMap(configMap));
		AccessContext context = new AccessContext();
		
		assertNotNull( context.getService(ISecurityService.class, Optional.empty()));
	}
	
	/**
	 * Test that we can have 2 services loaded with the same interface (they
	 * must use different annotations so they don't collide).
	 * @throws Exception 
	 * 
	 */
	@Test
	public void testTwoServicesWithSameInterface() throws Exception {
		Map<String, Object> configMap = new HashMap<String, Object>();
		configMap.put("service.SampleNestedCustomServiceOne.interface", "com.ikanow.aleph2.access_manager.data_access.sample_services.ICustomService");
		configMap.put("service.SampleNestedCustomServiceOne.service", "com.ikanow.aleph2.access_manager.data_access.sample_services.SampleCustomServiceOne");
		configMap.put("service.SampleNestedCustomServiceTwo.interface", "com.ikanow.aleph2.access_manager.data_access.sample_services.ICustomService");
		configMap.put("service.SampleNestedCustomServiceTwo.service", "com.ikanow.aleph2.access_manager.data_access.sample_services.SampleCustomServiceTwo");
		ModuleUtils.loadModulesFromConfig(ConfigFactory.parseMap(configMap));
		AccessContext context = new AccessContext();
		
		ICustomService service_one = context.getService(ICustomService.class, Optional.of("SampleNestedCustomServiceOne"));
		ICustomService service_two = context.getService(ICustomService.class, Optional.of("SampleNestedCustomServiceTwo"));
		//They will be separate instances so they will not hash to the same code, but should be the same parent class		
		assertNotEquals(service_one.getClass(), service_two.getClass());
		assertNotEquals(service_one.hashCode(), service_two.hashCode());
		assertNotNull((SampleCustomServiceOne)service_one);
		assertNotNull((SampleCustomServiceTwo)service_two);
	}
	
	@Test
	public void testDefaultAndNamed() throws Exception {
		Map<String, Object> configMap = new HashMap<String, Object>();
		configMap.put("service.SampleCustomServiceOne.interface", "com.ikanow.aleph2.access_manager.data_access.sample_services.ICustomService");
		configMap.put("service.SampleCustomServiceOne.service", "com.ikanow.aleph2.access_manager.data_access.sample_services.SampleCustomServiceOne");
		configMap.put("service.SampleCustomServiceOne.default", true);		
		ModuleUtils.loadModulesFromConfig(ConfigFactory.parseMap(configMap));
		AccessContext context = new AccessContext();
		
		ICustomService service_one = context.getService(ICustomService.class, Optional.of("SampleCustomServiceOne"));
		ICustomService service_two = context.getService(ICustomService.class, Optional.empty());
		//They will be separate instances so they will not hash to the same code, but should be the same parent class
		assertEquals(service_one.getClass(), service_two.getClass());
		
		//this is making the same check as above, just being explicit for brevity
		SampleCustomServiceOne service_one_casted = (SampleCustomServiceOne) service_one;
		assertEquals(service_one_casted.getClass(), service_two.getClass());
	}
	
	/**
	 * Creates 2 services that bind custom dependencies to the same interface but
	 * 2 different implementations, make sure they each get the correct implementation.
	 * @throws Exception 
	 * 
	 */
	@Test
	public void testNestedDependenciesDontCollide() throws Exception {
		Map<String, Object> configMap = new HashMap<String, Object>();		
		configMap.put("service.SampleCustomServiceOne.service", "com.ikanow.aleph2.access_manager.data_access.sample_services.SampleCustomServiceOne");
		configMap.put("service.SampleCustomServiceOne.default", true);			
		configMap.put("service.SampleCustomServiceTwo.service", "com.ikanow.aleph2.access_manager.data_access.sample_services.SampleCustomServiceTwo");
		configMap.put("service.SampleCustomServiceTwo.default", true);
		ModuleUtils.loadModulesFromConfig(ConfigFactory.parseMap(configMap));
		AccessContext context = new AccessContext();
		
		SampleCustomServiceOne service_one = context.getService(SampleCustomServiceOne.class, Optional.empty());
		System.out.println(service_one.dep.getANumber());
		SampleCustomServiceTwo service_two = context.getService(SampleCustomServiceTwo.class, Optional.empty());
		
		assertNotEquals(service_one.dep.getANumber(), service_two.dep.getANumber());
	}
	
	/**
	 * SampleCustomServiceOne will set up a binding to IDepedency with the annotation "SampleDepOne"
	 * SampleCustomServicePrivate will try inject that same IDependency "SampleDepOne"
	 * 
	 */
	@Test
	public void testPrivateDepedencies() {
		Map<String, Object> configMap = new HashMap<String, Object>();		
		configMap.put("service.SampleCustomServiceOne.service", "com.ikanow.aleph2.access_manager.data_access.sample_services.SampleCustomServiceOne");
		configMap.put("service.SampleCustomServiceOne.default", true);			
		configMap.put("service.SampleCustomServicePrivate.service", "com.ikanow.aleph2.access_manager.data_access.sample_services.SampleCustomServicePrivate");
		configMap.put("service.SampleCustomServicePrivate.default", true);
		
		boolean threwException = false; 
		try {
			ModuleUtils.loadModulesFromConfig(ConfigFactory.parseMap(configMap));
			AccessContext context = new AccessContext();
		} catch (Exception ex) {
			threwException = true;
		}
		assertTrue(threwException);
	}

	@Test
	public void testInjectOtherService() throws Exception {
		Map<String, Object> configMap = new HashMap<String, Object>();		
		configMap.put("service.SampleCustomServiceOne.service", "com.ikanow.aleph2.access_manager.data_access.sample_services.SampleCustomServiceOne");
		configMap.put("service.SampleCustomServiceOne.default", true);			
		configMap.put("service.SampleCustomServiceNestedOne.service", "com.ikanow.aleph2.access_manager.data_access.sample_services.SampleCustomServiceNestedOne");
		configMap.put("service.SampleCustomServiceNestedOne.default", true);
		ModuleUtils.loadModulesFromConfig(ConfigFactory.parseMap(configMap));
		AccessContext context = new AccessContext();
		
		SampleCustomServiceOne service_one = context.getService(SampleCustomServiceOne.class, Optional.empty());
		System.out.println(service_one.dep.getANumber());
		SampleCustomServiceNestedOne service_nested = context.getService(SampleCustomServiceNestedOne.class, Optional.empty());
		
		assertEquals(service_one.dep.getANumber(), service_nested.other_service.dep.getANumber());
	}
	
	@Test
	public void testServicesAreSingletons() throws Exception {
		Map<String, Object> configMap = new HashMap<String, Object>();
		configMap.put("service.SampleNestedCustomServiceOne.interface", "com.ikanow.aleph2.access_manager.data_access.sample_services.ICustomService");
		configMap.put("service.SampleNestedCustomServiceOne.service", "com.ikanow.aleph2.access_manager.data_access.sample_services.SampleCustomServiceOne");
		ModuleUtils.loadModulesFromConfig(ConfigFactory.parseMap(configMap));
		AccessContext context = new AccessContext();
		
		ICustomService service_one = context.getService(ICustomService.class, Optional.of("SampleNestedCustomServiceOne"));
		ICustomService service_two = context.getService(ICustomService.class, Optional.of("SampleNestedCustomServiceOne"));
		
		//They should be the same instance	
		assertEquals(service_one.hashCode(), service_two.hashCode());
	}
}
