package com.test;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.ikanow.aleph2.access_manager.data_access.AccessContext;
import com.ikanow.aleph2.access_manager.data_access.AccessMananger;
import com.ikanow.aleph2.access_manager.data_access.ServiceBinderModule;
import com.ikanow.aleph2.access_manager.data_access.ServiceModule;
import com.ikanow.aleph2.access_manager.data_access.sample_services.SampleCustomServiceNestedOne;
import com.ikanow.aleph2.access_manager.data_access.sample_services.SampleCustomServiceOne;
import com.ikanow.aleph2.access_manager.data_access.util.ConfigUtil;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class TestSecurityContext {

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
	public void test() throws Exception {
		Map<String, Object> configMap = new HashMap<String, Object>();		
		configMap.put("data_service.SecurityService.interface", "com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService");
		configMap.put("data_service.SecurityService.service", "com.test.SampleSecurityService");
		configMap.put("data_service.SecurityService.default", true);
		Config config = ConfigFactory.parseMap( configMap );
		
		//create service context
		Injector parent_injector = Guice.createInjector(new ServiceModule());
		
		//give child injector to each config service
		Injector security_injector = parent_injector.createChildInjector(new SampleSecurityServiceModule(), 
				new ServiceBinderModule(SampleSecurityService.class, Optional.of(ISecurityService.class), Optional.empty()));
		
		ISecurityService service = security_injector.getInstance(ISecurityService.class);
		
		//make sure it got the servercontext injected
		service.getIdentity(null);
	}

}
