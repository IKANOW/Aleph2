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
 *******************************************************************************/
package com.ikanow.aleph2.data_model.utils;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.ikanow.aleph2.data_model.interfaces.data_access.samples.ICustomService;
import com.ikanow.aleph2.data_model.interfaces.data_access.samples.ICustomService1;
import com.ikanow.aleph2.data_model.interfaces.data_access.samples.ICustomService2;
import com.ikanow.aleph2.data_model.interfaces.data_access.samples.SampleBadExtraDepedencyService;
import com.ikanow.aleph2.data_model.interfaces.data_access.samples.SampleCustomServiceOne;
import com.ikanow.aleph2.data_model.interfaces.data_access.samples.SampleCustomServicePlain;
import com.ikanow.aleph2.data_model.interfaces.data_access.samples.SampleCustomServiceThree;
import com.ikanow.aleph2.data_model.interfaces.data_access.samples.SampleCustomServiceTwo;
import com.ikanow.aleph2.data_model.interfaces.data_access.samples.SampleModule;
import com.ikanow.aleph2.data_model.interfaces.data_access.samples.SampleServiceContextService;
import com.ikanow.aleph2.data_model.interfaces.data_services.samples.SampleSecurityService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import fj.data.Either;

public class TestModuleUtils {

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
	public void test_LoadModulesFromConfig() throws Exception {
		Map<String, Object> configMap = new HashMap<String, Object>();		
		configMap.put("service.SampleCustomServiceOne.service", SampleCustomServiceOne.class.getCanonicalName());			
		ModuleUtils.loadModulesFromConfig(ConfigFactory.parseMap(configMap));
		
		SampleCustomServiceOne service_one = ModuleUtils.getService(SampleCustomServiceOne.class, Optional.empty());
		assertNotNull(service_one);
	}
	
	@Test
	public void test_ValidateOnlyOneDefault() {
		Map<String, Object> configMap = new HashMap<String, Object>();
		configMap.put("service.SampleCustomServiceOne.interface", ICustomService.class.getCanonicalName());
		configMap.put("service.SampleCustomServiceOne.service", SampleCustomServiceOne.class.getCanonicalName());	
		configMap.put("service.SampleCustomServiceOne.default", true);
		configMap.put("service.SampleCustomServiceTwo.interface", ICustomService.class.getCanonicalName());
		configMap.put("service.SampleCustomServiceTwo.service", SampleCustomServiceTwo.class.getCanonicalName());	
		configMap.put("service.SampleCustomServiceTwo.default", true);
		boolean threwException = false;
		try {
			ModuleUtils.loadModulesFromConfig(ConfigFactory.parseMap(configMap));
		} catch (Exception ex ){
			threwException = true;
		}
		assertTrue(threwException);
	}
	
	@Test
	public void test_GetExtraDepedencyModules() throws Exception {
		Map<String, Object> configMap = new HashMap<String, Object>();
		configMap.put("service.SampleCustomServiceOne.interface", ICustomService.class.getCanonicalName());
		configMap.put("service.SampleCustomServiceOne.service", SampleCustomServiceOne.class.getCanonicalName());	
		configMap.put("service.SampleCustomServiceOne.default", true);		
		ModuleUtils.loadModulesFromConfig(ConfigFactory.parseMap(configMap));
		
		ICustomService service_one = ModuleUtils.getService(ICustomService.class, Optional.of(SampleCustomServiceOne.class.getSimpleName()));
		assertNotNull(service_one);
	}
	
	@Test
	public void test_NoDefaultAutoSet() throws Exception {
		//test that the default services names are set as default unless they get overrode
		Map<String, Object> configMap = new HashMap<String, Object>();
		configMap.put("service.SecurityService.interface", ISecurityService.class.getCanonicalName());
		configMap.put("service.SecurityService.service", SampleSecurityService.class.getCanonicalName());
		ModuleUtils.loadModulesFromConfig(ConfigFactory.parseMap(configMap));		
		assertNotNull( ModuleUtils.getService(ISecurityService.class, Optional.empty()));
	}
	
	/**
	 * Test that we can have 2 services loaded with the same interface (they
	 * must use different annotations so they don't collide).
	 * @throws Exception 
	 * 
	 */
	@Test
	public void test_TwoServicesWithSameInterface() throws Exception {
		Map<String, Object> configMap = new HashMap<String, Object>();
		configMap.put("service.SampleCustomServiceOne.interface", ICustomService.class.getCanonicalName());
		configMap.put("service.SampleCustomServiceOne.service", SampleCustomServiceOne.class.getCanonicalName());			
		configMap.put("service.SampleCustomServiceTwo.interface", ICustomService.class.getCanonicalName());
		configMap.put("service.SampleCustomServiceTwo.service", SampleCustomServiceTwo.class.getCanonicalName());	
		ModuleUtils.loadModulesFromConfig(ConfigFactory.parseMap(configMap));
		
		ICustomService service_one = ModuleUtils.getService(ICustomService.class, Optional.of(SampleCustomServiceOne.class.getSimpleName()));
		ICustomService service_two = ModuleUtils.getService(ICustomService.class, Optional.of(SampleCustomServiceTwo.class.getSimpleName()));
		//They will be separate instances so they will not hash to the same code, but should be the same parent class		
		assertNotEquals(service_one.getClass(), service_two.getClass());
		assertNotEquals(service_one.hashCode(), service_two.hashCode());
		assertNotNull((SampleCustomServiceOne)service_one);
		assertNotNull((SampleCustomServiceTwo)service_two);
	}
	
	@Test
	public void test_DefaultAndNamed() throws Exception {
		Map<String, Object> configMap = new HashMap<String, Object>();
		configMap.put("service.SampleCustomServiceOne.interface", ICustomService.class.getCanonicalName());
		configMap.put("service.SampleCustomServiceOne.service", SampleCustomServiceOne.class.getCanonicalName());
		configMap.put("service.SampleCustomServiceOne.default", true);		
		ModuleUtils.loadModulesFromConfig(ConfigFactory.parseMap(configMap));
		
		ICustomService service_one = ModuleUtils.getService(ICustomService.class, Optional.of(SampleCustomServiceOne.class.getSimpleName()));
		ICustomService service_two = ModuleUtils.getService(ICustomService.class, Optional.empty());
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
	public void test_NestedDependenciesDontCollide() throws Exception {
		Map<String, Object> configMap = new HashMap<String, Object>();		
		configMap.put("service.SampleCustomServiceOne.interface", ICustomService.class.getCanonicalName());
		configMap.put("service.SampleCustomServiceOne.service", SampleCustomServiceOne.class.getCanonicalName());			
		configMap.put("service.SampleCustomServiceTwo.interface", ICustomService.class.getCanonicalName());
		configMap.put("service.SampleCustomServiceTwo.service", SampleCustomServiceTwo.class.getCanonicalName());
		ModuleUtils.loadModulesFromConfig(ConfigFactory.parseMap(configMap));		
		SampleCustomServiceOne service_one = (SampleCustomServiceOne) ModuleUtils.getService(ICustomService.class, Optional.of(SampleCustomServiceOne.class.getSimpleName()));		
		SampleCustomServiceTwo service_two = (SampleCustomServiceTwo) ModuleUtils.getService(ICustomService.class, Optional.of(SampleCustomServiceTwo.class.getSimpleName()));
		
		assertNotEquals(service_one.dep.getANumber(), service_two.dep.getANumber());
	}
	
	@Test
	public void test_ServicesAreSingletons() throws Exception {
		Map<String, Object> configMap = new HashMap<String, Object>();
		configMap.put("service.SampleCustomServiceOne.interface", ICustomService.class.getCanonicalName());
		configMap.put("service.SampleCustomServiceOne.service", SampleCustomServiceOne.class.getCanonicalName());	
		ModuleUtils.loadModulesFromConfig(ConfigFactory.parseMap(configMap));
		
		ICustomService service_one = ModuleUtils.getService(ICustomService.class, Optional.of(SampleCustomServiceOne.class.getSimpleName()));
		ICustomService service_two = ModuleUtils.getService(ICustomService.class, Optional.of(SampleCustomServiceOne.class.getSimpleName()));
		
		//They should be the same instance	
		assertEquals(service_one.hashCode(), service_two.hashCode());
	}
	
	@Test
	public void test_GetServiceWithoutInterface() throws Exception {
		Map<String, Object> configMap = new HashMap<String, Object>();
		configMap.put("service.SampleCustomServiceOne.service", SampleCustomServiceOne.class.getCanonicalName());	
		ModuleUtils.loadModulesFromConfig(ConfigFactory.parseMap(configMap));
		
		SampleCustomServiceOne service_one = ModuleUtils.getService(SampleCustomServiceOne.class, Optional.empty());
		assertNotNull(service_one);
	}
	
	@Test
	public void test_InjectServiceContext() throws Exception {
		Map<String, Object> configMap = new HashMap<String, Object>();
		configMap.put("service.SampleCustomServiceOne.service", SampleCustomServiceOne.class.getCanonicalName());
		configMap.put("service.SampleServiceContextService.service", SampleServiceContextService.class.getCanonicalName());	
		ModuleUtils.loadModulesFromConfig(ConfigFactory.parseMap(configMap));
		
		SampleServiceContextService service_one = ModuleUtils.getService(SampleServiceContextService.class, Optional.empty());
		assertNotNull(service_one);
		assertNotNull(service_one.getServiceContext());
		assertNotNull(service_one.getServiceContext().getService(SampleCustomServiceOne.class, Optional.empty()));
	}
	
	@Test
	public void test_CreatingInjector() throws Exception {
		Map<String, Object> configMap = new HashMap<String, Object>();
		Injector injector = ModuleUtils.createTestInjector(Arrays.asList(new SampleModule()), Optional.of(ConfigFactory.parseMap(configMap)));
		SampleCustomServiceOne service_one = injector.getInstance(SampleCustomServiceOne.class);
		assertNotNull(service_one);
		assertEquals(service_one.dep.getANumber(), 1);
	}
	
	@Test
	public void test_InjectGlobalConfig_defaults() throws Exception {
		Map<String, Object> configMap = new HashMap<String, Object>();
		Injector injector = ModuleUtils.createTestInjector(Arrays.asList(new SampleModule()), Optional.of(ConfigFactory.parseMap(configMap)));
		SampleCustomServiceThree service_troi = injector.getInstance(SampleCustomServiceThree.class);
		assertNotNull(service_troi);
		assertEquals(service_troi._globals.local_cached_jar_dir(), GlobalPropertiesBean.__DEFAULT_LOCAL_CACHED_JARS_DIR);
		assertEquals(service_troi._globals.local_root_dir(), GlobalPropertiesBean.__DEFAULT_LOCAL_ROOT_DIR);
		assertEquals(service_troi._globals.local_yarn_config_dir(), GlobalPropertiesBean.__DEFAULT_LOCAL_YARN_CONFIG_DIR);
	}
	
	@Test
	public void test_InjectGlobalConfig_specified() throws Exception {
		Map<String, Object> configMap = new HashMap<String, Object>();
		configMap.put("globals.local_cached_jar_dir", "a");
		configMap.put("globals.local_root_dir", "b");
		configMap.put("globals.local_yarn_config_dir", "c");
		Injector injector = ModuleUtils.createTestInjector(Arrays.asList(new SampleModule()), Optional.of(ConfigFactory.parseMap(configMap)));
		SampleCustomServiceThree service_troi = injector.getInstance(SampleCustomServiceThree.class);
		assertNotNull(service_troi);
		assertEquals(service_troi._globals.local_cached_jar_dir(), "a");
		assertEquals(service_troi._globals.local_root_dir(), "b");
		assertEquals(service_troi._globals.local_yarn_config_dir(), "c");
	}
	
	
	@Test
	public void test_ForgotStaticModule() throws Exception {
		Map<String, Object> configMap = new HashMap<String, Object>();
		configMap.put("service.SampleBadExtraDepedencyService.service", SampleBadExtraDepedencyService.class.getCanonicalName());	
		boolean threwError = false;
		try {
			ModuleUtils.loadModulesFromConfig(ConfigFactory.parseMap(configMap));
		} catch (Exception ex) {
			threwError = true;
		}
		assertTrue(threwError);
	}
	
	/**
	 * We forgot to bind the module, should return null.
	 * 
	 * @throws Exception
	 */
	@Test
	public void test_GettingModuleThatDNE() throws Exception {
		Map<String, Object> configMap = new HashMap<String, Object>();
		ModuleUtils.loadModulesFromConfig(ConfigFactory.parseMap(configMap));
		assertNull(ModuleUtils.getService(SampleCustomServiceOne.class, Optional.empty()));
	}
	
	/**
	 * Two services that use the same class should use the same injector
	 * 
	 * @throws Excpetion
	 */
	@Test
	public void test_SameServiceUsesSingleInjector() throws Exception {
		Map<String, Object> configMap = new HashMap<String, Object>();
		configMap.put("service.SampleMultipleService1.service", SampleCustomServicePlain.class.getCanonicalName());
		configMap.put("service.SampleMultipleService1.interface", ICustomService1.class.getCanonicalName());
		configMap.put("service.SampleMultipleService2.service", SampleCustomServicePlain.class.getCanonicalName());
		configMap.put("service.SampleMultipleService2.interface", ICustomService2.class.getCanonicalName());
		
		ModuleUtils.loadModulesFromConfig(ConfigFactory.parseMap(configMap));
		SampleCustomServicePlain service1 = (SampleCustomServicePlain) ModuleUtils.getService(ICustomService1.class, Optional.of("SampleMultipleService1"));		
		SampleCustomServicePlain service2 = (SampleCustomServicePlain) ModuleUtils.getService(ICustomService2.class, Optional.of("SampleMultipleService2"));
		assertTrue(service1.initialization_id.equals(service2.initialization_id));
	}
	
	@Test
	public void test_BindingSameThingTwiceDiffAnnotation() throws Exception {
		Map<String, Object> configMap = new HashMap<String, Object>();
		
		configMap.put("service.SampleCustomServiceOne.interface", ICustomService.class.getCanonicalName());
		configMap.put("service.SampleCustomServiceOne.service", SampleCustomServiceOne.class.getCanonicalName());
		configMap.put("service.SampleCustomServiceTwo.interface", ICustomService.class.getCanonicalName());
		configMap.put("service.SampleCustomServiceTwo.service", SampleCustomServiceOne.class.getCanonicalName());
		
		ModuleUtils.loadModulesFromConfig(ConfigFactory.parseMap(configMap));
		SampleCustomServiceOne service1 = (SampleCustomServiceOne) ModuleUtils.getService(ICustomService.class, Optional.of("SampleCustomServiceOne"));		
		SampleCustomServiceOne service2 = (SampleCustomServiceOne) ModuleUtils.getService(ICustomService.class, Optional.of("SampleCustomServiceTwo"));
		assertEquals(service1.dep.getANumber(), service2.dep.getANumber());
	}
	
	/////////////////////////////////////////////////////////////////////////////////////
	
	// TEST APP INJECTOR	
	
	public static class TestAppInjector extends AbstractModule {

		public TestAppInjector() {
			// (Wait to demonstrate the concurrency)
			try { Thread.sleep(2000L); } catch (Exception e) {}
		}
		
		// User ctor
		public TestAppInjector(Boolean user_call) {		
		}
		
		@Override
		protected void configure() {
			this.bind(TestAppInjector.class).in(Scopes.SINGLETON);
		}
		
	}
	
	@Inject TestAppInjector test_app_injector;
	
	@Test
	public void test_appInjector() throws Exception {
		
		Map<String, Object> configMap = new HashMap<String, Object>();

		//(start with a test injector)
		ModuleUtils.createTestInjector(Arrays.asList(new TestAppInjector(true)), Optional.of(ConfigFactory.parseMap(configMap)));		
		
		final CompletableFuture<Injector> app_inj0 = ModuleUtils.getAppInjector(); // (this should point to the test injector)		
		assertTrue("Test injector insta returns", app_inj0.isDone());
		TestAppInjector diff_app = app_inj0.get().getInstance(TestAppInjector.class);
		
		final CompletableFuture<TestAppInjector> test1 = 		
				CompletableFuture.supplyAsync(Lambdas.wrap_u(() -> 
				ModuleUtils.initializeApplication(Arrays.asList(new TestAppInjector(true)), Optional.of(ConfigFactory.parseMap(configMap)), Either.left(TestAppInjector.class))));
				
		Thread.sleep(100L); // (let the app injector take over from the test one)
		
		final CompletableFuture<Injector> app_inj1 = ModuleUtils.getAppInjector();
		assertFalse("App injector only completes when the main job is done", app_inj1.isDone());
		

		ModuleUtils.initializeApplication(Arrays.asList(new TestAppInjector(true)), Optional.of(ConfigFactory.parseMap(configMap)), Either.right(this));
		
		assertEquals(test_app_injector, test1.get());
		assertTrue("App injector only completes when the main job is done", app_inj1.isDone());
		assertEquals(test_app_injector, app_inj1.get().getInstance(TestAppInjector.class));
		
		assertFalse("Test and app injectors are diff: ", diff_app.equals(test_app_injector));
		
		TestAppInjector test2 = ModuleUtils.initializeApplication(Arrays.asList(new TestAppInjector(true)), Optional.of(ConfigFactory.parseMap(configMap)), Either.left(TestAppInjector.class));
		
		assertEquals(test_app_injector, test2);
		
		final CompletableFuture<Injector> app_inj2 = ModuleUtils.getAppInjector();
		assertTrue("App injector auto completes afterwards", app_inj2.isDone());
		assertEquals(test_app_injector, app_inj2.get().getInstance(TestAppInjector.class));

	}

	/////////////////////////////////////////////////////////////////////////////////////
	
	// UTILITY TO HELP SERVICE CONTEXT TESTING
	
	public static void loadModulesFromConfig(Config config) throws Exception {
		ModuleUtils.loadModulesFromConfig(config);
	}

}
