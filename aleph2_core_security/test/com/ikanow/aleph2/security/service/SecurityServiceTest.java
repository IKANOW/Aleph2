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
package com.ikanow.aleph2.security.service;

import static org.junit.Assert.*;

import java.io.File;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.authc.AuthenticationException;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISubject;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.FutureUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.SingleQueryComponent;
import com.ikanow.aleph2.data_model.utils.FutureUtils.ManagementFuture;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.security.utils.ErrorUtils;
import com.ikanow.aleph2.security.utils.ProfilingUtility;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import static org.mockito.Mockito.*;


public class SecurityServiceTest {
	private static final Logger logger = LogManager.getLogger(SecurityServiceTest.class);

	protected Config config = null;
	

	@Inject
	protected IServiceContext _temp_service_context = null;
	protected static IServiceContext _service_context = null;

	protected ISecurityService securityService = null;
	
	protected String regularUserId = "user";

	@Before
	public void setupDependencies() throws Exception {
		try {
			if (null == _service_context) {

				final String temp_dir = System.getProperty("java.io.tmpdir");

				// OK we're going to use guice, it was too painful doing this by
				config = ConfigFactory
						.parseReader(new InputStreamReader(this.getClass().getResourceAsStream("/test_core_security.properties")))
						.withValue("globals.local_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
						.withValue("globals.local_cached_jar_dir", ConfigValueFactory.fromAnyRef(temp_dir))
						.withValue("globals.distributed_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
						.withValue("globals.local_yarn_config_dir", ConfigValueFactory.fromAnyRef(temp_dir));

				Injector app_injector = ModuleUtils.createTestInjector(Arrays.asList(), Optional.of(config));
				app_injector.injectMembers(this);
				_service_context = _temp_service_context;
			}
			this.securityService = _service_context.getSecurityService();
		} catch (Throwable e) {
			logger.error(ErrorUtils.getLongForm(ErrorUtils.EXCEPTION_CAUGHT, e));

		}

	}

	@Test
	public void testAuthenticated() {
		ISubject subject = loginAsTestUser();
        try {
		} catch (AuthenticationException e) {
			logger.info("Caught (expected) Authentication exception:"+e.getMessage());
			
		
		assertEquals(true, subject.isAuthenticated());		
		}
	}
	
	@Test
	public void testRole(){
		ISubject subject = loginAsAdmin();
        //test a typed permission (not instance-level)
		assertEquals(true,securityService.hasRole(subject,"admin"));
	}

	@Test
	public void testPermission(){
		ISubject subject = loginAsRegularUser();
		String permission = "permission1";
        //test a typed permission (not instance-level)
		assertEquals(true,securityService.isPermitted(subject,permission));
	}

	@Test
	public void testRunAs(){
		ISubject subject = loginAsTestUser();
		// system community
		String runAsPrincipal = "user";
		String runAsRole = "user";
		String runAsPersonalPermission = "permission1";
		
		securityService.runAs(subject,Arrays.asList(runAsPrincipal));
		
		assertEquals(true,securityService.hasRole(subject,runAsRole));
        //test a typed permission (not instance-level)
		assertEquals(true,securityService.isPermitted(subject,runAsPersonalPermission));
		Collection<String> p = securityService.releaseRunAs(subject);
		logger.debug("Released Principals:"+p);
		securityService.runAs(subject,Arrays.asList(runAsPrincipal));
		
		assertEquals(true,securityService.hasRole(subject,runAsRole));
        //test a typed permission (not instance-level)
		assertEquals(true,securityService.isPermitted(subject,runAsPersonalPermission));
		p = securityService.releaseRunAs(subject);
		logger.debug("Released Principals:"+p);
	}
	
	protected ISubject loginAsTestUser() throws AuthenticationException{
		ISubject subject = securityService.login("testUser","testUser123");			
		return subject;
	}

	protected ISubject loginAsAdmin() throws AuthenticationException{
		ISubject subject = securityService.login("admin","admin123");			
		return subject;
	}

	protected ISubject loginAsRegularUser() throws AuthenticationException{
		ISubject subject = securityService.login("user","user123");			
		return subject;
	}
	
	@Test
	public void testCaching(){
		ISubject subject = loginAsRegularUser();
		// test personal community permission
		String permission = "permission2";
        //test a typed permission (not instance-level)
		ProfilingUtility.timeStart("TU-permisssion0");
		assertEquals(true,securityService.isPermitted(subject,permission));
		ProfilingUtility.timeStopAndLog("TU-permisssion0");
		for (int i = 0; i < 10; i++) {
			ProfilingUtility.timeStart("TU-permisssion"+(i+1));
			assertEquals(true,securityService.isPermitted(subject,permission));			
			ProfilingUtility.timeStopAndLog("TU-permisssion"+(i+1));
		}
		subject = loginAsAdmin();
		// test personal community permission
		permission = "permission2";
        //test a typed permission (not instance-level)
		ProfilingUtility.timeStart("AU-permisssion0");
		assertEquals(true,securityService.isPermitted(subject,permission));
		ProfilingUtility.timeStopAndLog("AU-permisssion");
		for (int i = 0; i < 10; i++) {
			ProfilingUtility.timeStart("AU-permisssion"+i+1);
			assertEquals(true,securityService.isPermitted(subject,permission));			
			ProfilingUtility.timeStopAndLog("AU-permisssion"+i+1);
		}
		
		for (int i = 0; i < 10; i++) {
			ProfilingUtility.timeStart("TU2-permisssion_L"+(i+1));
		subject = loginAsRegularUser();
		ProfilingUtility.timeStopAndLog("TU2-permisssion_L"+(i+1));
        //test a typed permission (not instance-level)
		ProfilingUtility.timeStart("TU2-permisssion"+(i+1));
		assertEquals(true,securityService.isPermitted(subject,permission));
			ProfilingUtility.timeStopAndLog("TU2-permisssion"+(i+1));
		}
	}

	@Test
	public void testInvalidateAuthenticationCache(){
		ISubject subject = loginAsTestUser();
		
		securityService.runAs(subject,Arrays.asList(regularUserId));

		// test personal community permission
		String permission = "permission1";
        //test a typed permission (not instance-level)
		ProfilingUtility.timeStart("TU-permisssion0");
		assertEquals(true,securityService.isPermitted(subject,permission));
		ProfilingUtility.timeStopAndLog("TU-permisssion0");
		for (int i = 0; i < 10; i++) {
			ProfilingUtility.timeStart("TU-permisssion"+(i+1));
			assertEquals(true,securityService.isPermitted(subject,permission));			
			ProfilingUtility.timeStopAndLog("TU-permisssion"+(i+1));
			if(i==5){
				securityService.invalidateAuthenticationCache(Arrays.asList(regularUserId));	
				//loginAsRegularUser();
			}
		}
	}

	@Test
	public void testInvalidateCache(){
		ISubject subject = loginAsTestUser();
		
		securityService.runAs(subject,Arrays.asList(regularUserId));

		// test personal community permission
		String permission = "permission1";
        //test a typed permission (not instance-level)
		ProfilingUtility.timeStart("TU-permisssion0");
		assertEquals(true,securityService.isPermitted(subject,permission));
		ProfilingUtility.timeStopAndLog("TU-permisssion0");
		for (int i = 0; i < 10; i++) {
			ProfilingUtility.timeStart("TU-permisssion"+(i+1));
			assertEquals(true,securityService.isPermitted(subject,permission));			
			ProfilingUtility.timeStopAndLog("TU-permisssion"+(i+1));
			if(i==5){
				securityService.invalidateCache();	
				//loginAsRegularUser();
			}
		}
	}

	@Test
	public void testRunAsDemoted(){
		ISubject subject = loginAsAdmin();
		// system community
		String runAsPrincipal = regularUserId; 
		
		assertEquals(true,securityService.hasRole(subject,"admin"));

		securityService.runAs(subject,Arrays.asList(runAsPrincipal));
		
		assertEquals(false,securityService.hasRole(subject,"admin"));
		Collection<String> p = securityService.releaseRunAs(subject);
		assertEquals(true,securityService.hasRole(subject,"admin"));
		logger.debug("Released Principals:"+p);
	}

	@Test
	public void testJvmSecurity(){
		@SuppressWarnings("unused")
		ISubject subject = loginAsRegularUser();
		securityService.enableJvmSecurityManager(true);
		File f =  new File("/tmp/data/misc");
		f.list();
		try {
			f =  new File("/tmp/data/");
			f.list();
			fail("Read Access to "+f.getName()+" is not allowed.");
		} catch (Throwable t) {			
			logger.debug("This is correct we want to see a read a security exception here:"+t);
		}
		finally {
			securityService.enableJvmSecurityManager(false);
		} 
	}

	@Test
	@Ignore
	public void testSecurityServiceMultiThreading(){
		ISubject subject = securityService.loginAsSystem();
		long timeNow = System.currentTimeMillis();
		long timeOut = timeNow+3000L;
		boolean hasAdminRole = false;
		while(timeNow < timeOut){
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			hasAdminRole  = securityService.hasRole(subject, "admin");
			if(hasAdminRole){
				fail("Subject received admin rights");
				break;
			}
			timeNow = System.currentTimeMillis();
		} // while
	
	}


	
	@Test
	public void testSystemLogin() {
		ISubject subject = securityService.loginAsSystem();			
		securityService.releaseRunAs(subject);		
		securityService.runAs(subject, Arrays.asList("testUser"));
		
	}
	
	@Test
	public void testSecuredCrudManagementService(){
		 AuthorizationBean authBean = new AuthorizationBean("admin");
		
		IDummyCrudService mockedDelegate = mock(IDummyCrudService.class);
		
		
		CompletableFuture<Optional<String>> future = CompletableFuture.completedFuture(Optional.of("abc"));
		ManagementFuture<Optional<String>> managedFuture =  FutureUtils.createManagementFuture(future);
		when(mockedDelegate.getObjectBySpec(any())).thenReturn(managedFuture);
		when(mockedDelegate.getObjectById(any())).thenReturn(managedFuture);
		
		DummySecuredCrudService securedCrudService = new DummySecuredCrudService(_service_context, mockedDelegate, authBean);
		
		securedCrudService.storeObject("abc");
		securedCrudService.storeObjects(Arrays.asList("abc","efg"));
		securedCrudService.storeObjects(Arrays.asList("abc","efg"),false);
		securedCrudService.optimizeQuery(Arrays.asList("f"));
		SingleQueryComponent<String> query = CrudUtils.allOf(String.class);
		securedCrudService.getObjectBySpec(query);
		securedCrudService.getObjectBySpec(query,Arrays.asList("f"),true);
		securedCrudService.getObjectById(1,Arrays.asList("f"),true);
		
	}

}
