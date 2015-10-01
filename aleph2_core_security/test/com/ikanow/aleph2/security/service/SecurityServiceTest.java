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

import static org.junit.Assert.assertEquals;

import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.authc.AuthenticationException;
import org.junit.Before;
import org.junit.Test;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISubject;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.security.utils.ProfilingUtility;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

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

			e.printStackTrace();
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


}
