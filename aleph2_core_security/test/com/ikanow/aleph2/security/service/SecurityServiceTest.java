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

import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Optional;

import org.apache.shiro.authc.UsernamePasswordToken;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISubject;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

public class SecurityServiceTest {
	protected Config config = null;

	@Inject
	protected IServiceContext _service_context = null;

	protected ISecurityService securityService = null;
	@Before
	public void setupDependencies() throws Exception {
		if (_service_context != null) {
			return;
		}

		final String temp_dir = System.getProperty("java.io.tmpdir");

		// OK we're going to use guice, it was too painful doing this by hand...
		config = ConfigFactory.parseReader(new InputStreamReader(this.getClass().getResourceAsStream("/test_core_security.properties")))
				.withValue("globals.local_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
				.withValue("globals.local_cached_jar_dir", ConfigValueFactory.fromAnyRef(temp_dir))
				.withValue("globals.distributed_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
				.withValue("globals.local_yarn_config_dir", ConfigValueFactory.fromAnyRef(temp_dir));

		Injector app_injector = ModuleUtils.createTestInjector(Arrays.asList(), Optional.of(config));
		app_injector.injectMembers(this);
		this.securityService =  _service_context.getSecurityService();
	}

	@Test
	public void testUnauthenticated() {
		ISubject subject = securityService.getSubject();
		assertNotNull(subject);
		assertEquals(false, subject.isAuthenticated());		
	}

	@Test
	public void testAuthenticated() {        
        ISubject subject = securityService.login("lonestarr", "vespa");
		assertEquals(true, subject.isAuthenticated());		
	}
	
	@Test
	public void testRole(){
		String role = "schwartz";
        UsernamePasswordToken token = new UsernamePasswordToken("lonestarr", "vespa");
        token.setRememberMe(true);
        ISubject subject = securityService.login("lonestarr", "vespa");
		assertEquals(true,securityService.hasRole(subject,role));
		
	}

	@Test
	@Ignore
	public void testSecondRealm(){
		String role = "admin";
        UsernamePasswordToken token = new UsernamePasswordToken();
        token.setRememberMe(true);
        ISubject subject = securityService.login("trojan", "none");
		assertEquals(true,securityService.hasRole(subject,role));
		
	}
}
