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
 ******************************************************************************/
package com.ikanow.aleph2.data_model.interfaces.shared_services;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ikanow.aleph2.data_model.interfaces.data_access.IServiceContext;
import com.ikanow.aleph2.data_model.objects.shared.Identity;
import com.ikanow.aleph2.data_model.utils.ContextUtils;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;

public class TestISecurityService {

	private static ISecurityService security_service;
	private static Map<String, Object> token_basic_auth;
	private static Identity test_identity = null;
	private final static String testResourceName = "SomeTestResource";
	private final static String testIdentifier = "test_id_12345";
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		security_service = new ModuleUtils.ServiceContext().getSecurityService();
		token_basic_auth = new HashMap<String, Object>();
		token_basic_auth.put("Authorization", "Basic dXNlcjpwYXNzd29yZA=="); //basic auth "Basic user:password"
		test_identity = security_service.getIdentity(token_basic_auth);
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
		revokeTestPermissions(test_identity, security_service, null);
	}
	
	private static void grantTestPermissions(Identity identity, ISecurityService security_service, String resourceIdentifier) {
		security_service.grantPermission(identity, TestISecurityService.class, resourceIdentifier, "READ");
		security_service.grantPermission(identity, testResourceName, resourceIdentifier, "READ");
	}
	
	private static void revokeTestPermissions(Identity identity, ISecurityService security_service, String resourceIdentifier) {
		security_service.clearPermission(TestISecurityService.class, resourceIdentifier);
		security_service.clearPermission(testResourceName, resourceIdentifier);
	}

	/**
	 * This method ends up testing both grant functions, both hasPerm functions
	 * 
	 */
	@Test
	public void testHasPermission() {
		//clear the rules so we know for sure there shouldn't be any
		revokeTestPermissions(test_identity, security_service, null);
		
		//there shouldn't be a rule for our current class, so test it fails
		assertFalse(security_service.hasPermission(test_identity, TestISecurityService.class, null, "READ"));
		assertFalse(security_service.hasPermission(test_identity, testResourceName, null, "READ"));
		
		//add some rules and test they work
		grantTestPermissions(test_identity, security_service, null);
		
		assertTrue(security_service.hasPermission(test_identity, TestISecurityService.class, null, "READ"));
		assertTrue(security_service.hasPermission(test_identity, testResourceName, null, "READ"));		
	}
	
	@Test
	public void testGetIdentity() {
		try {
			assertNotNull(security_service.getIdentity(token_basic_auth));
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testClearPermission() {
		//clear the rules so we know for sure there shouldn't be any
		revokeTestPermissions(test_identity, security_service, null);
		
		//there shouldn't be a rule for our current class, so test it fails
		assertFalse(security_service.hasPermission(test_identity, TestISecurityService.class, null, "READ"));
		assertFalse(security_service.hasPermission(test_identity, testResourceName, null, "READ"));
		
		//add some rules and test they work
		grantTestPermissions(test_identity, security_service, null);
		
		assertTrue(security_service.hasPermission(test_identity, TestISecurityService.class, null, "READ"));
		assertTrue(security_service.hasPermission(test_identity, testResourceName, null, "READ"));
		
		//clear the rules and make sure they no longer work
		security_service.clearPermission(TestISecurityService.class, null);		
		assertFalse(security_service.hasPermission(test_identity, TestISecurityService.class, null, "READ"));
		security_service.clearPermission(testResourceName, null);
		assertFalse(security_service.hasPermission(test_identity, testResourceName, null, "READ"));
	}
	
	@Test
	public void testRevokePermission() {
		//clear the rules so we know for sure there shouldn't be any
		revokeTestPermissions(test_identity, security_service, null);
		
		//there shouldn't be a rule for our current class, so test it fails
		assertFalse(security_service.hasPermission(test_identity, TestISecurityService.class, null, "READ"));
		assertFalse(security_service.hasPermission(test_identity, testResourceName, null, "READ"));
		
		//add some rules and test they work
		grantTestPermissions(test_identity, security_service, null);
		
		assertTrue(security_service.hasPermission(test_identity, TestISecurityService.class, null, "READ"));
		assertTrue(security_service.hasPermission(test_identity, testResourceName, null, "READ"));
		
		//revoke individual rules to make sure that works
		security_service.revokePermission(test_identity, TestISecurityService.class, null, "READ");
		assertFalse(security_service.hasPermission(test_identity, TestISecurityService.class, null, "READ"));
		security_service.revokePermission(test_identity, testResourceName, null, "READ");
		assertFalse(security_service.hasPermission(test_identity, testResourceName, null, "READ"));
	}
	
	@Test
	public void testHasPermissionIdentifier() {
		//clear the rules so we know for sure there shouldn't be any
		revokeTestPermissions(test_identity, security_service, null);
		
		//there shouldn't be a rule for our current class, so test it fails
		assertFalse(security_service.hasPermission(test_identity, TestISecurityService.class, testIdentifier, "READ"));
		assertFalse(security_service.hasPermission(test_identity, testResourceName, testIdentifier, "READ"));
		
		//add some rules and test they work
		grantTestPermissions(test_identity, security_service, testIdentifier);
		
		assertTrue(security_service.hasPermission(test_identity, TestISecurityService.class, testIdentifier, "READ"));
		assertTrue(security_service.hasPermission(test_identity, testResourceName, testIdentifier, "READ"));
	}
	
	@Test
	public void testClearPermissionIdentifier() {
		//clear the rules so we know for sure there shouldn't be any
		revokeTestPermissions(test_identity, security_service, null);
		
		//there shouldn't be a rule for our current class, so test it fails
		assertFalse(security_service.hasPermission(test_identity, TestISecurityService.class, testIdentifier, "READ"));
		assertFalse(security_service.hasPermission(test_identity, testResourceName, testIdentifier, "READ"));
		
		//add some rules and test they work
		grantTestPermissions(test_identity, security_service, testIdentifier);
		
		assertTrue(security_service.hasPermission(test_identity, TestISecurityService.class, testIdentifier, "READ"));
		assertTrue(security_service.hasPermission(test_identity, testResourceName, testIdentifier, "READ"));
		
		//clear the rules and make sure they no longer work
		security_service.clearPermission(TestISecurityService.class, testIdentifier);		
		assertFalse(security_service.hasPermission(test_identity, TestISecurityService.class, testIdentifier, "READ"));
		security_service.clearPermission(testResourceName, testIdentifier);
		assertFalse(security_service.hasPermission(test_identity, testResourceName, testIdentifier, "READ"));
	}
	
	@Test
	public void testRevokePermissionIdentifier() {
		//clear the rules so we know for sure there shouldn't be any
		revokeTestPermissions(test_identity, security_service, null);
		
		//there shouldn't be a rule for our current class, so test it fails
		assertFalse(security_service.hasPermission(test_identity, TestISecurityService.class, testIdentifier, "READ"));
		assertFalse(security_service.hasPermission(test_identity, testResourceName, testIdentifier, "READ"));
		
		//add some rules and test they work
		grantTestPermissions(test_identity, security_service, testIdentifier);
		
		assertTrue(security_service.hasPermission(test_identity, TestISecurityService.class, testIdentifier, "READ"));
		assertTrue(security_service.hasPermission(test_identity, testResourceName, testIdentifier, "READ"));
		
		//revoke individual rules to make sure that works
		security_service.revokePermission(test_identity, TestISecurityService.class, null, "READ");
		assertFalse(security_service.hasPermission(test_identity, TestISecurityService.class, null, "READ"));
		security_service.revokePermission(test_identity, testResourceName, null, "READ");
		assertFalse(security_service.hasPermission(test_identity, testResourceName, null, "READ"));
	}
}
