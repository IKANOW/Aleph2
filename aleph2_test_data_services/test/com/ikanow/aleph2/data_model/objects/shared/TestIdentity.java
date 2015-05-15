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
package com.ikanow.aleph2.data_model.objects.shared;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ikanow.aleph2.data_model.interfaces.data_access.IServiceContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.utils.ContextUtils;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;

public class TestIdentity {

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
		//clean up any security rules we created
		security_service.clearPermission(TestIdentity.class, null);
		security_service.clearPermission(testResourceName, null);
	}

	@Test
	public void testHasPermission() {				
		//all the rules should fail
		assertFalse(test_identity.hasPermission(this, "READ"));
		assertFalse(test_identity.hasPermission(TestIdentity.class, null, "READ"));
		assertFalse(test_identity.hasPermission(TestIdentity.class, testIdentifier, "READ"));
		assertFalse(test_identity.hasPermission(testResourceName, null, "READ"));
		assertFalse(test_identity.hasPermission(testResourceName, testIdentifier, "READ"));
		
		//grant permissions and try again
		security_service.grantPermission(test_identity, this.getClass(), null, "READ");
		assertTrue(test_identity.hasPermission(this, "READ"));
		assertTrue(test_identity.hasPermission(TestIdentity.class, null, "READ"));
		security_service.grantPermission(test_identity, this.getClass(), testIdentifier, "READ");
		assertTrue(test_identity.hasPermission(TestIdentity.class, testIdentifier, "READ"));
		security_service.grantPermission(test_identity, testResourceName, null, "READ");
		assertTrue(test_identity.hasPermission(testResourceName, null, "READ"));
		security_service.grantPermission(test_identity, testResourceName, testIdentifier, "READ");
		assertTrue(test_identity.hasPermission(testResourceName, testIdentifier, "READ"));
	}

}
