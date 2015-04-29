package com.ikanow.aleph2.data_model.objects.shared;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ikanow.aleph2.access_manager.data_access.AccessDriver;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;

public class TestIdentity {

	private static ISecurityService security_service;
	private static Map<String, Object> token_basic_auth;
	private static Identity test_identity = null;
	private final static String testResourceName = "SomeTestResource";
	private final static String testIdentifier = "test_id_12345";
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		security_service = AccessDriver.getAccessContext().getSecurityService();
		token_basic_auth = new HashMap<String, Object>();
		token_basic_auth.put("Authorization", "Basic dXNlcjpwYXNzd29yZA=="); //basic auth "Basic user:password"
		test_identity = security_service.getIdentity(token_basic_auth);
	}
	
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		//clean up any security rules we created
		security_service.clearPermission(TestIdentity.class);
		security_service.clearPermission(testResourceName);
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
