package com.ikanow.aleph2.data_model.objects.shared;

import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;

/**
 * Represents the identity of a user or usergroup
 * 
 * @author Burch
 *
 */
public abstract class Identity {

	@Inject ISecurityService security_service;
	
	public boolean hasPermission(Object resource, String operation) {
		//check the security service if there is a rule for this identity/resource/operation
		return security_service.hasPermission(this, resource.getClass(), null, operation);		
	}
	
	public boolean hasPermission(Class<?> resourceClass, String identifier, String operation) {
		return security_service.hasPermission(this, resourceClass, identifier, operation);
	}
	
	public boolean hasPermission(String resourceName, String identifier, String operation) {
		return security_service.hasPermission(this, resourceName, identifier, operation);
	}
}
