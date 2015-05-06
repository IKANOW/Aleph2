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

import java.util.Map;

import com.ikanow.aleph2.data_model.objects.shared.Identity;

public interface ISecurityService {
	
	/**
	 * Checks if the given identifier has permission to access the given resource/resource_id and perform the
	 * specific operation (typically one of CRUD).
	 * 
	 * @param identity
	 * @param resource_class
	 * @param resource_identifier
	 * @param operation
	 * @return
	 */
	public boolean hasPermission(Identity identity, Class<?> resourceClass, String resourceIdentifier, String operation);

	/**
	 * An alternative permission that allows any string name for a resourceName, this is not recommended as two
	 * applications can collide when submitting rules (e.g. app1 submits a rule for "some rule name" and app2 submits 
	 * a rule for the same "some rule name".
	 * 
	 * @param identity
	 * @param resourceName
	 * @param identifier
	 * @param operation
	 * @return
	 */
	public boolean hasPermission(Identity identity, String resourceName, String resourceIdentifier, String operation);
	
	/**
	 * Returns back an identity given the token, for example if your security service is
	 * implementing basic auth, you might put the Authorization header in the token
	 * and the service would check that against the basic auth db, returning the user
	 * associated with the passed in basic auth token.
	 * 
	 * @param token
	 * @return
	 * @throws Exception
	 */
	public Identity getIdentity(Map<String, Object> token) throws Exception;

	/**
	 * Methods for adding permission rules with resource class
	 * 
	 * @param identity
	 * @param resource_class
	 * @param resource_identifier
	 * @param operation
	 */
	public void grantPermission(Identity identity, Class<?> resourceClass, String resourceIdentifier, String operation);
	
	/**
	 * Method for adding permission rules with arbitrary resource
	 * @param identity
	 * @param resourceName
	 * @param resource_identifier
	 * @param operation
	 */
	public void grantPermission(Identity identity, String resourceName, String resourceIdentifier, String operation);
	
	/**
	 * Methods for removing permission rules with resource class
	 * 
	 * @param identity
	 * @param resource_class
	 * @param resource_identifier
	 * @param operation
	 */
	public void revokePermission(Identity identity, Class<?> resourceClass, String resourceIdentifier, String operation);
	
	/**
	 * Methods for removing permission rules with arbitrary resource
	 * 
	 * @param identity
	 * @param resourceName
	 * @param resource_identifier
	 * @param operation
	 */
	public void revokePermission(Identity identity, String resourceName, String resourceIdentifier, String operation);
	
	/**
	 * Utility function to remove all permissions from a resource class
	 * 
	 * @param resourceClass
	 */
	public void clearPermission(Class<?> resourceClass);
	
	/**
	 * Utility function to remove all permissions from a resource class
	 * 
	 * @param resourceName
	 */
	public void clearPermission(String resourceName);
}
