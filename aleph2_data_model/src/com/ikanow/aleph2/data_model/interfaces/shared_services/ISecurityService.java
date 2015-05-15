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

import org.checkerframework.checker.nullness.qual.NonNull;

import com.ikanow.aleph2.data_model.objects.shared.Identity;

/**
 * Handles permissions for the system.  All permissions are in reference to an Identity object so
 * all calls require an identity to be passed in with the requested resources/permissions being granted/revoked.
 * (save the clearPermission methods that just remove permissions on those resources for everyone, typically
 * used when deleting an entire resource).
 * 
 * 
 * @author Burch
 *
 */
public interface ISecurityService {
	
	/**
	 * Checks if the given identifier has permission to access the given resource/resource_id and perform the
	 * specific operation (typically one of CRUD).
	 * 
	 * @param identity The identity trying to access the resource
	 * @param resourceClass Any object that has permission rules associated with it
	 * @param resourceIdentifier An identifier for that resource if necessary, null if not
	 * @param operation The operation this identity wants to perform, usually one of CRUD
	 * @return
	 */
	public boolean hasPermission(@NonNull Identity identity, @NonNull Class<?> resourceClass, String resourceIdentifier, @NonNull String operation);

	/**
	 * An alternative permission that allows any string name for a resourceName, this is not recommended as two
	 * applications can collide when submitting rules (e.g. app1 submits a rule for "some rule name" and app2 submits 
	 * a rule for the same "some rule name".
	 * 
	 * @param identity The identity trying to access the resource
	 * @param resourceName The name of any object that has permission rules associated with it
	 * @param resourceIdentifier An identifier for that resource if necessary, null if not
	 * @param operation The operation this identity wants to perform, usually one of CRUD
	 * @return
	 */
	public boolean hasPermission(@NonNull Identity identity, @NonNull String resourceName, String resourceIdentifier, @NonNull String operation);
	
	/**
	 * Returns back an identity given the token, for example if your security service is
	 * implementing basic auth, you might put the Authorization header in the token
	 * and the service would check that against the basic auth db, returning the user
	 * associated with the passed in basic auth token.
	 * 
	 * @param token A map containing authentication tokens, these are different depending on what the currently
	 * injected SecurityService requires.  E.g. an OAuth service might require the Authorization header to be passed in.
	 * @return
	 * @throws Exception
	 */
	public Identity getIdentity(@NonNull Map<String, Object> token) throws Exception;

	/**
	 * Methods for adding permission rules with resource class
	 * 
	 * @param identity The identity trying to access the resource
	 * @param resourceClass Any object that has permission rules associated with it
	 * @param resourceIdentifier An identifier for that resource if necessary, null if not
	 * @param operation The operation this identity wants to perform, usually one of CRUD
	 */
	public void grantPermission(@NonNull Identity identity, @NonNull Class<?> resourceClass, String resourceIdentifier, @NonNull String operation);
	
	/**
	 * Method for adding permission rules with arbitrary resource
	 * @param identity The identity trying to access the resource
	 * @param resourceName The name of any object that has permission rules associated with it
	 * @param resourceIdentifier An identifier for that resource if necessary, null if not
	 * @param operation
	 */
	public void grantPermission(@NonNull Identity identity, @NonNull String resourceName, String resourceIdentifier, @NonNull String operation);
	
	/**
	 * Methods for removing permission rules with resource class
	 * 
	 * @param identity The identity trying to access the resource
	 * @param resourceClass Any object that has permission rules associated with it
	 * @param resourceIdentifier An identifier for that resource if necessary, null if not
	 * @param operation The operation this identity wants to perform, usually one of CRUD
	 */
	public void revokePermission(@NonNull Identity identity, @NonNull Class<?> resourceClass, String resourceIdentifier, @NonNull String operation);
	
	/**
	 * Methods for removing permission rules with arbitrary resource
	 * 
	 * @param identity The identity trying to access the resource
	 * @param resourceName The name of any object that has permission rules associated with it
	 * @param resourceIdentifier An identifier for that resource if necessary, null if not
	 * @param operation The operation this identity wants to perform, usually one of CRUD
	 */
	public void revokePermission(@NonNull Identity identity, @NonNull String resourceName, String resourceIdentifier, @NonNull String operation);
	
	/**
	 * Utility function to remove all permissions from a resource class
	 * 
	 * @param resourceClass Any object that has permission rules associated with it
	 * @param resourceIdentifier An identifier for that resource if necessary, null if not
	 */
	public void clearPermission(@NonNull Class<?> resourceClass, String resourceIdentifier);
	
	/**
	 * Utility function to remove all permissions from a resource
	 * 
	 * @param resourceName The name of any object that has permission rules associated with it
	 * @param resourceIdentifier An identifier for that resource if necessary, null if not
	 */
	public void clearPermission(@NonNull String resourceName, String resourceIdentifier);
}
