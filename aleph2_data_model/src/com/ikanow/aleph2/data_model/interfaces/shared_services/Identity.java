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

/**
 * Represents the identity of a user or usergroup for basing permission rules on.
 * An identity is the base object of the security model and it is an identity that
 * is told if it has permission to perform operations on specific resources.
 * 
 * @author Burch
 *
 */
public interface Identity {
	
	/**
	 * Returns back if this identity has permission to access the specific resource for
	 * the given operation.
	 * 
	 * @param resource The resource this identity wants to access
	 * @param operation The operation this identity wants to perform, usually one of CRUD
	 * @return
	 */
	public boolean hasPermission(Object resource, String operation);
	
	/**
	 * Returns back if this identity has permission to access the resource type with a specific 
	 * identity for the given operation.
	 * 
	 * @param resourceClass The class of the resource this identity wants to access
	 * @param identifier The identifier of the specific resource this identity wants to access, 
	 * or null for accessing the general class.  E.g. you might pass a user id if trying to 
	 * access a specific users profile, or you might pass null if you wanted to check if the identity
	 * has access to an application.
	 * @param operation The operation this identity wants to perform, usually one of CRUD
	 * @return
	 */
	public boolean hasPermission(Class<?> resourceClass, String identifier, String operation);
	
	/**
	 * Returns back if this identity has permission to access the resource type with a specific
	 * identity for the given operation.
	 * 
	 * @param resourceName The string name of the resource this identity wants to access
	 * @param identifier The identifier of the specific resource this identity wants to access, 
	 * or null for accessing the general class.  E.g. you might pass a user id if trying to 
	 * access a specific users profile, or you might pass null if you wanted to check if the identity
	 * has access to an application.
	 * @param operation The operation this identity wants to perform, usually one of CRUD
	 * @return
	 */
	public boolean hasPermission(String resourceName, String identifier, String operation);
}
