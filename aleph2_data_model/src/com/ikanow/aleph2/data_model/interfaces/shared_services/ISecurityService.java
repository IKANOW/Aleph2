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
package com.ikanow.aleph2.data_model.interfaces.shared_services;

import java.util.Collection;
import java.util.Optional;

import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;

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
public interface ISecurityService extends IUnderlyingService {
		
	public String IKANOW_SYSTEM_LOGIN = "IKANOW_SECURITY_LOGIN";
	public String IKANOW_SYSTEM_PASSWORD = "IKANOW_SECURITY_PASSWORD";

	public static String SECURITY_ASSET_COMMUNITY="community";
	public static String SECURITY_ASSET_SOURCE="source";

	public static String ACTION_READ="read";
	public static String ACTION_READ_WRITE="read,write";
	public static String ACTION_WRITE="write";
	public static String ACTION_WILDCARD="*";

	public ISubject login(String principalName, Object credentials);

	public ISubject loginAsSystem();

	public boolean hasRole(ISubject subject, String role);
	
	public boolean isPermitted(ISubject subject, String string);
	
    /**
     * Allows this subject to 'run as' or 'assume' another identity indefinitely.  This can only be
     * called when the {@code Subject} instance already has an identity (i.e. they are remembered from a previous
     * log-in or they have authenticated during their current session).
     * <p/>
     * Some notes about {@code runAs}:
     * <ul>
     * <li>You can tell if a {@code Subject} is 'running as' another identity by calling the
     * {@link #isRunAs() isRunAs()} method.</li>
     * <li>If running as another identity, you can determine what the previous 'pre run as' identity
     * was by calling the {@link #getPreviousPrincipals() getPreviousPrincipals()} method.</li>
     * <li>When you want a {@code Subject} to stop running as another identity, you can return to its previous
     * 'pre run as' identity by calling the {@link #releaseRunAs() releaseRunAs()} method.</li>
     * </ul>
     *
     * @param principals the identity to 'run as', aka the identity to <em>assume</em> indefinitely.
     * @throws NullPointerException  if the specified principals collection is {@code null} or empty.
     * @throws IllegalStateException if this {@code Subject} does not yet have an identity of its own.
     */
	void runAs(ISubject subject,Collection<String> principals); 
	
    /**
     * Releases the current 'run as' (assumed) identity and reverts back to the previous 'pre run as'
     * identity that existed before {@code #runAs runAs} was called.
     * <p/>
     * This method returns 'run as' (assumed) identity being released or {@code null} if this {@code Subject} is not
     * operating under an assumed identity.
     *
     * @return the 'run as' (assumed) identity being released or {@code null} if this {@code Subject} is not operating
     *         under an assumed identity.
     * @see #runAs
     */
    Collection<String> releaseRunAs(ISubject subject);

	/** Returns a secured management CRUD
	 * @param crud - the delegate 
	 * @param authorizationBean - the authorization context of the calling "user"
	 * @return
	 */
	<O> IManagementCrudService<O> secured(IManagementCrudService<O> crud, AuthorizationBean authorizationBean);

	/** Returns a secure data provider
	 * @param provider
	 * @param authorizationBean - the authorization context of the calling "user"
	 * @return
	 */
	IDataServiceProvider secured(IDataServiceProvider provider, AuthorizationBean authorizationBean);
	
	void invalidateAuthenticationCache(Collection<String> principalNames);

	void invalidateCache();

	/** 
	 * This function enables the Jvm Security Manager in the system.
	 * @param enabled
	 */
	void enableJvmSecurityManager(boolean enabled);

	/** 
	 * This function enables the JVM Security on a per thread basis and can be used to wrap function calls. 
	 * @param enabled
	 */
	void enableJvmSecurity(boolean enabled);

	/**
	 * This function is a placeholder of the implementing security service to provide a callback for monitoring modifications etc.
	 * @return boolean - true if changes to policies have been performed and need to be re-loaded
	 */
	default boolean isCacheInvalid(){
		return false;
	}

	/** 
	 * Checks if a user has permission on a specific object,e.g.a DataBucketBEan etc. The service must be logged in as a system user to check the permission.
	 * The objectId or fullName wilbe extracted and the check will be performed.
	 * @param Optional<String> userID useId of the asset 'owner' or whoever has potentially the permission. 
	 * If not set then one as to use runAs witjh the userId the user beforehand, and releaseRunAs() afterwards,e.g. if one wants to check multiple times with the same userId. 
	 * @param assetOrPermission the permissible object.
	 * @param action - read,write wor wildcard action for permission
	 * @return true if user has permission, false otherwise
	 */
	public boolean isUserPermitted(Optional<String> userID, Object assetOrPermission, Optional<String> action);

	boolean hasUserRole(Optional<String> userID, String role);
}
