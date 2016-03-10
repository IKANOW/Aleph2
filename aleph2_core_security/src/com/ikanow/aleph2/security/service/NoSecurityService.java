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

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;

import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISubject;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;

/** Stubbed out security class that allows anythign
 * @author Alex
 */
public class NoSecurityService implements ISecurityService {

	/** Mock security subject
	 * @author Alex
	 */
	public static class MockSubject implements ISubject {

		@Override
		public Object getSubject() {
			return this;
		}

		@Override
		public boolean isAuthenticated() {
			return true;
		}

		@Override
		public String getName() {
			return null;
		}		
	}
	
	@Override
	public Collection<Object> getUnderlyingArtefacts() {
		return Arrays.asList(this);
	}

	@Override
	public <T> Optional<T> getUnderlyingPlatformDriver(Class<T> driver_class,
			Optional<String> driver_options) {
		return Optional.empty();
	}

	@Override
	public ISubject login(String principalName, Object credentials) {
		return new MockSubject();
	}

	@Override
	public ISubject loginAsSystem() {
		return new MockSubject();
	}

	@Override
	public boolean hasRole(ISubject subject, String role) {
		return true;
	}

	@Override
	public boolean isPermitted(ISubject subject, String string) {
		return true;
	}

	@Override
	public void runAs(ISubject subject, Collection<String> principals) {
	}

	@Override
	public Collection<String> releaseRunAs(ISubject subject) {
		return Arrays.asList();
	}

	@Override
	public <O> IManagementCrudService<O> secured(
			IManagementCrudService<O> crud, AuthorizationBean authorizationBean) {
		return crud;
	}

	@Override
	public IDataServiceProvider secured(IDataServiceProvider provider,
			AuthorizationBean authorizationBean) {
		return provider;
	}

	@Override
	public void invalidateAuthenticationCache(Collection<String> principalNames) {
	}

	@Override
	public void invalidateCache() {

	}

	@Override
	public void enableJvmSecurityManager(boolean enabled) {
	}

	@Override
	public void enableJvmSecurity(boolean enabled) {
	}

	@Override
	public boolean isUserPermitted(Optional<String> userID,
			Object assetOrPermission, Optional<String> action) {
		return true;
	}

	@Override
	public boolean hasUserRole(Optional<String> userID, String role) {
		return true;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService#getUserAccessToken(java.lang.String)
	 */
	@Override
	public ISubject getUserContext(String user_id) {
		return new MockSubject();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService#getUserAccessToken(java.lang.String, java.lang.String)
	 */
	@Override
	public ISubject getUserContext(String user_id, String password) {
		return new MockSubject();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService#getSystemUserAccessToken()
	 */
	@Override
	public ISubject getSystemUserContext() {
		return new MockSubject();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService#isUserPermitted(com.ikanow.aleph2.data_model.interfaces.shared_services.ISubject, java.lang.Object, java.util.Optional)
	 */
	@Override
	public boolean isUserPermitted(ISubject user_token,
			Object assetOrPermission, Optional<String> action) {
		return true;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService#hasUserRole(com.ikanow.aleph2.data_model.interfaces.shared_services.ISubject, java.lang.String)
	 */
	@Override
	public boolean hasUserRole(ISubject user_token, String role) {
		return true;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService#invalidateUserContext(com.ikanow.aleph2.data_model.interfaces.shared_services.ISubject)
	 */
	@Override
	public void invalidateUserContext(ISubject subject) {
	}
}
