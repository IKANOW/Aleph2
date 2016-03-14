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
	public void enableJvmSecurity(Optional<String> principalName,boolean enabled) {
	}

	@Override
	public boolean isUserPermitted(String userID, Object assetOrPermission, Optional<String> action) {
		return true;
	}

	@Override
	public boolean hasUserRole(String userID, String role) {
		return true;
	}

	@Override
	public boolean isUserPermitted(String principal, String permission) {
		return true;
	}

	@Override
	public boolean isPermitted(String permission) {
		return true;
	}

	@Override
	public boolean hasRole(String role) {
		return true;
	}
	@Override
	public ISubject getCurrentSubject() {
		return new MockSubject();
	}


}
