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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;

/** A useful mock object for unit testing. Allows everything
 *  TODO (ALEPH-31): provide an interface to add overrides for testing
 * @author acp
 *
 */
public class MockSecurityService implements ISecurityService {

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
		
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService#getUnderlyingArtefacts()
	 */
	@Override
	public Collection<Object> getUnderlyingArtefacts() {
		return Collections.emptyList();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService#getUnderlyingPlatformDriver(java.lang.Class, java.util.Optional)
	 */
	@Override
	public <T> Optional<T> getUnderlyingPlatformDriver(Class<T> driver_class,
			Optional<String> driver_options) {
		return Optional.empty();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService#login(java.lang.String, java.lang.Object)
	 */
	@Override
	public ISubject login(String principalName, Object credentials) {
		return new MockSubject();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService#hasRole(com.ikanow.aleph2.data_model.interfaces.shared_services.ISubject, java.lang.String)
	 */
	@Override
	public boolean hasRole(ISubject subject, String role) {		
		return _mock_role_map.getOrDefault(role, false);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService#isPermitted(com.ikanow.aleph2.data_model.interfaces.shared_services.ISubject, java.lang.String)
	 */
	@Override
	public boolean isPermitted(ISubject subject, String string) {
		return _mock_role_map.getOrDefault(string, false);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService#runAs(com.ikanow.aleph2.data_model.interfaces.shared_services.ISubject, java.util.Collection)
	 */
	@Override
	public void runAs(ISubject subject, Collection<String> principals) {
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService#releaseRunAs(com.ikanow.aleph2.data_model.interfaces.shared_services.ISubject)
	 */
	@Override
	public Collection<String> releaseRunAs(ISubject subject) {
		return Collections.emptyList();
	}


	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService#secured(com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService, com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean)
	 */
	@Override
	public <O> IManagementCrudService<O> secured(IManagementCrudService<O> crud, AuthorizationBean authorizationBean) {
		return crud;
	}

	//////////////////////////
	
	// Some override code we'll add to as needed
	
	protected Map<String, Boolean> _mock_role_map = new HashMap<>();

	/** Set a mock role
	 * @param role
	 * @param permission
	 */
	public void setGlobalMockRole(String role, boolean permission) {
		_mock_role_map.put(role, permission);
	}

	@Override
	public ISubject loginAsSystem() {
		return new MockSubject();
	}

	@Override
	public void invalidateAuthenticationCache(Collection<String> principalNames) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void invalidateCache() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void enableJvmSecurityManager(boolean enabled) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void enableJvmSecurity(boolean enabled) {
		// TODO Auto-generated method stub
		
	}
	
}
