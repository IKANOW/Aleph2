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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import scala.Tuple2;

import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.Patterns;

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

		@Override
		public String getName() {
			return null;
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
		return _mock_role_map.get().getOrDefault(role, false);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService#isPermitted(com.ikanow.aleph2.data_model.interfaces.shared_services.ISubject, java.lang.String)
	 */
	@Override
	public boolean isPermitted(ISubject subject, String string) {
		return _mock_role_map.get().getOrDefault(string, false);
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

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService#secured(com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider, com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean)
	 */
	@Override
	public IDataServiceProvider secured(IDataServiceProvider provider, AuthorizationBean authorizationBean) {
		return provider;
	}	
	
	//////////////////////////
	
	// Some override code we'll add to as needed
	
	protected Map<String, Boolean> _test_role_map = new HashMap<>();
	
	protected ThreadLocal<Map<String, Boolean>> _mock_role_map = new ThreadLocal<Map<String, Boolean>>() {

		/* (non-Javadoc)
		 * @see java.lang.ThreadLocal#initialValue()
		 */
		@Override
		protected Map<String, Boolean> initialValue() {
			return new HashMap<>();
		}
		
	};

	/** Set a mock role (completely generic - for use when testing "isPermitted")
	 * @param role
	 * @param permission
	 */
	public void setGlobalMockRole(String role, boolean permission) {
		_test_role_map.put(role, permission);
	}

	/** More faithful approximation of user perms (for use when testing "isUserPermitted")
	 * @param user_id
	 * @param asset
	 * @param role
	 * @param permission
	 */
	public void setUserMockRole(String user_id, String asset, String action, boolean permission) {
		_test_role_map.put(user_id + ":" + asset + ":" + action, permission);
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService#loginAsSystem()
	 */
	@Override
	public ISubject loginAsSystem() {
		// (not quite right because this should only be written in when run as user, but in most cases will be using higher level code that
		//  does this for me)
		_mock_role_map.set(Collections.unmodifiableMap(_test_role_map));
		return new MockSubject();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService#invalidateAuthenticationCache(java.util.Collection)
	 */
	@Override
	public void invalidateAuthenticationCache(Collection<String> principalNames) {
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService#invalidateCache()
	 */
	@Override
	public void invalidateCache() {
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService#enableJvmSecurityManager(boolean)
	 */
	@Override
	public void enableJvmSecurityManager(boolean enabled) {
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService#enableJvmSecurity(boolean)
	 */
	@Override
	public void enableJvmSecurity(boolean enabled) {
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService#isUserPermitted(java.util.Optional, java.lang.Object, java.util.Optional)
	 */
	@Override
	public boolean isUserPermitted(Optional<String> userID, Object assetOrPermission,
			Optional<String> oAction) {
		
		loginAsSystem();
		
		return Patterns.match(assetOrPermission).<List<String>>andReturn()
			.when(DataBucketBean.class, b -> Arrays.asList(b._id(), b.full_name()))
			.when(SharedLibraryBean.class, s -> Arrays.asList(s._id(), s.path_name()))
			.when(Tuple2.class, t2 -> Arrays.asList(t2._2().toString()))
			.otherwise(s -> Arrays.asList(s.toString()))
			.stream()
			.anyMatch(to_check -> {
				final String test_string = userID.orElse("*") + ":" + to_check + ":" + oAction.orElse("*");
				return _mock_role_map.get().getOrDefault(test_string, false);
			})
			;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService#hasUserRole(java.util.Optional, java.lang.String)
	 */
	@Override
	public boolean hasUserRole(Optional<String> userID, String role) {
		
		loginAsSystem();
		
		return _mock_role_map.get().getOrDefault(role, false);
	}

}
