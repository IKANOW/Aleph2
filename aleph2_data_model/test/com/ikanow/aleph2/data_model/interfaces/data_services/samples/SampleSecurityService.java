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
package com.ikanow.aleph2.data_model.interfaces.data_services.samples;

import java.util.Collection;
import java.util.Optional;

import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISubject;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;

public class SampleSecurityService implements ISecurityService {
	
	@Override
	public Collection<Object> getUnderlyingArtefacts() {
		return null;
	}

	@Override
	public <T> Optional<T> getUnderlyingPlatformDriver(Class<T> driver_class,
			Optional<String> driver_options) {
		return null;
	}

	@Override
	public ISubject login(String principalName, Object credentials) {
		return null;		
	}

	@Override
	public boolean hasRole(ISubject subject, String role) {
		return false;
	}

	@Override
	public boolean isPermitted(ISubject subject, String string) {
		return false;
	}

	@Override
	public void runAs(ISubject subject, Collection<String> principals) {
	}

	@Override
	public Collection<String> releaseRunAs(ISubject subject) {
		return null;
	}


	@Override
	public <O> IManagementCrudService<O> secured(IManagementCrudService<O> crud, AuthorizationBean authorizationBean) {
		return null;
	}

	@Override
	public IDataServiceProvider secured(IDataServiceProvider provider,
			AuthorizationBean authorizationBean) {
		return null;
	}
	
	@Override
	public ISubject loginAsSystem() {
		return null;
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
}
