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

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;

public class TestNoSecurityService {

	@SuppressWarnings("unchecked")
	@Test
	public void test_noSecurityService() {
		
		// (basically all just coverage)
		
		NoSecurityService.MockSubject subject = new NoSecurityService.MockSubject();
		Assert.assertEquals(null, subject.getName());
		Assert.assertEquals(subject, subject.getSubject());
		Assert.assertEquals(true, subject.isAuthenticated());
		
		NoSecurityService service = new NoSecurityService();
		service.enableJvmSecurity(Optional.empty(),true);
		service.enableJvmSecurity(Optional.empty(),false);
		service.enableJvmSecurityManager(Optional.empty(),true);
		service.enableJvmSecurityManager(Optional.empty(),false);
		Assert.assertEquals((Collection<Object>)Arrays.<Object>asList(service), service.getUnderlyingArtefacts());
		Assert.assertEquals(Optional.empty(), service.getUnderlyingPlatformDriver(String.class, Optional.empty()));
		Assert.assertEquals(true, service.hasUserRole("user", "role"));
		Assert.assertEquals(true, service.hasUserRole("user", "role"));
		service.invalidateAuthenticationCache(Arrays.asList(""));
		service.invalidateCache();
		Assert.assertEquals(true, service.isUserPermitted("user", "action"));
		Assert.assertEquals(true, service.isUserPermitted("user", "test", Optional.empty()));
		Assert.assertEquals(NoSecurityService.MockSubject.class, service.login("", "").getClass());
		
		@SuppressWarnings({ "rawtypes" })
		final IManagementCrudService test_crud = Mockito.mock(IManagementCrudService.class);
		Assert.assertEquals(test_crud, service.secured(test_crud, new AuthorizationBean("user")));
		final IDataServiceProvider test_data = Mockito.mock(IDataServiceProvider.class);
		Assert.assertEquals(test_data, service.secured(test_data,  new AuthorizationBean("user")));
	}
}
