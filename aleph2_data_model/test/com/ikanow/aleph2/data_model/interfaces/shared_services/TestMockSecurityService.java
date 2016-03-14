<<<<<<< HEAD
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
import java.util.Collections;
import java.util.Optional;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import scala.Tuple2;

import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;

public class TestMockSecurityService {

	@SuppressWarnings("unchecked")
	@Test
	public void test_MockSecurityService() {
		
		// (basically all just coverage)
		
		MockSecurityService.MockSubject subject = new MockSecurityService.MockSubject();
		Assert.assertEquals(null, subject.getName());
		Assert.assertEquals(subject, subject.getSubject());
		Assert.assertEquals(true, subject.isAuthenticated());
		
		MockSecurityService service = new MockSecurityService();
		service.enableJvmSecurity(true);
		service.enableJvmSecurity(false);
		service.enableJvmSecurityManager(true);
		service.enableJvmSecurityManager(false);
		Assert.assertEquals(Collections.emptyList(), service.getUnderlyingArtefacts());
		Assert.assertEquals(Optional.empty(), service.getUnderlyingPlatformDriver(String.class, Optional.empty()));
		Assert.assertEquals(false, service.hasRole(subject, "role"));
		Assert.assertEquals(false, service.hasUserRole(Optional.empty(), "role"));
		service.invalidateAuthenticationCache(Arrays.asList(""));
		service.invalidateCache();
		Assert.assertEquals(false, service.isPermitted(subject, "action"));
		Assert.assertEquals(false, service.isUserPermitted(Optional.empty(), "test", Optional.empty()));
		Assert.assertEquals(MockSecurityService.MockSubject.class, service.login("", "").getClass());
		Assert.assertEquals(MockSecurityService.MockSubject.class, service.loginAsSystem().getClass());
		Assert.assertEquals(Arrays.asList(), service.releaseRunAs(subject));
		service.runAs(subject, Arrays.asList());
		
		@SuppressWarnings({ "rawtypes" })
		final IManagementCrudService test_crud = Mockito.mock(IManagementCrudService.class);
		Assert.assertEquals(test_crud, service.secured(test_crud, new AuthorizationBean("user")));
		final IDataServiceProvider test_data = Mockito.mock(IDataServiceProvider.class);
		Assert.assertEquals(test_data, service.secured(test_data,  new AuthorizationBean("user")));
		
		// OK check a couple of simple uses
		
		service.setGlobalMockRole("mock_test", true);
		Assert.assertEquals(true, service.hasUserRole(Optional.empty(), "mock_test"));
		
		service.setUserMockRole("mock_test_user", "mock_test_asset", "mock_test_action", true);
		Assert.assertEquals(true, service.isUserPermitted(Optional.of("mock_test_user"), "mock_test_asset", Optional.of("mock_test_action")));
		
		final DataBucketBean bucket = BeanTemplateUtils.build(DataBucketBean.class).with(DataBucketBean::full_name, "mock_test_asset").done().get();
		final SharedLibraryBean library_bean  = BeanTemplateUtils.build(SharedLibraryBean.class).with(SharedLibraryBean::path_name, "mock_test_asset").done().get();
		final Tuple2<String, String> t2 = Tuples._2T("", "mock_test_asset");
		
		Assert.assertEquals(true, service.isUserPermitted(Optional.of("mock_test_user"), bucket, Optional.of("mock_test_action")));
		Assert.assertEquals(true, service.isUserPermitted(Optional.of("mock_test_user"), library_bean, Optional.of("mock_test_action")));
		Assert.assertEquals(true, service.isUserPermitted(Optional.of("mock_test_user"), t2, Optional.of("mock_test_action")));
	}
}
=======
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
import java.util.Collections;
import java.util.Optional;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import scala.Tuple2;

import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;

public class TestMockSecurityService {

	@SuppressWarnings("unchecked")
	@Test
	public void test_MockSecurityService() {
		
		// (basically all just coverage)
		
		MockSecurityService.MockSubject subject = new MockSecurityService.MockSubject();
		Assert.assertEquals(null, subject.getName());
		Assert.assertEquals(subject, subject.getSubject());
		Assert.assertEquals(true, subject.isAuthenticated());
		
		MockSecurityService service = new MockSecurityService();
		service.enableJvmSecurity(Optional.empty(),true);
		service.enableJvmSecurity(Optional.empty(),false);
		service.enableJvmSecurityManager(true);
		service.enableJvmSecurityManager(false);
		Assert.assertEquals(Collections.emptyList(), service.getUnderlyingArtefacts());
		Assert.assertEquals(Optional.empty(), service.getUnderlyingPlatformDriver(String.class, Optional.empty()));
		Assert.assertEquals(false, service.hasUserRole("user", "role"));
		Assert.assertEquals(false, service.hasUserRole("user", "role"));
		service.invalidateAuthenticationCache(Arrays.asList(""));
		service.invalidateCache();
		Assert.assertEquals(false, service.isUserPermitted("user", "action"));
		Assert.assertEquals(false, service.isUserPermitted("user", "test", Optional.empty()));
		Assert.assertEquals(MockSecurityService.MockSubject.class, service.login("", "").getClass());
		
		@SuppressWarnings({ "rawtypes" })
		final IManagementCrudService test_crud = Mockito.mock(IManagementCrudService.class);
		Assert.assertEquals(test_crud, service.secured(test_crud, new AuthorizationBean("user")));
		final IDataServiceProvider test_data = Mockito.mock(IDataServiceProvider.class);
		Assert.assertEquals(test_data, service.secured(test_data,  new AuthorizationBean("user")));
		
		// OK check a couple of simple uses
		
		service.setGlobalMockRole("mock_test", true);
		Assert.assertEquals(true, service.hasUserRole("user", "mock_test"));
		
		service.setUserMockRole("mock_test_user", "mock_test_asset", "mock_test_action", true);
		Assert.assertEquals(true, service.isUserPermitted("mock_test_user", "mock_test_asset", Optional.of("mock_test_action")));
		
		final DataBucketBean bucket = BeanTemplateUtils.build(DataBucketBean.class).with(DataBucketBean::full_name, "mock_test_asset").done().get();
		final SharedLibraryBean library_bean  = BeanTemplateUtils.build(SharedLibraryBean.class).with(SharedLibraryBean::path_name, "mock_test_asset").done().get();
		final Tuple2<String, String> t2 = Tuples._2T("", "mock_test_asset");
		
		Assert.assertEquals(true, service.isUserPermitted("mock_test_user", bucket, Optional.of("mock_test_action")));
		Assert.assertEquals(true, service.isUserPermitted("mock_test_user", library_bean, Optional.of("mock_test_action")));
		Assert.assertEquals(true, service.isUserPermitted("mock_test_user", t2, Optional.of("mock_test_action")));
	}
}
>>>>>>> refs/heads/master
