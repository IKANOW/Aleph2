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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.shiro.authc.credential.CredentialsMatcher;
import org.apache.shiro.mgt.SecurityManager;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.multibindings.Multibinder;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.security.interfaces.IAuthProvider;
import com.ikanow.aleph2.security.interfaces.IRoleProvider;
import com.ikanow.aleph2.security.module.CoreSecurityModule;

public class MockSecurityService extends SecurityService implements ISecurityService {
	
	protected static Map<String, Set<String>> rolesMap = new HashMap<String, Set<String>>();
	protected static Map<String, Set<String>> permissionsMap = new HashMap<String, Set<String>>();
	protected static Map<String, AuthorizationBean> authMap = new HashMap<String, AuthorizationBean>();
	protected static MapRoleProvider roleProvider =  new MapRoleProvider(rolesMap, permissionsMap);
	protected static MapAuthProvider authProvider = new MapAuthProvider(authMap);
			
	static{
		System.setProperty(IKANOW_SYSTEM_LOGIN, "system");
		System.setProperty(IKANOW_SYSTEM_PASSWORD, "system123");

		AuthorizationBean ab1 = new AuthorizationBean("admin");
		ab1.setCredentials("admin123");
		authMap.put("admin",ab1);
		AuthorizationBean ab2 = new AuthorizationBean("user");
		ab2.setCredentials("user123");
		authMap.put("user",ab2);
		AuthorizationBean ab3 = new AuthorizationBean("testUser");
		ab3.setCredentials("testUser123");
		authMap.put("testUser",ab3);
		AuthorizationBean ab4 = new AuthorizationBean("system");
		ab4.setCredentials("system123");
		authMap.put("system",ab4);
		
		permissionsMap.put("admin", new HashSet<String>(Arrays.asList("*")));
		permissionsMap.put("user", new HashSet<String>(Arrays.asList("permission1","permission2","permission3","read:tmp:data:misc","package:*","permission:*","DataBucketBean:read:bucketId1","community:*:communityId1")));
		permissionsMap.put("testUser", new HashSet<String>(Arrays.asList("t1","t2","t3")));

		rolesMap.put("admin", new HashSet<String>(Arrays.asList("admin")));
		rolesMap.put("user", new HashSet<String>(Arrays.asList("user")));
		rolesMap.put("testUser", new HashSet<String>(Arrays.asList("testUser")));

	}

	@Inject
	public MockSecurityService(IServiceContext serviceContext, SecurityManager securityManager) {
		super(serviceContext, securityManager);		
	}
	
	public static List<Module> getExtraDependencyModules() {
		return Arrays.asList((Module)new CoreSecurityModule(){
				@Override
				protected void bindRoleProviders() {				
					//calling  super to increase junit coverage
					super.bindRoleProviders();
					Multibinder<IRoleProvider> uriBinder = Multibinder.newSetBinder(binder(), IRoleProvider.class);					
				    uriBinder.addBinding().toInstance(roleProvider);
				}
				
				@Override
				protected void bindCredentialsMatcher() {
					//calling super to increase junit coverage
					super.bindCredentialsMatcher();
			 		bind(CredentialsMatcher.class).to(NoCredentialsMatcher.class);
					
				}
				
				@Override
				protected void bindAuthProviders() {
					//calling super to increase junit coverage
					super.bindAuthProviders();

			 		bind(IAuthProvider.class).toInstance(authProvider);
				
				}
			});	
	}

	/**
	 * Placeholder method for derived security services.
	 * @return
	 */
	protected Injector getInjector(){
		Injector injector = Guice.createInjector(getExtraDependencyModules().get(0));
		return injector;
	}

}
