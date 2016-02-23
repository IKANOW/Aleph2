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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.shiro.SecurityUtils;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.authc.credential.CredentialsMatcher;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.session.Session;
import org.apache.shiro.session.SessionException;
import org.apache.shiro.subject.SimplePrincipalCollection;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.subject.support.DelegatingSubject;

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
import org.apache.shiro.util.ThreadContext;

public class MockSecurityService extends SecurityService implements ISecurityService {
	
	protected static Map<String, Set<String>> rolesMap = new HashMap<String, Set<String>>();
	protected static Map<String, Set<String>> permissionsMap = new HashMap<String, Set<String>>();
	protected static Map<String, AuthorizationBean> authMap = new HashMap<String, AuthorizationBean>();
	protected static MockRoleProvider roleProvider =  new MockRoleProvider(rolesMap, permissionsMap);
	protected static MockAuthProvider authProvider = new MockAuthProvider(authMap);
			
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

//// new set of threading tests
	public synchronized Subject loginAsSystem2(){
		Subject currentUser = SecurityUtils.getSubject();
		String principalName = systemUsername;
		String password = systemPassword;
		boolean needsLogin = true;
		try{
		    Session session = currentUser.getSession();
		    logger.debug("loginAsSystem2 : "+session.getId()+" : "+currentUser);

		if(currentUser.isAuthenticated()){
			while(currentUser.isRunAs()){
				currentUser.releaseRunAs();
			}
			Object principal = currentUser.getPrincipal();
			// check if currentPrincipal 
			if(systemUsername.equals(""+principal)){
				needsLogin=false;
			}else{
				logger.warn("Found authenticated user ("+principal+") different than system user, logging out this user.");
				currentUser.logout();
			}
		}
		}catch(Exception e){
			// try to get rid of expired session so system can login again
			logger.debug("Caught "+e.getClass().getName()+": "+ e.getMessage());
			// create new session
			ThreadContext.unbindSubject();
			currentUser = SecurityUtils.getSubject();			
			needsLogin = true;
		}
		if(needsLogin){
			UsernamePasswordToken token = new UsernamePasswordToken(principalName,password);
		    currentUser.login((AuthenticationToken)token);
		    Session session = currentUser.getSession(true);
		    logger.debug("Logged in user and Created session:"+session.getId());
		}

		return currentUser;
	}
	
	
	public synchronized void runAs2(String principal) {
		Subject currentUser = loginAsSystem2();		
		currentUser.runAs(new SimplePrincipalCollection(Arrays.asList(principal),getRealmName()));
	}

	public synchronized boolean isPermitted2(String permission) {
		Subject currentUser = SecurityUtils.getSubject();		
		return currentUser.isPermitted(permission);
	}

	public synchronized boolean hasRole2(String role) {
		Subject currentUser = SecurityUtils.getSubject();		
		return currentUser.hasRole(role);
	}

	public synchronized boolean isUserPermitted2(String principal, String permission) {
		runAs2(principal);
		Subject currentUser = SecurityUtils.getSubject();		
		return currentUser.isPermitted(permission);
	}

	public synchronized  boolean hasUserRole2(String principal, String role) {
		runAs2(principal);
		Subject currentUser = SecurityUtils.getSubject();		
		return currentUser.hasRole(role);
	}
}
