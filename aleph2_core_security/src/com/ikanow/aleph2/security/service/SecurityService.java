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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.cache.CacheManager;
import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.realm.Realm;
import org.apache.shiro.session.Session;
import org.apache.shiro.session.mgt.DefaultSessionManager;
import org.apache.shiro.session.mgt.SessionManager;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.subject.SimplePrincipalCollection;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadContext;

import com.google.inject.Inject;
import com.google.inject.Module;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IExtraDependencyLoader;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISubject;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.security.interfaces.IClearableRealmCache;
import com.ikanow.aleph2.security.module.CoreSecurityModule;

public class SecurityService implements ISecurityService, IExtraDependencyLoader{

	protected ISubject currentSubject = null;
	private static final Logger logger = LogManager.getLogger(SecurityService.class);
	
	protected static String systemUsername = System.getProperty(IKANOW_SYSTEM_LOGIN, "4e3706c48d26852237078005");
	protected static String systemPassword = System.getProperty(IKANOW_SYSTEM_PASSWORD, "not allowed!");

	@Inject
	protected IServiceContext serviceContext;
	protected CacheManager cacheManager;
	Collection<Realm> realms = new HashSet<Realm>();


	@Inject
	public SecurityService(IServiceContext serviceContext, SecurityManager securityManager, CacheManager cacheManager) {
		this.serviceContext = serviceContext;
		SecurityUtils.setSecurityManager(securityManager);
		this.cacheManager = cacheManager;
		if(securityManager instanceof DefaultSecurityManager){
			SessionManager sessionManager = ((DefaultSecurityManager)securityManager).getSessionManager();
			this.realms = ((DefaultSecurityManager)securityManager).getRealms();
			logger.debug("Session manager:"+sessionManager);	
		}		

	}


	protected void init(){
		try {

	        // get the currently executing user:
	        Subject currentUser = SecurityUtils.getSubject();
	        this.currentSubject = new SubjectWrapper(currentUser);
	        // Do some stuff with a Session (no need for a web or EJB container!!!)
			
		} catch (Throwable e) {
			logger.error("Caught exception",e);
		}

	}
	
	@Override
	public Collection<Object> getUnderlyingArtefacts() {
		return Collections.emptyList();
	}

	@Override
	public <T> Optional<T> getUnderlyingPlatformDriver(Class<T> driver_class, Optional<String> driver_options) {
		return Optional.empty();
	}


	@Override
	public ISubject login(String principalName, Object credentials) {
		
		String password = (String)credentials;
        UsernamePasswordToken token = new UsernamePasswordToken(principalName,password);
        //token.setRememberMe(true);

        ensureUserIsLoggedOut();
        Subject shiroSubject = getShiroSubject();
        shiroSubject.login((AuthenticationToken)token);
        currentSubject = new SubjectWrapper(shiroSubject);
		return currentSubject;
	}

	protected void ensureUserIsLoggedOut()
	{
	    try
	    {
	    	Subject shiroSubject = getShiroSubject();
	        if (shiroSubject == null)
	            return;

	        // Log the user out and kill their session if possible.
	        shiroSubject.logout();
	        Session session = shiroSubject.getSession(false);
	        if (session == null)
	            return;

	        session.stop();
	    }
	    catch (Exception e)
	    {
	        // Ignore all errors, as we're trying to silently 
	        // log the user out.
	    }
	}

	// Clean way to get the subject
	protected Subject getShiroSubject()
	{
	    Subject currentUser = ThreadContext.getSubject();// SecurityUtils.getSubject();

	    if (currentUser == null)
	    {
	        currentUser = SecurityUtils.getSubject();
	    }

	    return currentUser;
	}
	

	@Override
	public boolean hasRole(ISubject subject, String roleIdentifier) {
		boolean ret = ((Subject)subject.getSubject()).hasRole(roleIdentifier);
		return ret;
	}

	

	public static List<Module> getExtraDependencyModules() {
		return Arrays.asList((Module)new CoreSecurityModule());
	}


	@Override
	public void youNeedToImplementTheStaticFunctionCalled_getExtraDependencyModules() {
		// TODO Auto-generated method stub
		
	}


	@Override
	public boolean isPermitted(ISubject subject, String permission) {
		boolean ret = ((Subject)subject.getSubject()).isPermitted(permission);
		return ret;
	}



	@Override
	public <O> IManagementCrudService<O> secured(IManagementCrudService<O> crud, AuthorizationBean authorizationBean) {		
		return new SecuredCrudManagementDbService<O>(serviceContext, crud, authorizationBean);
	}


	@Override
	public ISubject loginAsSystem() {
		ISubject subject = login(systemUsername,systemPassword);			
		return subject;
	}


	@Override
	public void runAs(ISubject subject,Collection<String> principals) {
		// TODO Auto-generated method stub
		
		((Subject)subject.getSubject()).runAs(new SimplePrincipalCollection(principals,getRealmName()));
	}


	@SuppressWarnings("unchecked")
	@Override
	public Collection<String> releaseRunAs(ISubject subject) {
		PrincipalCollection p = ((Subject)subject.getSubject()).releaseRunAs();	
		return p.asList();
	}



	protected String getRealmName(){
		String name = "SecurityService";
		if(realms.iterator().hasNext()){
			name = realms.iterator().next().getName();
		}
		return name;
	}

	public void invalidateAuthenticationCache(Collection<String> principalNames){
		for (Realm realm : realms) {
			if(realm instanceof IClearableRealmCache){
				IClearableRealmCache ar = (IClearableRealmCache)realm;
				ar.clearAuthorizationCached(principalNames);
		} 
			
		}
	}

	/**
	 * This function invalidates the whole cache
	 */
	public void invalidateCache(){
		for (Realm realm : realms) {
			if(realm instanceof IClearableRealmCache){
				IClearableRealmCache ar = (IClearableRealmCache)realm;
				ar.clearAllCaches();
		} 
			
		}
	}

	public void setSessionTimeout(long globalSessionTimeout){
		SecurityManager securityManager = SecurityUtils.getSecurityManager();
		if(securityManager instanceof DefaultSecurityManager){
			SessionManager sessionManager = ((DefaultSecurityManager)securityManager).getSessionManager();
			if(sessionManager instanceof DefaultSessionManager){
				((DefaultSessionManager)sessionManager).setGlobalSessionTimeout(globalSessionTimeout);
			}
			logger.debug("Session manager:"+sessionManager);	
		}
	}

	
}
