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
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IExtraDependencyLoader;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISubject;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.security.interfaces.IClearableRealmCache;
import com.ikanow.aleph2.security.module.CoreSecurityModule;

public class SecurityService implements ISecurityService, IExtraDependencyLoader{

	protected ThreadLocal<ISubject> tlCurrentSubject = new ThreadLocal<ISubject>();
	protected static final Logger logger = LogManager.getLogger(SecurityService.class);
	
	protected static String systemUsername = null;
	protected static String systemPassword = null;

	@Inject
	protected IServiceContext serviceContext;
	protected CacheManager cacheManager;
	Collection<Realm> realms = new HashSet<Realm>();

	protected JVMSecurityManager jvmSecurityManager;

	protected PermissionExtractor permissionExtractor = new PermissionExtractor();
	
	@Inject
	public SecurityService(IServiceContext serviceContext, SecurityManager securityManager) {
		this.serviceContext = serviceContext;
		SecurityUtils.setSecurityManager(securityManager);
		this.cacheManager = CoreEhCacheManager.getInstance().getCacheManager();
		if(securityManager instanceof DefaultSecurityManager){
			SessionManager sessionManager = ((DefaultSecurityManager)securityManager).getSessionManager();
			this.realms = ((DefaultSecurityManager)securityManager).getRealms();
			logger.debug("Session manager:"+sessionManager);	
		}		

		systemUsername = System.getProperty(IKANOW_SYSTEM_LOGIN, "4e3706c48d26852237078005");
		systemPassword = System.getProperty(IKANOW_SYSTEM_PASSWORD, "not allowed!");
	}



	@Override
	public Collection<Object> getUnderlyingArtefacts() {
		return Arrays.asList(new PermissionExtractor()); 
			//(not sure if "this" object is safe to use or if it will point to derived class, so just using random class that i know is from this JAR)
	}

	@Override
	public <T> Optional<T> getUnderlyingPlatformDriver(Class<T> driver_class, Optional<String> driver_options) {
		return Optional.empty();
	}

	/////////////////////////////////////////////////////////////////////////
	
	// NEW API
	
	@Override
	public ISubject getUserContext(String user_id) {
		ISubject root_subject = getUserContext(systemUsername, systemPassword);
		((Subject)(root_subject.getSubject())).runAs(new SimplePrincipalCollection(user_id,getRealmName()));
		return root_subject;
	}

	@Override
	public ISubject getUserContext(String user_id, String password) {
		final Subject new_subject = new Subject.Builder().buildSubject();
        final UsernamePasswordToken token = new UsernamePasswordToken(user_id,password);
        new_subject.login(token);
		return new SubjectWrapper(new_subject);
	}
	
	@Override
	public void invalidateUserContext(ISubject subject) {
		((Subject)(subject.getSubject())).logout();
	}
	
	@Override
	public ISubject getSystemUserContext() {
		return getUserContext(systemUsername, systemPassword);
	}
	
	@Override
	public boolean isUserPermitted(ISubject user_token, Object assetOrPermission, Optional<String> action) {
		boolean permitted = false;
		List<String> permissions = permissionExtractor.extractPermissionIdentifiers(assetOrPermission, action);
		if (permissions != null && permissions.size() > 0) {
			for (String permission : permissions) {
				permitted = isPermitted(user_token, permission);
				if (permitted) {
					break;
				}
			}
		}
		return permitted;
	}

	@Override
	public boolean hasUserRole(ISubject user_token, String role) {
		//TODO get rid of hasRole and replace with this
		return hasRole(user_token,role);
	}			
	
	@Override
	public boolean isPermitted(ISubject subject, String permission) {
		return ((Subject)subject.getSubject()).isPermitted(permission);
	}
	
	@Override
	public boolean isUserPermitted(Optional<String> userID, Object assetOrPermission, Optional<String> action) {
		ISubject subject = null;
		try {
			subject = userID.map(uid -> this.getUserContext(uid)).orElseGet(() -> this.getSystemUserContext());
			return isUserPermitted(subject, assetOrPermission, action);
		}
		finally {
			if (null != subject) this.invalidateUserContext(subject);
		}
	}

	@Override
	public boolean hasUserRole(Optional<String> userID, String role) {
		ISubject subject = null;
		try {
			subject = userID.map(uid -> this.getUserContext(uid)).orElseGet(() -> this.getSystemUserContext());
			return hasUserRole(subject, role);
		}
		finally {
			if (null != subject) this.invalidateUserContext(subject);
		}
	}
	
	/////////////////////////////////////////////////////////////////////////////
	
	@Override
	public ISubject login(String principalName, Object credentials) {
		
		
		String password = (String)credentials;
        UsernamePasswordToken token = new UsernamePasswordToken(principalName,password);
        
        //token.setRememberMe(true);

        ensureUserIsLoggedOut();
        Subject shiroSubject = getShiroSubject();
        shiroSubject.login((AuthenticationToken)token);
        tlCurrentSubject.remove();
        ISubject currentSubject = new SubjectWrapper(shiroSubject);
        tlCurrentSubject.set(currentSubject);
        if(jvmSecurityManager!=null){
        	jvmSecurityManager.setSubject(currentSubject);
        }
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
	public <O> IManagementCrudService<O> secured(IManagementCrudService<O> crud, AuthorizationBean authorizationBean) {		
		return new SecuredCrudManagementDbService<O>(serviceContext, crud, authorizationBean);
	}

	@Override
	public IDataServiceProvider secured(IDataServiceProvider provider,
			AuthorizationBean authorizationBean) {
		return new SecuredDataServiceProvider(serviceContext, provider, authorizationBean);
	}
	
	
	//TODO: ->protected
	@Override
	public ISubject loginAsSystem() {
		ISubject subject = login(systemUsername,systemPassword);		
		return subject;
	}


	//TODO: ->protected
	@Override
	public void runAs(ISubject subject,Collection<String> principals) {
		// TODO Auto-generated method stub	
		((Subject)subject.getSubject()).runAs(new SimplePrincipalCollection(principals,getRealmName()));
        if(jvmSecurityManager!=null){
        	jvmSecurityManager.setSubject(subject);
        }

	}


	//TODO: ->protected
	@SuppressWarnings("unchecked")
	@Override
	public Collection<String> releaseRunAs(ISubject subject) {
		PrincipalCollection p = ((Subject)subject.getSubject()).releaseRunAs();
		return (p!=null)? p.asList(): null;
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


	@Override
	public void enableJvmSecurityManager(boolean enabled) {
		if (enabled) {			
			if (jvmSecurityManager == null) {
				Object currSysManager = System.getSecurityManager();
				if (currSysManager instanceof JVMSecurityManager) {
					this.jvmSecurityManager = (JVMSecurityManager) currSysManager;
				} else {
					this.jvmSecurityManager = new JVMSecurityManager(this);
					this.jvmSecurityManager.setSubject(tlCurrentSubject.get());
					System.setSecurityManager(jvmSecurityManager);
				}				
			}

		} else {
			// disable security manager if it is our's
			Object currSysManager = System.getSecurityManager();
			if (currSysManager instanceof JVMSecurityManager) {				
				System.setSecurityManager(null);
				this.jvmSecurityManager.releaseSubject();				
				this.jvmSecurityManager = null;
			}
		}
	}


	@Override
	public void enableJvmSecurity(boolean enabled) {
		if(enabled){
			enableJvmSecurityManager(true);
			jvmSecurityManager.setEnabled(true);
		}else{
			if (jvmSecurityManager != null) {
				jvmSecurityManager.setEnabled(false);
			} 
		}
	}


	
}
