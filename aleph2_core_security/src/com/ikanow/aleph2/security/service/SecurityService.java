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
import java.util.List;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.subject.Subject;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IExtraDependencyLoader;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISubject;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.security.module.CoreSecurityModule;

public class SecurityService implements ISecurityService, IExtraDependencyLoader{

	protected ISubject currentSubject = null;
	private static final Logger logger = LogManager.getLogger(SecurityService.class);
	@Inject
	protected IServiceContext serviceContext;

	@Inject
	public SecurityService(IServiceContext serviceContext, SecurityManager securityManager) {
		this.serviceContext = serviceContext;
		SecurityUtils.setSecurityManager(securityManager);
		}


	protected void init(){
		try {
//		    SecurityManager securityManager = getInjector().getInstance(SecurityManager.class);
//		    SecurityUtils.setSecurityManager(securityManager);Factory<SecurityManager> factory = new IniSecurityManagerFactory("classpath:shiro.ini");    
//	        SecurityManager securityManager = factory.getInstance();
//	        SecurityUtils.setSecurityManager(securityManager);


	        // get the currently executing user:
	        Subject currentUser = SecurityUtils.getSubject();
	        this.currentSubject = new SubjectWrapper(currentUser);
	        // Do some stuff with a Session (no need for a web or EJB container!!!)
			
		} catch (Throwable e) {
			logger.error("Caught exception",e);
		}

	}
	/**
	 * Placeholder metgod for derived security services.
	 * @return
	 */
	protected Injector getInjector(){
		Injector injector = Guice.createInjector(new CoreSecurityModule());
		return injector;
	}
	
	@Override
	public Collection<Object> getUnderlyingArtefacts() {
		return Collections.emptyList();
	}

	@Override
	public <T> Optional<T> getUnderlyingPlatformDriver(Class<T> driver_class, Optional<String> driver_options) {
		return Optional.empty();
	}


	public ISubject getSubject() {
		if(currentSubject == null){
			init();
		}
		return currentSubject;
	}

	@Override
	public ISubject login(String principalName, Object credentials) {
		
		String password = (String)credentials;
        UsernamePasswordToken token = new UsernamePasswordToken(principalName,password);

        ISubject subject = getSubject(); 
		((Subject)subject.getSubject()).login((AuthenticationToken)token);
		return subject;
	}

	@Override
	public boolean hasRole(ISubject subject, String roleIdentifier) {
		boolean ret = ((Subject)getSubject().getSubject()).hasRole(roleIdentifier);
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
	public boolean isPermitted(ISubject subject, String string) {
		// TODO Auto-generated method stub
		return false;
	}


	@Override
	public void runAs(ISubject subject, Collection<String> principals) {
		// TODO Auto-generated method stub
		
	}


	@Override
	public Collection<String> releaseRunAs(ISubject subject) {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public <O> IManagementCrudService<O> secured(IManagementCrudService<O> crud, AuthorizationBean authorizationBean) {
		
		return null;
	}




}
