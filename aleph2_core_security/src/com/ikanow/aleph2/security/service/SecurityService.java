package com.ikanow.aleph2.security.service;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.subject.Subject;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISubject;
import com.ikanow.aleph2.data_model.interfaces.shared_services.Identity;
import com.ikanow.aleph2.security.module.CoreSecurityModule;

public class SecurityService implements ISecurityService {

	protected ISubject currentSubject = null;
	private static final Logger logger = LogManager.getLogger(SecurityService.class);
	
	public SecurityService() {
		try {
			Injector injector = Guice.createInjector(new CoreSecurityModule());
		    SecurityManager securityManager = injector.getInstance(SecurityManager.class);
//		    SecurityUtils.setSecurityManager(securityManager);Factory<SecurityManager> factory = new IniSecurityManagerFactory("classpath:shiro.ini");
//	        SecurityManager securityManager = factory.getInstance();
	        SecurityUtils.setSecurityManager(securityManager);


	        // get the currently executing user:
	        Subject currentUser = SecurityUtils.getSubject();
	        currentSubject = new SubjectWrapper(currentUser);
	        // Do some stuff with a Session (no need for a web or EJB container!!!)
			
		} catch (Throwable e) {
			logger.error("Caught exception",e);
		}
	}
	
	@Override
	public Collection<Object> getUnderlyingArtefacts() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> Optional<T> getUnderlyingPlatformDriver(Class<T> driver_class, Optional<String> driver_options) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean hasPermission(Identity identity, Class<?> resourceClass, String resourceIdentifier, String operation) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean hasPermission(Identity identity, String resourceName, String resourceIdentifier, String operation) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Identity getIdentity(Map<String, Object> token) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void grantPermission(Identity identity, Class<?> resourceClass, String resourceIdentifier, String operation) {
		// TODO Auto-generated method stub

	}

	@Override
	public void grantPermission(Identity identity, String resourceName, String resourceIdentifier, String operation) {
		// TODO Auto-generated method stub

	}

	@Override
	public void revokePermission(Identity identity, Class<?> resourceClass, String resourceIdentifier, String operation) {
		// TODO Auto-generated method stub

	}

	@Override
	public void revokePermission(Identity identity, String resourceName, String resourceIdentifier, String operation) {
		// TODO Auto-generated method stub

	}

	@Override
	public void clearPermission(Class<?> resourceClass, String resourceIdentifier) {
		// TODO Auto-generated method stub

	}

	@Override
	public void clearPermission(String resourceName, String resourceIdentifier) {
		// TODO Auto-generated method stub

	}

	@Override
	public ISubject getSubject() {
		return currentSubject;
	}

	@Override
	public void login(ISubject subject, Object token) {
		((Subject)currentSubject.getSubject()).login((AuthenticationToken)token);
		
	}

	@Override
	public boolean hasRole(ISubject subject, String roleIdentifier) {
		boolean ret = ((Subject)currentSubject.getSubject()).hasRole(roleIdentifier);
		return ret;
	}

}
