package com.ikanow.aleph2.security.service;

import java.util.*;

import org.apache.shiro.authc.credential.CredentialsMatcher;
import org.apache.shiro.cache.CacheManager;
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
	
	static{
		
		AuthorizationBean ab1 = new AuthorizationBean("admin");
		ab1.setCredentials("admin123");
		authMap.put("admin",ab1);
		AuthorizationBean ab2 = new AuthorizationBean("user");
		ab2.setCredentials("user123");
		authMap.put("user",ab2);
		AuthorizationBean ab3 = new AuthorizationBean("testUser");
		ab3.setCredentials("testUser123");
		authMap.put("testUser",ab3);
		
		permissionsMap.put("admin", new HashSet<String>(Arrays.asList("*")));
		permissionsMap.put("user", new HashSet<String>(Arrays.asList("permission1","permission2","permission3")));
		permissionsMap.put("testUser", new HashSet<String>(Arrays.asList("t1","t2","t3")));

		rolesMap.put("admin", new HashSet<String>(Arrays.asList("admin")));
		rolesMap.put("user", new HashSet<String>(Arrays.asList("user")));
		rolesMap.put("testUser", new HashSet<String>(Arrays.asList("testUser")));

	}

	@Inject
	public MockSecurityService(IServiceContext serviceContext, SecurityManager securityManager, CacheManager cacheManager) {
		super(serviceContext, securityManager, cacheManager);		
	}
	
	public static List<Module> getExtraDependencyModules() {
		return Arrays.asList((Module)new CoreSecurityModule(){
				@Override
				protected void bindRoleProviders() {				
					Multibinder<IRoleProvider> uriBinder = Multibinder.newSetBinder(binder(), IRoleProvider.class);
					
					MockRoleProvider mrp = new MockRoleProvider(rolesMap, permissionsMap);
				    uriBinder.addBinding().toInstance(mrp);
				}
				
				@Override
				protected void bindCredentialsMatcher() {
			 		bind(CredentialsMatcher.class).to(NoCredentialsMatcher.class);
					
				}
				
				@Override
				protected void bindAuthProviders() {
					MockAuthProvider authProvider = new MockAuthProvider(authMap);
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
