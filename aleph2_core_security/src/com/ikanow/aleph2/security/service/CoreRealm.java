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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.authc.AccountException;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.authc.credential.CredentialsMatcher;
import org.apache.shiro.authz.AuthorizationException;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authz.SimpleAuthorizationInfo;
import org.apache.shiro.cache.Cache;
import org.apache.shiro.realm.AuthorizingRealm;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.subject.SimplePrincipalCollection;

import scala.Tuple2;

import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.security.interfaces.IAuthProvider;
import com.ikanow.aleph2.security.interfaces.IClearableRealmCache;
import com.ikanow.aleph2.security.interfaces.IRoleProvider;


public class CoreRealm extends AuthorizingRealm implements IClearableRealmCache {
	private static final Logger logger = LogManager.getLogger(CoreRealm.class);

	protected Map<String,List<String>> roles = new HashMap<String,List<String>>();
	protected Map<String,List<String>> permissions = new HashMap<String,List<String>>();
	protected static Map<String,String> userNamePasswords = new HashMap<String,String>();
		
	static {
		userNamePasswords.put("admin", "admin123");
		userNamePasswords.put("user", "user123");
		userNamePasswords.put("system", "system123");
		userNamePasswords.put("test", "test123");
    }

	protected final IServiceContext _context;

	protected Set<IRoleProvider> roleProviders;
	protected IAuthProvider authProvider;
	
	@Inject
	public CoreRealm(final IServiceContext service_context, CredentialsMatcher matcher, IAuthProvider authProvider,Set<IRoleProvider> roleProviders) {		
		super(CoreEhCacheManager.getInstance().getCacheManager(),matcher);
		_context = service_context;
		this.authProvider = authProvider;
		this.roleProviders = roleProviders;
		logger.debug("Realm name="+getName());
	}
	

	 
    /**
     * This implementation of the interface expects the principals collection to return a String username keyed off of
     * this realm's {@link #getName() name}
     *
     * @see #getAuthorizationInfo(org.apache.shiro.subject.PrincipalCollection)
     */
    @Override
    protected AuthorizationInfo doGetAuthorizationInfo(PrincipalCollection principals) {

        //null usernames are invalid
        if (principals == null) {
            throw new AuthorizationException("PrincipalCollection method argument cannot be null.");
        }

        String username = (String) getAvailablePrincipal(principals);
        Set<String> roles = new HashSet<String>();
        Set<String> permissions = new HashSet<String>();
        
        for (IRoleProvider roleProvider : roleProviders) {
        	Tuple2<Set<String>, Set<String>> t2 = roleProvider.getRolesAndPermissions(username);
        	roles.addAll(t2._1());
            permissions.addAll(t2._2());        
        }  // for      

        SimpleAuthorizationInfo info = new SimpleAuthorizationInfo(roles);
        info.addStringPermissions(permissions);        
        return info;

    }


    protected AuthenticationInfo doGetAuthenticationInfo(AuthenticationToken token) throws AuthenticationException {

        UsernamePasswordToken upToken = (UsernamePasswordToken) token;
        String principalName = upToken.getUsername();

        // Null username is invalid
        if (principalName == null) {
            throw new AccountException("Null usernames are not allowed by this realm.");
        }

        AuthenticationInfo info = null;
        try {
        
        	AuthorizationBean b = authProvider.getAuthBean(principalName);
        	info = new CoreAuthenticationInfo(b, getName());
        
        } catch (Exception e) {
            final String message = "There was an error while authenticating user [" + principalName + "]";
            logger.error(message,e);

            // Rethrow any errors as an authentication exception
            throw new AuthenticationException(message, e);
        } finally {
            // TODO close connection?
        }

        return info;
    }
    
    @Override
    public void clearAuthorizationCached(Collection<String> principalNames){
   	 logger.debug("clearCachedAuthorizationInfo for "+principalNames);
   	 SimplePrincipalCollection principals = new SimplePrincipalCollection(principalNames, this.getClass().getName());
   	 super.doClearCache(principals);   	 
    }

    @Override
    public void clearAllCaches(){
		 logger.debug("clearAllCaches");
			

		 Cache<Object, AuthenticationInfo> ac = getAuthenticationCache();
			if(ac!=null){
				ac.clear();
			}
			Cache<Object, AuthorizationInfo> ar = getAuthorizationCache();
			if(ar!=null){
				ar.clear();
			}
		
    }
     
}
