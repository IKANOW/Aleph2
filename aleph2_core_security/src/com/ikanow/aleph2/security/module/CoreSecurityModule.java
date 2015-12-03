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
package com.ikanow.aleph2.security.module;

import java.util.Collection;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.cache.CacheManager;
import org.apache.shiro.cache.ehcache.EhCacheManager;
import org.apache.shiro.config.ConfigurationException;
import org.apache.shiro.guice.ShiroModule;
import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.realm.Realm;

import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.binder.AnnotatedBindingBuilder;
import com.google.inject.util.Types;
import com.ikanow.aleph2.security.service.CoreRealm;

/** Core module for Security Service
 * @author Joern
 */
public class CoreSecurityModule extends ShiroModule {
	private static final Logger logger = LogManager.getLogger(CoreSecurityModule.class);

	static{
		// prevent ehcache from making calls
		System.setProperty("net.sf.ehcache.skipUpdateCheck","true");
	}
    protected void configureShiro() {
			bindCredentialsMatcher();
			bindAuthProviders();
    		bindRoleProviders();
        	bindRealms();
        	bindCacheManager();
        	bindMisc();
    }

    protected void bindCacheManager() {
    	
    	System.setProperty("ehcache.disk.store.dir", System.getProperty("java.io.tmpdir") +"/shiro-cache-" + System.getProperty("user.name"));
    	bind(CacheManager.class).toInstance(new EhCacheManager(){
    	    public String getCacheManagerConfigFile() {
    	        return "classpath:ehcache.xml";
    	    }
    	});		
    	expose(CacheManager.class);
	}

	/** 
     * Place holder to overwrite. 
     */
    protected void bindMisc() {
		// TODO Auto-generated method stub
		
	}

/*	@Provides
    Ini loadShiroIni() {
        return Ini.fromResourcePath("classpath:shiro.ini");
    }
  */  
    protected void bindRealms(){
        	bindRealm().to(CoreRealm.class).asEagerSingleton();         
     }

    /** 
     * Place holder to overwrite. 
     */
    protected void bindCredentialsMatcher(){
    	logger.debug("bindCredentialsMatcher -placeholder, override in sub-modules");
    }
    /** 
     * Place holder to overwrite. 
     */
    protected void bindAuthProviders(){
    	logger.debug("bindAuthProviders -placeholder, override in sub-modules");
    }

    /** 
     * Place holder to overwrite. 
     */
    protected void bindRoleProviders(){
    	logger.debug("bindRoleProviders - placeholder, override in sub-modules");
    }
    
    /**
     * Binds the security manager.  Override this method in order to provide your own security manager binding.
     * <p/>
     * By default, a {@link org.apache.shiro.mgt.DefaultSecurityManager} is bound as an eager singleton.
     *
     * @param bind
     */
    protected void bindSecurityManager(AnnotatedBindingBuilder<? super SecurityManager> bind) {
        try {
            bind.toConstructor(DefaultSecurityManager.class.getConstructor(Collection.class)).asEagerSingleton();
        } catch (NoSuchMethodException e) {
            throw new ConfigurationException("This really shouldn't happen.  Either something has changed in Shiro, or there's a bug in " + ShiroModule.class.getSimpleName(), e);
        }
    }

    // duplicates from Shiromodule. Why did they have to make them private there?
    @SuppressWarnings({"unchecked"})
    protected static Key<Set<Realm>> realmSetKey() {
        return (Key<Set<Realm>>) Key.get(TypeLiteral.get(Types.setOf(Realm.class)));
    }

    @SuppressWarnings({"unchecked"})
    public static Key<Collection<Realm>> realmCollectionKey() {
        return (Key<Collection<Realm>>) Key.get(Types.newParameterizedType(Collection.class, Realm.class));
    }

}
