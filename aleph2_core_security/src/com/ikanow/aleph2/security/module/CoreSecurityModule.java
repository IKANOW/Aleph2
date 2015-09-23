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

import org.apache.shiro.cache.CacheManager;
import org.apache.shiro.cache.ehcache.EhCacheManager;
import org.apache.shiro.config.ConfigurationException;
import org.apache.shiro.config.Ini;
import org.apache.shiro.guice.ShiroModule;
import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.realm.text.IniRealm;

import com.google.inject.Provides;
import com.google.inject.binder.AnnotatedBindingBuilder;

/** Core module for Security Service
 * @author Joern
 */
public class CoreSecurityModule extends ShiroModule {
    protected void configureShiro() {
        	bindRealms();
        	bind(CacheManager.class).to(EhCacheManager.class).asEagerSingleton();
    }

    @Provides
    Ini loadShiroIni() {
        return Ini.fromResourcePath("classpath:shiro.ini");
    }
    
    /**
     * Binds the session manager.  Override this method in order to provide your own session manager binding
     * used for testing timeouts because ini settings do not work for this.
     * <p/>
     * By default, a {@link org.apache.shiro.session.mgt.DefaultSessionManager} is bound as an eager singleton.
     *
     * @param bind
     */
    
/*    protected void bindSessionManager(AnnotatedBindingBuilder<SessionManager> bind) {
    	DefaultSessionManager sessionManager = new DefaultSessionManager();
//    	EnterpriseCacheSessionDAO sessionDao = new EnterpriseCacheSessionDAO();
    	EhCacheManager cacheManager =  new EhCacheManager(); 
    	bind(CacheManager.class).toInstance(cacheManager);
  //  	sessionDao.setCacheManager(cacheManager);
 //   	sessionManager.setSessionDAO(sessionDao);    	
    	//sessionManager.setGlobalSessionTimeout(2000);
        bind.toInstance(sessionManager);
    }

    */
    protected void bindRealms(){
       try {
        bindRealm().toConstructor(IniRealm.class.getConstructor(Ini.class));
        } catch (NoSuchMethodException e) {
            addError(e);
        }        	
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

}
