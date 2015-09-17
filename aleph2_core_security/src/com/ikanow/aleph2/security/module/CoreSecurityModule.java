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

import org.apache.shiro.config.Ini;
import org.apache.shiro.guice.ShiroModule;
import org.apache.shiro.realm.text.IniRealm;

import com.google.inject.Provides;

/** Core module for Security Service
 * @author Joern
 */
public class CoreSecurityModule extends ShiroModule {
    protected void configureShiro() {
        	bindRealms();
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
    	sessionManager.setGlobalSessionTimeout(2000);
        bind.toInstance(sessionManager);
    }
    */
    protected void bindRealms(){
        try {
        bindRealm().toConstructor(IniRealm.class.getConstructor(Ini.class));
        // TODO test of additional realm, remove 
//        SimpleAccountRealm accountRealm = new SimpleAccountRealm("backdoor");
//        accountRealm.addAccount("trojan", "none", "admin");
//        bindRealm().toInstance(accountRealm);
        } catch (NoSuchMethodException e) {
            addError(e);
        }    	
    }
}
