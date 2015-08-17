package com.ikanow.aleph2.security.module;

import org.apache.shiro.config.Ini;
import org.apache.shiro.guice.ShiroModule;
import org.apache.shiro.realm.SimpleAccountRealm;
import org.apache.shiro.realm.text.IniRealm;

import com.google.inject.Provides;

public class CoreSecurityModule extends ShiroModule {
    protected void configureShiro() {
        try {
            bindRealm().toConstructor(IniRealm.class.getConstructor(Ini.class));
            // TODO test of additional realm, remove 
            SimpleAccountRealm accountRealm = new SimpleAccountRealm("backdoor");
            accountRealm.addAccount("trojan", "none", "admin");
            bindRealm().toInstance(accountRealm);            
        } catch (NoSuchMethodException e) {
            addError(e);
        }
    }

    @Provides
    Ini loadShiroIni() {
        return Ini.fromResourcePath("classpath:shiro.ini");
    }
}
