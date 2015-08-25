package com.ikanow.aleph2.security.service;



import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.credential.SimpleCredentialsMatcher;

public class NoCredentialsMatcher extends SimpleCredentialsMatcher {
	private static final Logger logger = LogManager.getLogger(NoCredentialsMatcher.class);

	
    /**
     * This implementation returns true.
     */
    @Override
    public boolean doCredentialsMatch(AuthenticationToken token, AuthenticationInfo info) { 
       
        return true;
    }

}