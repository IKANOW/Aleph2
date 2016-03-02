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

import java.security.Permission;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISubject;

//
//system permission use together with JVMRolePovider:
//"connect:<*|host>:[*|<port>]"
//"read:<filePath having '/' replaced with ':'>"
//"write:<filePath having '/' replaced with ':'>"
//"delete:<filePath having '/' replaced with ':'>"
//"exec:<filePath having '/' replaced with ':'>"
//"exec:<filePath having '/' replaced with ':'>"
//"package:<package name having '.' replaced with ':'>"
// permisson:<permissionName'>"

public class JVMSecurityManager extends SecurityManager {
	private static final Logger logger = LogManager.getLogger(JVMSecurityManager.class);

	protected ThreadLocal<Boolean> tlEnabled = new ThreadLocal<Boolean>();
	protected ISecurityService securityService;
	protected ThreadLocal<ISubject> tlSubject = new ThreadLocal<ISubject>();
	protected ThreadLocal<Boolean> tlCheckSuper =  new ThreadLocal<Boolean>();
	
	protected static String JAVA_HOME = System.getProperty("java.home"); 

	public boolean checkSuper() {
		Boolean lock = tlCheckSuper.get();
		return (lock != null && lock);

	}

	public void setAlsoCheckSuper(boolean checkSuper) {
		this.tlCheckSuper.set(checkSuper);
	}

	public JVMSecurityManager(ISecurityService securityService) {
		super();
		this.securityService = securityService;
		String policy = System.getProperty("securirty.policy");
		if(policy!=null){
			setAlsoCheckSuper(true);
		}
	}

	public void setEnabled(boolean enabled) {
		tlEnabled.set(enabled);
	}

	public void setSubject(ISubject subject) {
		tlSubject.set(subject);
	}

	/** call this to dispose of the ThreadLocal subject before nullifying the security manager
	 * 
	 */
	public void releaseSubject() {
		tlSubject.remove();
	}

	@Override
	public void checkConnect(String host, int port) {

		if (isEnabled()) {
			// TODO do we want custom logic here or just add permissions
			/*
			 * //if failed we are using our javascript security
			 * //http://en.wikipedia.org/wiki/Private_network //see also
			 * http://en.wikipedia.org/wiki/Reserved_IP_addresses // (note I
			 * don't block all the addresses in there, only local/local subnet
			 * ones) //deny for 10.* //deny for 192.186.* //deny for 127.*
			 * //deny for 169.253.* if ( host.matches(
			 * "^(10\\.|127\\.|192\\.168\\.|172\\.1[6-9]\\.|172\\.[2][0-9]\\.|172\\.3[01]\\.|169\\.254\\.).*"
			 * ) ) { throw new SecurityException(
			 * "Hosts: 10.*, 192.168.*, 127.*, 172.16-31,169.254.* are not allowed to be connected to"
			 * ); }
			 */
			String check = "connect:" + convertHostToWildcardPermission(host) + ":" + port;
			if (!isPermitted(check))
			{
				throwSecurityException(tlSubject,"checkConnect", " connect host:" + port);
			}

			// Always do this: so we're the union of configured restrictions+the
			// above custom restrictions
		} //enabled
		if(checkSuper()){
			super.checkConnect(host, port);
		}
	}



	@Override
	public void checkRead(String file) {

		if (isEnabled()) {
			// Allow JRE access, and classes and JAR files/native libs and that's it...
			if (!file.startsWith(JAVA_HOME)) {
				if (!(file.endsWith(".class") || file.endsWith(".jar") || file.endsWith(".dll") || file.endsWith(".so"))) {
					String check = "read:" + convertPathToWildcardPermission(file);
//					if (!securityService.isPermitted(tlSubject.get(), check)) {
    				if (!securityService.isPermitted(check)) {
						throwSecurityException(tlSubject,"checkRead", file);
					}
				}
			}
		} 
		if(checkSuper()){
			super.checkRead(file);
		}
	}

	// Infinite loop workaround: http://jira.smartfrog.org/jira/browse/SFOS-236
	protected boolean inReadCheck = false;

	@Override
	public void checkRead(String file, Object context) {
		
		if (isEnabled()) {
			String check = "read:" + convertPathToWildcardPermission(file);
			// Allow JRE access, and classes and JAR files/native libs and that's it...
			if (!file.startsWith(JAVA_HOME)) {
				if (!(file.endsWith(".class") || file.endsWith(".jar") || file.endsWith(".dll") || file.endsWith(".so"))) {
					if (!isPermitted(check)) {
						throwSecurityException(tlSubject, "checkRead",file);
					} // permitted
				}
			}
		}
		if(checkSuper()){
			super.checkRead(file, context);
		}
	}

	@Override
	public void checkWrite(String file) {
		if (isEnabled()) {
			String check = "write:" + convertPathToWildcardPermission(file);
			if (!isPermitted( check)) {
				throwSecurityException(tlSubject,"checkWrite", file);
			}
		}
		if(checkSuper()){
			super.checkWrite(file);
		}
	}

	@Override
	public void checkDelete(String file) {
		if (isEnabled()) {
			String check = "delete:" + convertPathToWildcardPermission(file);
			if (!isPermitted( check)) {
				throwSecurityException(tlSubject,"checkDelete" ,file);
			}
		}
		if(checkSuper()){
			super.checkDelete(file);
		}
	}

	@Override
	public void checkExec(String cmd) {
		if (isEnabled()) {
			String check = "exec:" + convertPathToWildcardPermission(cmd);
			if (!isPermitted( check)) {
				throwSecurityException(tlSubject, "checkExec",cmd);
			}
		}
		if(checkSuper()){
			super.checkExec(cmd);
		}
	}

	@Override
	public void checkPackageAccess(String packageName) {
		if (isEnabled()) {
			String check =  "package:" +convertPackageToWildcardPermission(packageName);			
			if (!isPermitted( check)) {
				throwSecurityException(tlSubject, "checkPackageAccess",packageName);
			}
		}
		if(checkSuper()){
			super.checkPackageAccess(packageName);
		}
	}// TESTED (by hand)

	@Override
	public void checkPermission(Permission permission) {
		if (isEnabled()) {
			String check = "permission:"+permission.getName();
			if (!isPermitted( check)) {
				throwSecurityException(tlSubject, "checkPermission",check);
			}
		}
		if(checkSuper()){
			super.checkPermission(permission);
		}
	}

	protected boolean isEnabled() {
		Boolean lock = tlEnabled.get();
		return (lock != null && lock);
	}

	protected static void throwSecurityException(ThreadLocal<ISubject> tlSubject,String source,  String message) throws SecurityException {
		// we do not want to acces outside packages here, so we do not use Errorutils.
		String errorMsg = "Security Error in "+source +" for "+tlSubject.get().getName()+" accessing "+message;
		logger.error(errorMsg);
		throw new SecurityException(errorMsg);
	}
	
	protected static String convertPathToWildcardPermission(String path) {
		if (path != null) {
			path = path.replace("/", ":");
			if (path.startsWith(":")) {
				path = path.substring(1);
			}
		}
		return path;
	}

	protected static String convertHostToWildcardPermission(String host) {
		if (host != null) {
			host = host.replace(".", ":");
		}
		return host;
	}

	protected static String convertPackageToWildcardPermission(String host) {
		if (host != null) {
			host = host.replace(".", ":");
		}
		return host;
	}

	protected boolean isPermitted(String permission) {
		ISubject subject = tlSubject.get();
		boolean permitted = false;
		if (subject != null) {
			// prevent recursive calls while checking
			boolean enabled = isEnabled();
			setEnabled(false);
			boolean checkSuperEnabled = checkSuper();
			setAlsoCheckSuper(false);
			logger.trace("isPermitted*"+enabled+"*"+permission);
			try {
				permitted = securityService.isPermitted(permission);				
			} catch (Throwable t) {
				logger.error(t);
			}
			// restore previous state
			setEnabled(enabled);
			setAlsoCheckSuper(checkSuperEnabled);
		}
		return permitted;
	}

}