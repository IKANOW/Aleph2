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
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.security.utils.ErrorUtils;
//
//system permission use together with JVMRolePovider:
//"connect:<*|host>:[*|<port>]"
//"read:<filePath having '/' replaced with ':'>"
//"write:<filePath having '/' replaced with ':'>"
//"delete:<filePath having '/' replaced with ':'>"
//"exec:<filePath having '/' replaced with ':'>"
//"package:<packagename>]"
// permisson is mapped as is

	public class JVMSecurityManager extends SecurityManager 
	{		private static final Logger logger = LogManager.getLogger(JVMSecurityManager.class);

		protected ThreadLocal<Boolean> tlEnabled = new ThreadLocal<Boolean>();
		protected ThreadLocal<AuthorizationBean> tlAuthBean;
		protected ISecurityService securityService;
		protected ThreadLocal<ISubject> tlSubject = new ThreadLocal<ISubject>();

		public JVMSecurityManager(ISecurityService securityService){
			super();
			this.securityService = securityService;
			tlSubject.set(securityService.loginAsSystem());
		}
		
		public void setAuthBean(AuthorizationBean authBean){
			tlAuthBean.set(authBean);
		}

		public void setSecureFlag(boolean isJavascript)
		{
			tlEnabled.set(isJavascript);
		}

		@Override
		public void checkConnect(String host, int port)
		{	
			
			if (isEnabled())
			{			
				// TODO do we want custom logic here or just add permissions
/*				//if failed we are using our javascript security
				//http://en.wikipedia.org/wiki/Private_network
				//see also http://en.wikipedia.org/wiki/Reserved_IP_addresses 
				// (note I don't block all the addresses in there, only local/local subnet ones)
				//deny for 10.*
				//deny for 192.186.*
				//deny for 127.*
				//deny for 169.253.*
				if ( host.matches("^(10\\.|127\\.|192\\.168\\.|172\\.1[6-9]\\.|172\\.[2][0-9]\\.|172\\.3[01]\\.|169\\.254\\.).*") )
				{
					throw new SecurityException("Hosts: 10.*, 192.168.*, 127.*, 172.16-31,169.254.* are not allowed to be connected to");
				}
				*/
				String check = "connect:"+convertHostToWildcardPermission(host)+":"+port;
				if (!securityService.isPermitted(tlSubject.get(), check) );
				{
					throwSecurityException(tlSubject," connect host:"+port);
				}
				
			}
			// Always do this: so we're the union of configured restrictions+the above custom restrictions
			super.checkConnect(host,port);		
		}		
		
		protected static String convertHostToWildcardPermission(String host) {
			if(host!=null){
			host = host.replaceAll(".", ":");
			}
			return host;
		}

		@Override
		public synchronized void checkRead(String file) {			
					
			if (isEnabled())
			{
				String check = "read:"+convertPathToWildcardPermission(file);
				if(!securityService.isPermitted(tlSubject.get(), check)){
					throwSecurityException(tlSubject,file);
				}
			}
			super.checkRead(file);
		}
		
	protected static String convertPathToWildcardPermission(String path) {
		if (path != null) {
			path = path.replaceAll("/", ":");
			if (path.startsWith(":")) {
				path = path.substring(1);
			}
		}
		return path;
	}

		@Override
		public void checkRead(String file, Object context) {
			if (isEnabled())
			{
				String check = "read:"+convertPathToWildcardPermission(file);
				if(!securityService.isPermitted(tlSubject.get(), check)){
					throwSecurityException(tlSubject,file);
				}
			}
			super.checkRead(file, context);
		}
		
		@Override
		public void checkWrite(String file) {
			if (isEnabled())
			{
				String check = "write:"+convertPathToWildcardPermission(file);
				if(!securityService.isPermitted(tlSubject.get(), check)){
					throwSecurityException(tlSubject,file);
				}
			}
			super.checkWrite(file);
		}
		
		@Override
		public void checkDelete(String file) {
			if (isEnabled())
			{
				String check = "delete:"+convertPathToWildcardPermission(file);
				if(!securityService.isPermitted(tlSubject.get(), check)){
					throwSecurityException(tlSubject,file);
				}
			}
			super.checkDelete(file);
		}
		
		@Override
		public void checkExec(String cmd) {
			if (isEnabled())
			{
				String check = "exec:"+convertPathToWildcardPermission(cmd);
				if(!securityService.isPermitted(tlSubject.get(), check)){
					throwSecurityException(tlSubject,cmd);
				}
			}
			super.checkExec(cmd);
		}	

		@Override
		public void checkPackageAccess(String packageName) {
			if (isEnabled())
			{
				String check = packageName;
				if(!securityService.isPermitted(tlSubject.get(), check)){
					throwSecurityException(tlSubject,packageName);
				}
			}
			super.checkPackageAccess(packageName);
		}//TESTED (by hand)
		
		@Override
		public void checkPermission(Permission permission) { 
			if (isEnabled())
			{
				String check = permission.getName();
				if(!securityService.isPermitted(tlSubject.get(), check)){
					throwSecurityException(tlSubject,check);
				}
			}
			super.checkPermission(permission);
		}//TESTED (by hand)
		

		protected boolean isEnabled(){
			Boolean lock = tlEnabled.get();
			return (lock != null && lock);
		}
		
		protected static void throwSecurityException(ThreadLocal<ISubject> tlSubject,String message) throws SecurityException{
			String errorMsg = ErrorUtils.get(ErrorUtils.SECURITY_ERROR,tlSubject.get().getSubject(),message); 
			logger.error(errorMsg);
			throw new SecurityException("Read Access is not allowed");							
		}
}