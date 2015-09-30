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

import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.subject.SimplePrincipalCollection;

import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;

public class CoreAuthenticationInfo implements AuthenticationInfo {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3026403309641522543L;
	private AuthorizationBean authorizationBean;

	public AuthorizationBean getAuthorizationBean() {
		return authorizationBean;
	}

	private SimplePrincipalCollection principalCollection;

	public CoreAuthenticationInfo(AuthorizationBean ab, String realmName) {
		this.authorizationBean = ab;
		this.principalCollection = new SimplePrincipalCollection(ab.getPrincipalName(), realmName);
	}

	@Override
	public PrincipalCollection getPrincipals() {

		return principalCollection;
	}

	@Override
	public Object getCredentials() {
		return authorizationBean.getCredentials();
	}

}
