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
 ******************************************************************************/
package com.ikanow.aleph2.data_model.objects.shared;

/** Contains information about authorization rights on an object
 * @author acp
 */
public class AuthorizationBean {
	protected AuthorizationBean() {}
	
	public AuthorizationBean(final String auth_token) {
		this.auth_token = auth_token;		
	}
	public String auth_token() { return auth_token; }
	protected String auth_token;
}
