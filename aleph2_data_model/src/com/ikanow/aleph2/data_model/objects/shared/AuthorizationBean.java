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

import java.util.Collections;
import java.util.Map;

/** Contains information about authorization rights on an object
 * @author acp
 */
public class AuthorizationBean {
	protected AuthorizationBean() {}
	
	/** User constructor
	 * @param auth_token_vs_role
	 */
	public AuthorizationBean(final Map<String, String> auth_token_vs_role) {
		this.auth_token_vs_role = auth_token_vs_role;		
	}
	/** User/group token vs generic role string
	 * @return
	 */
	public Map<String, String> auth_token() { return auth_token_vs_role == null ? auth_token_vs_role : Collections.unmodifiableMap(auth_token_vs_role); }
	protected Map<String, String> auth_token_vs_role;
}
