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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import scala.Tuple2;

import com.ikanow.aleph2.security.interfaces.IRoleProvider;

public class MockRoleProvider implements IRoleProvider{

	
	protected Map<String, Set<String>> rolesMap;
	protected Map<String, Set<String>> permissionsMap;
	
	public MockRoleProvider(Map<String,Set<String>> rolesMap, Map<String,Set<String>> permissionsMap){
		this.rolesMap = rolesMap;
		this.permissionsMap = permissionsMap;
	}
	@Override
	public Tuple2<Set<String>, Set<String>> getRolesAndPermissions(String principalName) {

		Set<String> roles = rolesMap.get(principalName);
		Set<String> permissions = permissionsMap.get(principalName);
		if(roles==null){
			roles =  new HashSet<String>();
		}
		if(permissions==null){
			permissions =  new HashSet<String>();
		}
		return new Tuple2<Set<String>, Set<String>>(roles,permissions);
	}

}
