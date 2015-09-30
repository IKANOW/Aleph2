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
