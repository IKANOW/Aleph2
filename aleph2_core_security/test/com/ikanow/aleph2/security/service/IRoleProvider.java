package com.ikanow.aleph2.security.service;

import java.util.Set;

import scala.Tuple2;

public interface IRoleProvider {

	public Tuple2<Set<String>,Set<String>> getRolesAndPermissions(String principalName);
	
	
}
