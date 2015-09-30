package com.ikanow.aleph2.security.service;

import java.util.Map;
import java.util.Set;

import scala.Tuple2;

import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.security.interfaces.IAuthProvider;
import com.ikanow.aleph2.security.interfaces.IRoleProvider;

public class MockAuthProvider implements IAuthProvider{

	protected Map<String, AuthorizationBean> authMap;
	
	public MockAuthProvider(Map<String,AuthorizationBean> authMap){
		this.authMap = authMap;
	}

	@Override
	public AuthorizationBean getAuthBean(String principalName) {
		return authMap.get(principalName);
	}

}