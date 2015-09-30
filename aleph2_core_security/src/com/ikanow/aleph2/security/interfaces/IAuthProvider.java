package com.ikanow.aleph2.security.interfaces;

import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;

public interface IAuthProvider {

	public AuthorizationBean getAuthBean(String principalName);
	
}
