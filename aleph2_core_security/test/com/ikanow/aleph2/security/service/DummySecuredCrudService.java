package com.ikanow.aleph2.security.service;

import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;

public class DummySecuredCrudService extends SecuredCrudManagementDbService<String> {

	public DummySecuredCrudService(IServiceContext serviceContext, IManagementCrudService<String> delegate, AuthorizationBean authBean) {
		super(serviceContext, delegate, authBean);
		// TODO Auto-generated constructor stub
	}

}
