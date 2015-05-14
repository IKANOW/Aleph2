package com.ikanow.aleph2.access_manager.data_access;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.ikanow.aleph2.data_model.interfaces.data_access.IServiceContext;

public class ServiceModule extends AbstractModule {

	@Override
	protected void configure() {
		bind(IServiceContext.class).to(ServiceContext.class).in(Scopes.SINGLETON);
	}

}
