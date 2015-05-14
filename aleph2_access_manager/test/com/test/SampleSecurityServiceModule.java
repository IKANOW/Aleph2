package com.test;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.ikanow.aleph2.access_manager.data_access.sample_services.IDependency;
import com.ikanow.aleph2.access_manager.data_access.sample_services.SampleDependencyOne;

public class SampleSecurityServiceModule extends AbstractModule {

	@Override
	protected void configure() {
		bind(IDependency.class).to(SampleDependencyOne.class).in(Scopes.SINGLETON);
	}

}
