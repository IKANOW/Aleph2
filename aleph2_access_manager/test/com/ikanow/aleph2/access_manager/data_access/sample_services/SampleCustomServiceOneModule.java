package com.ikanow.aleph2.access_manager.data_access.sample_services;

import com.google.inject.AbstractModule;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.name.Names;

public class SampleCustomServiceOneModule extends AbstractModule {

	@Override
	protected void configure() {
		bind(IDependency.class).annotatedWith(Names.named("SampleDepOne")).to(SampleDependencyOne.class).in(Scopes.SINGLETON);
	}

}
