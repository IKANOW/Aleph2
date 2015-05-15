package com.ikanow.aleph2.access_manager.data_access.sample_services;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.name.Names;

public class SampleCustomServiceTwoModule implements Module {
	
	@Override
	public void configure(Binder binder) {
		binder.bind(IDependency.class).annotatedWith(Names.named("SampleDepTwo")).to(SampleDependencyTwo.class);
	}

}
