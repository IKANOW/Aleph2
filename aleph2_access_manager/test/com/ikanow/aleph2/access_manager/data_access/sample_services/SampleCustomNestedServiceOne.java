package com.ikanow.aleph2.access_manager.data_access.sample_services;

import com.google.inject.Inject;

public class SampleCustomNestedServiceOne implements ICustomNestedService {
	IDependency dep;
	
	@Inject 
	public SampleCustomNestedServiceOne(IDependency dep) {
		this.dep = dep;
	}
}
