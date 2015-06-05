package com.ikanow.aleph2.data_model.interfaces.data_access.samples;


import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;

public class SampleServiceContextService {
	private IServiceContext service_context;
	
	@Inject
	public SampleServiceContextService(IServiceContext service_context) {
		this.service_context = service_context;
	}
	
	public IServiceContext getServiceContext() {
		return this.service_context;
	}
}
