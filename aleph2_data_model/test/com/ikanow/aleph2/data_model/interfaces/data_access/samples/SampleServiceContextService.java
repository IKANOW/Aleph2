package com.ikanow.aleph2.data_model.interfaces.data_access.samples;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.interfaces.data_access.IServiceContext;

public class SampleServiceContextService {
	private IServiceContext service_context;
	
	@Inject
	public SampleServiceContextService(@NonNull IServiceContext service_context) {
		this.service_context = service_context;
	}
	
	public IServiceContext getServiceContext() {
		return this.service_context;
	}
}
