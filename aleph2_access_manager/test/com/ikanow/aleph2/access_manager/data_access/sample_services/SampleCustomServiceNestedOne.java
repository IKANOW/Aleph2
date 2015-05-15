package com.ikanow.aleph2.access_manager.data_access.sample_services;

import com.google.inject.Inject;

public class SampleCustomServiceNestedOne {
	public SampleCustomServiceOne other_service;
	
	@Inject
	public SampleCustomServiceNestedOne(SampleCustomServiceOne other_service) {
		this.other_service = other_service;
	}
}
