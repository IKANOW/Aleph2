package com.ikanow.aleph2.data_model.interfaces.data_access.samples;

import java.util.UUID;

public class SampleCustomServicePlain implements ICustomService1, ICustomService2 {
	public String initialization_id;
	
	public SampleCustomServicePlain() {
		initialization_id = UUID.randomUUID().toString();
	}
}
