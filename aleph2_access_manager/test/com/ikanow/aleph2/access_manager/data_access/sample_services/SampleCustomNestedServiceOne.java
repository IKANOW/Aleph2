package com.ikanow.aleph2.access_manager.data_access.sample_services;

import java.lang.reflect.InvocationTargetException;

import javax.inject.Named;

import com.google.inject.Inject;

public class SampleCustomNestedServiceOne implements SampleICustomNestedService {
	SampleICustomNestedService underlying_service;
	
	@Inject 
	public SampleCustomNestedServiceOne(@Named("SampleNestedCustomServiceUnderlying") SampleICustomNestedService underlying_service) {
		this.underlying_service = underlying_service;
	}
	
//	public static String getMessage() {
//		return "bye";
//	}
	
	public static void main(String[] args) throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, ClassNotFoundException {
		//System.out.println(SampleICustomNestedService.getMessage());
		//System.out.println(SampleCustomNestedServiceOne.getMessage());
		
		Class<?> service = Class.forName("com.ikanow.aleph2.access_manager.data_access.sample_services.SampleCustomNestedServiceOne");
		System.out.println( service.getMethod("getMessage", null).invoke(null, null));
	}
}
