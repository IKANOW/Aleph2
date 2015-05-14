package com.ikanow.aleph2.access_manager.data_access;

import java.util.Optional;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.name.Names;

public class ServiceBinderModule extends AbstractModule {

	private Class serviceClass;
	private Optional<Class> interfaceClazz;
	private Optional<String> annotationName;
	
	public ServiceBinderModule(@NonNull Class serviceClazz, Optional<Class> interfaceClazz, Optional<String> annotationName) {
		this.serviceClass = serviceClazz;
		this.interfaceClazz = interfaceClazz;
		this.annotationName = annotationName;
	}
	
	@Override
	protected void configure() {
		if ( interfaceClazz.isPresent() ) {
			if ( annotationName.isPresent() ) {
				bind(interfaceClazz.get()).annotatedWith(Names.named(annotationName.get())).to(serviceClass).in(Scopes.SINGLETON); 
			} else
				bind(interfaceClazz.get()).to(serviceClass).in(Scopes.SINGLETON);
		} else {
			bind(serviceClass).in(Scopes.SINGLETON); //you can't annotate a plain bind
		}		
	}

}
