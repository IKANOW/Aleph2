package com.ikanow.aleph2.access_manager.data_access.sample_services;

import java.util.Collections;
import java.util.List;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.google.inject.Module;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IExtraDependencyLoader;

public class SampleExtraDependencyLoader implements IExtraDependencyLoader {

	public static @NonNull List<Module> getExtraDependencyModules() { 
		//TODO kick out a depedency
		return Collections.emptyList(); 
	}
	
	@Override
	public void youNeedToImplementTheStaticFunctionCalled_getExtraDependencyModules() {
		//unused
		
	}

}
