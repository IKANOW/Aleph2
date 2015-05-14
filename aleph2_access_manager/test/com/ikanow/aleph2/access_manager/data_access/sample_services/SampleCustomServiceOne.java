package com.ikanow.aleph2.access_manager.data_access.sample_services;

import java.util.Arrays;
import java.util.List;

import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.name.Named;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IExtraDependencyLoader;

public class SampleCustomServiceOne implements ICustomService, IExtraDependencyLoader {

	public IDependency dep;
	
	@Inject
	public SampleCustomServiceOne(@Named("SampleDepOne") IDependency dep) {
		this.dep = dep;
	}
	
	public static List<Module> getExtraDependencyModules() {
		return Arrays.asList(new SampleCustomServiceOneModule());
	}
	
	@Override
	public void youNeedToImplementTheStaticFunctionCalled_getExtraDependencyModules() {
		//empty
	}

}
