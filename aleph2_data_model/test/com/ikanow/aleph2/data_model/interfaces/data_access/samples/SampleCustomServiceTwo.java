package com.ikanow.aleph2.data_model.interfaces.data_access.samples;

import java.util.Arrays;
import java.util.List;

import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.name.Named;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IExtraDependencyLoader;

public class SampleCustomServiceTwo implements ICustomService, IExtraDependencyLoader {
	public IDependency dep;
	
	@Inject
	public SampleCustomServiceTwo(@Named("SampleDepTwo") IDependency dep) {
		this.dep = dep;
	}
	
	public static List<Module> getExtraDependencyModules() {
		return Arrays.asList(new SampleCustomServiceTwoModule());
	}
	
	@Override
	public void youNeedToImplementTheStaticFunctionCalled_getExtraDependencyModules() {
		//empty
	}
}
