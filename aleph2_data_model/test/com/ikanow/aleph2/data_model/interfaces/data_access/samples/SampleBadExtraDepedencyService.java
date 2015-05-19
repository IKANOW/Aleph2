package com.ikanow.aleph2.data_model.interfaces.data_access.samples;

import com.ikanow.aleph2.data_model.interfaces.shared_services.IExtraDependencyLoader;

public class SampleBadExtraDepedencyService implements IExtraDependencyLoader {

	@Override
	public void youNeedToImplementTheStaticFunctionCalled_getExtraDependencyModules() {
		// TODO Auto-generated method stub

	}
	
	//NOTICE I did not implement getExtraDependencyModules() intentionally to fail the test

}
