/*******************************************************************************
 * Copyright 2015, The IKANOW Open Source Project.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.ikanow.aleph2.data_model.interfaces.data_access.samples;

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
