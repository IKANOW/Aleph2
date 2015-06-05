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
package com.ikanow.aleph2.data_model.interfaces.shared_services;

import java.util.Collections;
import java.util.List;


import com.google.inject.AbstractModule;

//import com.google.inject.Module;

/** A interface "contract" that indicates that the service developer wishes the framework to use the top level
 *  dependency injection framework to load some "internal" classes.
 * @author acp
 *
 */
public interface IExtraDependencyLoader {

	/** A default function that returns a list of Modules (usually just one, or none) that should be launched in guice
	 *  whenever this service is instantiated. This won't actually get overridden, but provided the top level function.
	 *   
	 * @return a possibly empty list of modules
	 */
	static List<AbstractModule> getExtraDependencyModules() { return Collections.emptyList(); }
	
	/** This method is unusued, it simply exists to remind developers that they need to create a function called
	 * {@link #getExtraDependencyModules()}, with the same method (practically "overriding" it) in their implementing class for this service.
	 * 
	 */
	public void youNeedToImplementTheStaticFunctionCalled_getExtraDependencyModules();
}
