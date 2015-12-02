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
 *******************************************************************************/
package com.ikanow.aleph2.data_model.interfaces.shared_services;

import java.util.Collection;
import java.util.Optional;

/** A service that enables the underlying artefacts to be exposed
 * @author Alex
 */
public interface IUnderlyingService {

	/** This method needs to be implemented by the underlying management DB and return an object from each dependency (normally just the CRUD service, maybe the search index service also)
	 *  This enables context libraries to grab the associated JAR files and place them in external processes' classpaths. This method will also return the underlying CRUD service itself,
	 *  for convenience
	 *  NOTE: by convention should also return "this"
	 * @return
	 */
	Collection<Object> getUnderlyingArtefacts();
	
	/** USE WITH CARE: this returns the driver to the underlying technology
	 *  shouldn't be used unless absolutely necessary!
	 *  One standard use is in analytics, where the driver_class is an instance of IAnalyticsAccessContext
	 *  and the driver options are "owner_id:bucket_full_name:JSON(input_config)" ... this enables analytics engines to request 
	 *  input drivers from data services without the two really knowing about each other
	 * @param driver_class the class of the driver
	 * @param a string containing options in some technology-specific format
	 * @return a driver to the underlying technology. Will exception if you pick the wrong one!
	 */
	<T> Optional<T> getUnderlyingPlatformDriver(final Class<T> driver_class, final Optional<String> driver_options);
}
