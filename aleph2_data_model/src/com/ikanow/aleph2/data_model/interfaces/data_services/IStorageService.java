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
package com.ikanow.aleph2.data_model.interfaces.data_services;

import java.util.Optional;

import org.checkerframework.checker.nullness.qual.NonNull;

public interface IStorageService {

	/** Returns the root path for all Aleph2 DB related activities
	 * @return the root path, in a URI that is supported by the underlying file system (see getUnderlyingPlatformDriver)
	 */
	String getRootPath();
	
	/** USE WITH CARE: this returns the driver to the underlying technology
	 *  shouldn't be used unless absolutely necessary!
	 *  In this particular case, it will always point to HDFS FileSystem class, so it _can_ be used
	 *  safely (the FileSystem API requires a huge set of JARs so this is left generic)
	 * @param driver_class the class of the driver
	 * @param a string containing options in some technology-specific format
	 * @return a driver to the underlying technology. Will exception if you pick the wrong one!
	 */
	@NonNull 
	<T> T getUnderlyingPlatformDriver(final @NonNull Class<T> driver_class, final Optional<String> driver_options);
}
