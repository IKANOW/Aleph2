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
package com.ikanow.aleph2.core.shared.utils;

/** Error string specific to logic contained in the core shared library
 * @author Alex
 */
public class SharedErrorUtils extends com.ikanow.aleph2.data_model.utils.ErrorUtils {

	public static final String SHARED_LIBRARY_NAME_NOT_FOUND = "Shared library {1} not found (or user does not have read permission): {0}";
	public static final String ERROR_LOADING_CLASS = "Error loading class {1}: {0}";
	public static final String ERROR_CLASS_NOT_SUPERCLASS = "Error: class {0} is not an implementation of {1}: this may be because you have included eg aleph2_data_model in your class - you should not include any core/contrib JARs in there.";
	public static final String ERROR_CACHING_SHARED_LIBS = "Misc error caching shared libs for bucket {1}: {0}";
}
