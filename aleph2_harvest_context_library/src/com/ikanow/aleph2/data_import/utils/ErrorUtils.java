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
package com.ikanow.aleph2.data_import.utils;

/** Error messages for harvest context
 * @author Joern
 */
public class ErrorUtils extends com.ikanow.aleph2.data_model.utils.ErrorUtils {

	final public static String NOT_YET_IMPLEMENTED = "This operation is not currently supported";
	final public static String TECHNOLOGY_NOT_MODULE = "Can only be called from technology, not module";
	final public static String NO_BUCKET = "Unable to locate bucket: {0}";
	final public static String NOT_SUPPORTED_IN_BATCH_ENRICHMENT = "Functionality does not apply to batch enrichment context - this is for streaming enrichment";
	final public static String SERVICE_RESTRICTIONS = "Can't call getHarvestContextSignature with different 'services' parameter; can't call getUnderlyingArtefacts without having called getHarvestContextSignature.";
	final public static String VALIDATION_ERROR = "Validation Error: {0}";
	final public static String EXCEPTION_CAUGHT = "Caught Exception: {0}";

}
