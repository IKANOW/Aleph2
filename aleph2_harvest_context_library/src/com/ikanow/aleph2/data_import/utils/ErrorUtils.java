/*******************************************************************************
* Copyright 2015, The IKANOW Open Source Project.
* 
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License, version 3,
* as published by the Free Software Foundation.
* 
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Affero General Public License for more details.
* 
* You should have received a copy of the GNU Affero General Public License
* along with this program. If not, see <http://www.gnu.org/licenses/>.
******************************************************************************/
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
