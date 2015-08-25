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
package com.ikanow.aleph2.analytics.utils;

/** Error messages for analytics context
 * @author Alex
 */
public class ErrorUtils extends com.ikanow.aleph2.data_model.utils.ErrorUtils {

	final public static String NOT_YET_IMPLEMENTED = "Functionality is not yet implemented";
	final public static String NOT_SUPPORTED_IN_STREAM_ANALYICS = "Functionality does not apply to streaming context - this is for batch";
	final public static String NOT_SUPPORTED_IN_BATCH_ANALYICS = "Functionality does not apply to batch context - this is for streaming";
	final public static String SERVICE_RESTRICTIONS = "Can't call getAnalyticsContextSignature with different 'services' parameter; can't call getUnderlyingArtefacts without having called getEnrichmentContextSignature.";
	final public static String TECHNOLOGY_NOT_MODULE = "Can only be called from technology, not module";
	final public static String MODULE_NOT_TECHNOLOGY = "Can only be called from module, not technology";
}
