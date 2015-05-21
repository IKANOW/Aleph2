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
package com.ikanow.aleph2.data_import_manager.harvest.utils;

import java.util.Date;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;

public class HarvestErrorUtils extends com.ikanow.aleph2.data_model.utils.ErrorUtils {

	public static final String NO_TECHNOLOGY_NAME_OR_ID = "No harvest technology name or id in bucket {0}";
	public static final String HARVEST_TECHNOLOGY_NAME_NOT_FOUND = "No valid harvest technology {0} found for bucket {1}";
	public static final String SHARED_LIBRARY_NAME_NOT_FOUND = "Shared library {1} not found: {0}";
	public static final String ERROR_LOADING_CLASS = "Error loading class {1}: {0}";
	 
	///////////////////////////////////////////////////////////////////////////
	
	/** Builds a fairly generic error message to return 
	 * @param error - the error string
	 * @param handler - the actor handling this error
	 * @param message - the original message that spawned this error
	 * @return
	 */
	public static <M> BasicMessageBean buildErrorMessage(final @NonNull String handler, final @NonNull M message,
			final @NonNull String error, final @NonNull Object... params)
	{
		return new BasicMessageBean(
					new Date(), // date
					false, // success
					handler,
					message.getClass().getSimpleName(), // command
					null, // message code
					params.length == 0 ? error : ErrorUtils.get(error, params), // error message, with optional formatting
					null // details
					);
	}
	
}
