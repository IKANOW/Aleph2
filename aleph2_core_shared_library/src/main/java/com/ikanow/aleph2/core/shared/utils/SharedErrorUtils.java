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
package com.ikanow.aleph2.core.shared.utils;

import java.util.Date;

import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;

public class SharedErrorUtils extends com.ikanow.aleph2.data_model.utils.ErrorUtils {

	public static final String SHARED_LIBRARY_NAME_NOT_FOUND = "Shared library {1} not found: {0}";
	public static final String ERROR_LOADING_CLASS = "Error loading class {1}: {0}";
	public static final String ERROR_CLASS_NOT_SUPERCLASS = "Error: class {0} is not an implementation of {1}: this may be because you have included eg aleph2_data_model in your class - you should not include any core/contrib JARs in there.";
	public static final String ERROR_CACHING_SHARED_LIBS = "Misc error caching shared libs for bucket {1}: {0}";
	 
	///////////////////////////////////////////////////////////////////////////
	
	/** Builds a fairly generic error message to return 
	 * @param error - the error string
	 * @param handler - the actor handling this error
	 * @param message - the original message that spawned this error
	 * @return
	 */
	public static <M> BasicMessageBean buildErrorMessage(final String handler, final M message,
			final String error, final Object... params)
	{
		return new BasicMessageBean(
					new Date(), // date
					false, // success
					handler,
					message instanceof String ? message.toString() : message.getClass().getSimpleName(), // command
					null, // message code
					params.length == 0 ? error : ErrorUtils.get(error, params), // error message, with optional formatting
					null // details
					);
	}
	
}
