/*******************************************************************************
 * Copyright 2016, The IKANOW Open Source Project.
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

import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;

/**
 * @author Burch
 *
 */
public interface ILoggingService extends IUnderlyingService {
	
	/**
	 * Typical entry point for user generated log messages.  Returns a logger pointed to the passed in bucket.
	 * All log messages will be appended with a "generated_by":"user" field:value.
	 * @param bucket
	 * @return
	 */
	public IBucketLogger getLogger(final DataBucketBean bucket);
	/**
	 * Logging entry point for system generated log messages.  Returns a logger pointed to the passed in bucket.
	 * All log messages will be appended with a "generated_by":"system" field:value.
	 * @param bucket
	 * @return
	 */
	public IBucketLogger getSystemLogger(final DataBucketBean bucket);
	/**
	 * Entrypoint for external services not related to a bucket.  Returns a logger pointed to an external space and
	 * referencing the subsystem passed in (name of the service).
	 * @param subsystem
	 * @return
	 */
	public IBucketLogger getExternalLogger(final String subsystem);		
}


