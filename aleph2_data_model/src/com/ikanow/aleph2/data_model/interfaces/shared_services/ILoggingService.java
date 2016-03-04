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

import java.util.concurrent.CompletableFuture;

import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;

/**
 * @author Burch
 *
 */
public interface ILoggingService {
	
	public CompletableFuture<IBucketLogger> getLogger(final DataBucketBean bucket);
	public CompletableFuture<IBucketLogger> getSystemLogger(final DataBucketBean bucket);
	public CompletableFuture<IBucketLogger> getExternalLogger(final String subsystem);
	
	/**
	 * Typical entrypoint for user generated log messages.  These will be filtered and sent to storage based on the bucket config.
	 * They always have the field "generated_by" set to "user".
	 * 
	 * @param level
	 * @param bucket
	 * @param message
	 * @return
	 */
//	public CompletableFuture<?> log(final Level level, final DataBucketBean bucket, final BasicMessageBean message);
	
	//system log - todo some day we can split these out to be 2 different interfaces to help avoid confusion to users
	/**
	 * Logging entrypoint for system generated log messages.  These will be filtered and sent to storage based on the bucket config.
	 * They always have the field "generated_by" set to "system".  If bucket is empty, will grab the external bucket and send messages to
	 * there instead.
	 * 
	 * @param level
	 * @param bucket
	 * @param message
	 * @return
	 */
//	public CompletableFuture<?> systemLog(final Level level, final Optional<DataBucketBean> bucket, final BasicMessageBean message);
}


