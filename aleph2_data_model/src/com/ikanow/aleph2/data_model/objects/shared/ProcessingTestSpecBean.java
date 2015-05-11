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
package com.ikanow.aleph2.data_model.objects.shared;

import org.checkerframework.checker.nullness.qual.NonNull;

/** This bean encapsulates test-specific requests to the harvester
 * @author acp
 */
public class ProcessingTestSpecBean {

	protected ProcessingTestSpecBean() {}
	
	/** User constructor
	 * @param requested_num_objects
	 * @param max_run_time_secs
	 */
	public ProcessingTestSpecBean(final @NonNull Long requested_num_objects,
			final @NonNull Long max_run_time_secs) {
		this.requested_num_objects = requested_num_objects;
		this.max_run_time_secs = max_run_time_secs;
	}
	/** A requested number of documents - this might not be the precise number returned (eg for distributed processing 
	 * @return the desired number of objects
	 */
	public Long requested_num_objects() {
		return requested_num_objects;
	}
	/** The max time to allow the test to run for
	 * @return max time to allow the test to run for (in seconds)
	 */
	public Long max_run_time_secs() {
		return max_run_time_secs;
	}
	
	private Long requested_num_objects;
	private Long max_run_time_secs;
}
