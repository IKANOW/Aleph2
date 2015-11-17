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
package com.ikanow.aleph2.data_model.objects.shared;

import java.io.Serializable;


/** This bean encapsulates test-specific requests to the harvester
 * @author acp
 */
public class ProcessingTestSpecBean implements Serializable {
	private static final long serialVersionUID = -461079824203542423L;
	protected ProcessingTestSpecBean() {}
	
	/** User constructor, defaults max storage time to 24h, max startup time to 2m and
	 * overwrite existing data to true
	 * @param requested_num_objects
	 * @param max_run_time_secs
	 */
	public ProcessingTestSpecBean(final Long requested_num_objects,
			final Long max_run_time_secs) {
		this(requested_num_objects, 120L, max_run_time_secs, 86400L, true);
	}
	
	/**
	 * User constructor
	 * @param requested_num_objects
	 * @param max_run_time_secs
	 * @param max_storage_time_secs
	 * @param overwrite_existing_data
	 */
	public ProcessingTestSpecBean(final Long requested_num_objects,
			final Long max_startup_time_secs,
			final Long max_run_time_secs,
			final Long max_storage_time_secs,
			final Boolean overwrite_existing_data) {
		this.requested_num_objects = requested_num_objects;
		this.max_startup_time_secs = max_startup_time_secs;
		this.max_run_time_secs = max_run_time_secs;
		this.max_storage_time_secs = max_storage_time_secs;
		this.overwrite_existing_data = overwrite_existing_data;
	}
	
	/** A requested number of documents - this might not be the precise number returned (eg for distributed processing 
	 * @return the desired number of objects
	 */
	public Long requested_num_objects() {
		return requested_num_objects;
	}
	
	/**
	 * The max time to allow the test to startup
	 * @return max time to allow the test to startup for (in seconds)
	 */
	public Long max_startup_time_secs() {
		return max_startup_time_secs;
	}
	
	/** The max time to allow the test to run for
	 * @return max time to allow the test to run for (in seconds)
	 */
	public Long max_run_time_secs() {
		return max_run_time_secs;
	}
	
	/**
	 * Permission to overwrite old test data or not
	 * @return whether to overwrite old test data or not, defaults to true
	 */
	public Boolean overwrite_existing_data() {
		return overwrite_existing_data;
	}
	
	/**
	 * The max time to store test data for
	 * @return max time to store test data for (in seconds), defaults to 86400 (24h)
	 */
	public Long max_storage_time_secs() {
		return max_storage_time_secs;
	}
	
	private Long requested_num_objects;
	private Long max_startup_time_secs;
	private Long max_run_time_secs;
	private Long max_storage_time_secs;
	private Boolean overwrite_existing_data;
}
