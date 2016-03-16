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
package com.ikanow.aleph2.logging.data_model;

import org.apache.logging.log4j.Level;

/**
 * Config options for the Logging Service.
 * @author Burch
 *
 */
public class LoggingServiceConfigBean {

	public static final String PROPERTIES_ROOT = "CoreLoggingService";
	private String default_time_field;
	private Level default_system_log_level;
	private Level default_user_log_level;
	
	protected LoggingServiceConfigBean() {}
	
	public LoggingServiceConfigBean(final String default_time_field, final Level default_system_log_level, final Level default_user_log_level) {
		this.default_time_field = default_time_field;
		this.default_system_log_level = default_system_log_level;
		this.default_user_log_level = default_user_log_level;
	}
	
	/**
	 * Default field to output logigng timestamp as (defaults to 'date')
	 * @return
	 */
	public String default_time_field() { return this.default_time_field; }
	/**
	 * Default Level to log system level messages as (defaults to 'OFF')
	 * @return
	 */
	public Level default_system_log_level() { return this.default_system_log_level; }
	/**
	 * Default Level to log user level messages as (defaults to 'OFF')
	 * @return
	 */
	public Level default_user_log_level() { return this.default_user_log_level; }
}
