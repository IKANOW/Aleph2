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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author Burch
 *
 */
public class LoggingServiceConfig {
	private static Logger _logger = LogManager.getLogger();
	private String default_time_field;
	private Level default_system_log_level;
	private Level default_user_log_level;
	private Level system_mirror_to_log4j_level;
	
	public LoggingServiceConfig(final String default_time_field, final String default_system_log_level, final String default_user_log_level, final String system_mirror_to_log4j_level) {
		this.default_time_field = default_time_field;
		this.default_system_log_level = convertToLevel(default_system_log_level, Level.OFF);
		this.default_user_log_level = convertToLevel(default_user_log_level, Level.OFF);
		this.system_mirror_to_log4j_level = convertToLevel(system_mirror_to_log4j_level, Level.OFF);;
		_logger.log(Level.DEBUG, "LoggingService config set to: t-" + default_time_field + " s-" +default_system_log_level + " u-" + default_user_log_level + " l-" + system_mirror_to_log4j_level);
	}
	
	/**
	 * @param default_system_log_level2
	 * @param off
	 * @return
	 */
	private Level convertToLevel(String string_level, Level fallback) {
		try {
			return Level.valueOf(string_level);
		} catch (Exception ex) {
			return fallback;
		}
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
	/**
	 * If true, sends an additional message to log4j, false does nothing.
	 * @return
	 */
	public Level system_mirror_to_log4j_level() { return this.system_mirror_to_log4j_level; }
}
