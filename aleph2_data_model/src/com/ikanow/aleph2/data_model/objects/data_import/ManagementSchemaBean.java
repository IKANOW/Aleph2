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
package com.ikanow.aleph2.data_model.objects.data_import;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;

import org.apache.logging.log4j.Level;

import com.google.common.collect.ImmutableMap;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.ColumnarSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.SearchIndexSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.StorageSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.TemporalSchemaBean;

/**
 * @author Burch
 *
 */
public class ManagementSchemaBean implements Serializable {

	private static final long serialVersionUID = -1325104245856394072L;
	
	/**
	 * Per bucket schema for the Logging Service
	 * @return the logging_schema
	 */
	public LoggingSchemaBean logging_schema() { return this.logging_schema; }
	/**
	 * Per bucket logging schema for the Temporal Service
	 * @return the temporal_schema
	 */
	public TemporalSchemaBean temporal_schema() { return this.temporal_schema; }
	/**
	 * Per bucket logging schema for the Storage Service
	 * @return the storage_schema
	 */
	public StorageSchemaBean storage_schema() { return this.storage_schema; }
	/**
	 * Per bucket logging schema for the Search Index Service
	 * @return the search_index_schema
	 */
	public SearchIndexSchemaBean search_index_schema() { return this.search_index_schema; }
	/**
	 * Per bucket logging schema for the Columnar Schema
	 * @return the columnar_schema
	 */
	public ColumnarSchemaBean columnar_schema() { return this.columnar_schema; }
	
	private LoggingSchemaBean logging_schema;
	private TemporalSchemaBean temporal_schema;
	private StorageSchemaBean storage_schema;
	private SearchIndexSchemaBean search_index_schema;
	private ColumnarSchemaBean columnar_schema;
		

	
	public static class LoggingSchemaBean implements Serializable {

		private static final long serialVersionUID = -1750400948595976387L;
		public static final String name = "logging_service";
		
		/** Describes if the columnar db service is used for this bucket
		 * @return the enabled
		 */
		public Boolean enabled() {
			return enabled;
		}
		/** (OPTIONAL) Enables a non-default service to be used for this schema
		 * @return the overriding service_name
		 */
		public String service_name() {
			return service_name;
		}
		/**
		 * Sets the default log level, defaults to ERROR if not specified
		 * @return
		 */
		public Level log_level() {
			return Optional.ofNullable(this.log_level).orElse(Level.ERROR);
		}
		/**
		 * (OPTIONAL) Set of service names and the log level you want them to output at
		 * @return
		 */
		public Map<String, Level> log_level_overrides() {
			return Optional.ofNullable(this.log_level_overrides).orElse(ImmutableMap.of());
		}
		
		private Boolean enabled;
		private String service_name;
		private Level log_level;
		private Map<String, Level> log_level_overrides;
		
	}
}
