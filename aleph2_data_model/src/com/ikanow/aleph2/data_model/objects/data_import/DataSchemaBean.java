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
package com.ikanow.aleph2.data_model.objects.data_import;

import java.util.List;
import java.util.Map;

/**
 * Immutable object describing the data from this bucket
 * @author acp
 *
 */
public class DataSchemaBean {

	private ArchiveSchemaBean archive_schema;
	private ObjectDbSchemaBean object_schema;
	private SearchIndexSchemaBean search_index_schema;
	private ColumnarDbSchemaBean columnar_db_schema;
	private TemporalSchemaBean temporal_schema;
	private GeospatialSchemaBean geospatial_schema;
	private GraphDbSchemaBean graph_db_schema;
	
	public static class ArchiveSchemaBean {
		//TODO 
		private Boolean enabled;
		private String grouping_time_period;
		private String exist_age_max;
		private Map<String, Object> technology_override_schema;
	}
	public static class ObjectDbSchemaBean {
		//TODO 
		private Boolean enabled;
		private Boolean deduplicate;
		private List<String> deduplication_fields;
		private Map<String, Object> technology_override_schema;
	}
	public static class SearchIndexSchemaBean {
		//TODO 
		private Boolean enabled;
		private Map<String, Object> technology_override_schema;
	}
	public static class ColumnarDbSchemaBean {
		//TODO 
		private Boolean enabled;
		private List<String> field_include_list;
		private List<String> field_exclude_list;
		private String field_include_regex;
		private String field_exclude_regex;
		private List<String> field_type_include_list;
		private List<String> field_type_exclude_list;
		private Map<String, Object> technology_override_schema;
	}
	public static class TemporalSchemaBean {
		//TODO 
		private Boolean enabled;
		private String grouping_time_period;
		private String hot_age_max;
		private String warm_age_max;
		private String cold_age_max;
		private String exist_age_max;
		private Map<String, Object> technology_override_schema;
	}
	public static class GeospatialSchemaBean {
		//TODO 
		private Boolean enabled;
		private Map<String, Object> technology_override_schema;
	}
	public static class GraphDbSchemaBean {
		//TODO 
		private Boolean enabled;
		private Map<String, Object> technology_override_schema;
	}
	
}
