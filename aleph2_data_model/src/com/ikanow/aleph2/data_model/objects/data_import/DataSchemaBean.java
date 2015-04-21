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

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Immutable object describing the data from this bucket
 * @author acp
 *
 */
public class DataSchemaBean {

	public DataSchemaBean() {}
	
	/** User constructor
	 */
	public DataSchemaBean(@Nullable StorageSchemaBean storage_schema,
			@Nullable DocumentSchemaBean document_schema,
			@Nullable SearchIndexSchemaBean search_index_schema,
			@Nullable ColumnarSchemaBean columnar_schema,
			@Nullable TemporalSchemaBean temporal_schema,
			@Nullable GeospatialSchemaBean geospatial_schema,
			@Nullable GraphSchemaBean graph_schema,
			@Nullable DataWarehouseSchemaBean data_warehouse_schema
			)
	{
		this.archive_schema = storage_schema;
		this.document_schema = document_schema;
		this.search_index_schema = search_index_schema;
		this.columnar_schema = columnar_schema;
		this.temporal_schema = temporal_schema;
		this.geospatial_schema = geospatial_schema;
		this.graph_schema = graph_schema;
		this.data_warehouse_schema = data_warehouse_schema;
	}
	/** Per bucket schema for the Archive Service
	 * @return the archive_schema
	 */
	public StorageSchemaBean storage_schema() {
		return archive_schema;
	}
	/** Per bucket schema for the Object DB Service
	 * @return the object_schema
	 */
	public DocumentSchemaBean document_schema() {
		return document_schema;
	}
	/** Per bucket schema for the Search Index Service
	 * @return the search_index_schema
	 */
	public SearchIndexSchemaBean search_index_schema() {
		return search_index_schema;
	}
	/** Per bucket schema for the Columnar DB Service
	 * @return the columnar_db_schema
	 */
	public ColumnarSchemaBean columnar_schema() {
		return columnar_schema;
	}
	/** Per bucket schema for the Temporal Service
	 * @return the temporal_schema
	 */
	public TemporalSchemaBean temporal_schema() {
		return temporal_schema;
	}
	/** Per bucket schema for the Geospatial Service
	 * @return the geospatial_schema
	 */
	public GeospatialSchemaBean geospatial_schema() {
		return geospatial_schema;
	}
	/** Per bucket schema for the Graph DB Service
	 * @return the graph_db_schema
	 */
	public GraphSchemaBean graph_schema() {
		return graph_schema;
	}
	
	private StorageSchemaBean archive_schema;
	private DocumentSchemaBean document_schema;
	private SearchIndexSchemaBean search_index_schema;
	private ColumnarSchemaBean columnar_schema;
	private TemporalSchemaBean temporal_schema;
	private GeospatialSchemaBean geospatial_schema;
	private GraphSchemaBean graph_schema;
	private DataWarehouseSchemaBean data_warehouse_schema;
	
	/** Per bucket schema for the Archive Service
	 * @author acp
	 *
	 */
	public static class StorageSchemaBean {
		
		public StorageSchemaBean() {}
		
		/** User constructor
		 */
		public StorageSchemaBean(@NonNull Boolean enabled, @Nullable String grouping_time_period,
				@Nullable String exist_age_max,
				@Nullable Map<String, Object> technology_override_schema) {
			this.enabled = enabled;
			this.grouping_time_period = grouping_time_period;
			this.exist_age_max = exist_age_max;
			this.technology_override_schema = technology_override_schema;
		}
		/** Describes if the archive service is used for this bucket
		 * @return the enabled
		 */
		public Boolean enabled() {
			return enabled;
		}
		/** A string describing the time period granularity for searches
		 *  (ie it will be possible to "search" over objects within each time period of this granularity by directory only) 
		 *  CURRENTLY SUPPORTED: "hour", "day", "week", "month", "year"
		 * @return the grouping_time_period
		 */
		public String grouping_time_period() {
			return grouping_time_period;
		}
		/** A string describing the age at which documents in this bucket are deleted
		 * (eg "10 days", "864000", "5 years")
		 * @return the exist_age_max
		 */
		public String exist_age_max() {
			return exist_age_max;
		}
		/** Technology-specific settings for this schema - see the specific service implementation for details 
		 * USE WITH CAUTION
		 * @return the technology_override_schema
		 */
		public Map<String, Object> technology_override_schema() {
			return technology_override_schema;
		}
		private Boolean enabled;
		private String service_name;
		private String grouping_time_period;
		private String exist_age_max;
		private Map<String, Object> technology_override_schema;
	}
	/** Per bucket schema for the Document Service
	 * @author acp
	 *
	 */
	public static class DocumentSchemaBean {
		
		public DocumentSchemaBean() {}
		
		/** User constructor
		 */
		public DocumentSchemaBean(@NonNull Boolean enabled, @Nullable Boolean deduplicate,
				@Nullable List<String> deduplication_fields,
				@Nullable Map<String, Object> technology_override_schema) {
			this.enabled = enabled;
			this.deduplicate = deduplicate;
			this.deduplication_fields = deduplication_fields;
			this.technology_override_schema = technology_override_schema;
		}
		/** Describes if the document db service is used for this bucket
		 * @return the enabled
		 */
		public Boolean enabled() {
			return enabled;
		}
		/** If enabled, then deduplication-during-the-harvest-phase is engaged
		 *  The default field is "_id" (which is then by default generated from the object body, which can be computationally expensive)
		 * @return the deduplicate
		 */
		public Boolean deduplicate() {
			return deduplicate;
		}
		/** If deduplication is enabled then this ordered list of fields is the deduplication key (together with bucket)
		 * @return the deduplication_fields
		 */
		public List<String> deduplication_fields() {
			return deduplication_fields;
		}
		/** If deduplication is enabled then this ordered list of strings describes the scope of the deduplication, 
		 * ie the set of buckets and/or multi-buckets for which a matching set of fields is held.
		 * @return the deduplication_fields
		 */
		public List<String> deduplication_contexts() {
			return deduplication_contexts;
		}
		/** Technology-specific settings for this schema - see the specific service implementation for details 
		 * USE WITH CAUTION
		 * @return the technology_override_schema
		 */
		public Map<String, Object> technology_override_schema() {
			return technology_override_schema;
		}
		private Boolean enabled;
		private String service_name;
		private Boolean deduplicate;
		private List<String> deduplication_fields;
		private List<String> deduplication_contexts;
		private Map<String, Object> technology_override_schema;
	}
	/** Per bucket schema for the Search Index Service
	 * @author acp
	 */
	public static class SearchIndexSchemaBean {
		
		public SearchIndexSchemaBean() {}
		
		/** User constructor
		 */
		public SearchIndexSchemaBean(@NonNull Boolean enabled,
				@Nullable Map<String, Object> technology_override_schema) {
			super();
			this.enabled = enabled;
			this.technology_override_schema = technology_override_schema;
		}
		/** Describes if the search index service is used for this bucket
		 * @return the enabled
		 */
		public Boolean enabled() {
			return enabled;
		}
		/** Technology-specific settings for this schema - see the specific service implementation for details 
		 * USE WITH CAUTION
		 * @return the technology_override_schema
		 */
		public Map<String, Object> technology_override_schema() {
			return technology_override_schema;
		}
		private Boolean enabled;
		private String service_name;
		private Map<String, Object> technology_override_schema;
	}
	/** Per bucket schema for the Columnar Service
	 * @author acp
	 */
	public static class ColumnarSchemaBean {
		
		public ColumnarSchemaBean() {}
		
		/** User constructor
		 */
		public ColumnarSchemaBean(@NonNull Boolean enabled,
				@Nullable List<String> field_include_list,
				@Nullable List<String> field_exclude_list, String field_include_regex,
				@Nullable String field_exclude_regex,
				@Nullable List<String> field_type_include_list,
				@Nullable List<String> field_type_exclude_list,
				@Nullable Map<String, Object> technology_override_schema) {
			this.enabled = enabled;
			this.field_include_list = field_include_list;
			this.field_exclude_list = field_exclude_list;
			this.field_include_regex = field_include_regex;
			this.field_exclude_regex = field_exclude_regex;
			this.field_type_include_list = field_type_include_list;
			this.field_type_exclude_list = field_type_exclude_list;
			this.technology_override_schema = technology_override_schema;
		}
		/** Describes if the columnar db service is used for this bucket
		 * @return the enabled
		 */
		public Boolean enabled() {
			return enabled;
		}
		/** A list of fields to index as a "column"
		 * @return the field_include_list
		 */
		public List<String> field_include_list() {
			return field_include_list;
		}
		/** A list of fields to exclude (from other include lists/patterns) as a "column"
		 * @return the field_exclude_list
		 */
		public List<String> field_exclude_list() {
			return field_exclude_list;
		}
		/** A regular expression to select fields to index as a "column"
		 * @return the field_include_regex
		 */
		public String field_include_regex() {
			return field_include_regex;
		}
		/** A regular expression to select fields to exclude as a "column"
		 * @return the field_exclude_regex
		 */
		public String field_exclude_regex() {
			return field_exclude_regex;
		}
		/** A list of field types to decide which fields to index as a "column"
		 * @return the field_type_include_list
		 */
		public List<String> field_type_include_list() {
			return field_type_include_list;
		}
		/** A list of field types to decide which fields to exclude as a "column"
		 * @return the field_type_exclude_list
		 */
		public List<String> field_type_exclude_list() {
			return field_type_exclude_list;
		}
		/** Technology-specific settings for this schema - see the specific service implementation for details 
		 * USE WITH CAUTION
		 * @return the technology_override_schema
		 */
		public Map<String, Object> technology_override_schema() {
			return technology_override_schema;
		}
		private Boolean enabled;
		private String service_name;
		private List<String> field_include_list;
		private List<String> field_exclude_list;
		private String field_include_regex;
		private String field_exclude_regex;
		private List<String> field_type_include_list;
		private List<String> field_type_exclude_list;
		private Map<String, Object> technology_override_schema;
	}
	/** Per bucket schema for the Temporal Service
	 * @author acp
	 */
	public static class TemporalSchemaBean {

		public TemporalSchemaBean() {}
		
		/** User constructor
		 */
		public TemporalSchemaBean(@NonNull Boolean enabled, @Nullable String grouping_time_period,
				@Nullable String hot_age_max, @Nullable String warm_age_max, @Nullable String cold_age_max,
				@Nullable String exist_age_max,
				@Nullable Map<String, Object> technology_override_schema) {
			this.enabled = enabled;
			this.grouping_time_period = grouping_time_period;
			this.hot_age_max = hot_age_max;
			this.warm_age_max = warm_age_max;
			this.cold_age_max = cold_age_max;
			this.exist_age_max = exist_age_max;
			this.technology_override_schema = technology_override_schema;
		}
		
		/** Describes if the columnar db service is used for this bucket
		 * @return the enabled
		 */
		public Boolean enabled() {
			return enabled;
		}
		/** A string describing the time period granularity for searches
		 *  (ie it will be possible to "search" more efficiently for objects in the same time period) 
		 *  CURRENTLY SUPPORTED: "hour", "day", "week", "month", "year"
		 * @return the grouping_time_period
		 */
		public String grouping_time_period() {
			return grouping_time_period;
		}
		/** The oldest object that is treated as very commonly accessed (speed vs resource usage = speed)
		 * (eg "10 days", "864000", "5 years")
		 * @return the hot_age_max
		 */
		public String hot_age_max() {
			return hot_age_max;
		}
		/** The oldest object that is treated as "typically" accessed (speed vs resource usage = compromise)
		 * (eg "10 days", "864000", "5 years")
		 * @return the warm_age_max
		 */
		public String warm_age_max() {
			return warm_age_max;
		}
		/** The oldest object that is treated as "infrequently" accessed (speed vs resource usage = resource usage)
		 * (eg "10 days", "864000", "5 years")
		 * @return the warm_age_max
		 */
		public String cold_age_max() {
			return cold_age_max;
		}
		/** The oldest object that is not deleted
		 * (eg "10 days", "864000", "5 years")
		 * @return the exist_age_max
		 */
		public String exist_age_max() {
			return exist_age_max;
		}
		/** Technology-specific settings for this schema - see the specific service implementation for details 
		 * USE WITH CAUTION
		 * @return the technology_override_schema
		 */
		public Map<String, Object> technology_override_schema() {
			return technology_override_schema;
		}
		private Boolean enabled;
		private String service_name;
		private String grouping_time_period;
		private String hot_age_max;
		private String warm_age_max;
		private String cold_age_max;
		private String exist_age_max;
		private Map<String, Object> technology_override_schema;
	}
	/** Per bucket schema for the Geospatial Service
	 * @author acp
	 */
	public static class GeospatialSchemaBean {
		//TODO define an initial set of geo-spatial schema
		//private Boolean enabled;
		//private String service_name;
		//private Map<String, Object> technology_override_schema;
	}
	/** Per bucket schema for the Graph DB Service
	 * @author acp
	 */
	public static class GraphSchemaBean {
		//TODO define an initial set of graph schema 
		// (eg options: 1] use annotations, 2] link on specified field pairs within object or fields across object, 3] build 2-hop via objects) 
		//private Boolean enabled;
		//private String service_name;
		//private Map<String, Object> technology_override_schema;
	}

	/** Per bucket schema for the Data Warehouse service 
	 * @author acp
	 */
	public static class DataWarehouseSchemaBean {
		//TODO "sql" (hive) view of the data
		//config: map JSON to sql fields ie allow generation of serde
		//also maps buckets to database/table format
		//private Boolean enabled;
		//private String service_name;
		//private Map<String, Object> technology_override_schema;
	}
	
}
