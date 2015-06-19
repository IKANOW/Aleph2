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

	protected DataSchemaBean() {}
	
	/** User constructor
	 */
	public DataSchemaBean(final StorageSchemaBean storage_schema,
			final DocumentSchemaBean document_schema,
			final SearchIndexSchemaBean search_index_schema,
			final ColumnarSchemaBean columnar_schema,
			final TemporalSchemaBean temporal_schema,
			final GeospatialSchemaBean geospatial_schema,
			final GraphSchemaBean graph_schema,
			final DataWarehouseSchemaBean data_warehouse_schema
			)
	{
		this.storage_schema = storage_schema;
		this.document_schema = document_schema;
		this.search_index_schema = search_index_schema;
		this.columnar_schema = columnar_schema;
		this.temporal_schema = temporal_schema;
		this.geospatial_schema = geospatial_schema;
		this.graph_schema = graph_schema;
		this.data_warehouse_schema = data_warehouse_schema;
	}
	/** Per bucket schema for the Storage Service
	 * @return the archive_schema
	 */
	public StorageSchemaBean storage_schema() {
		return storage_schema;
	}
	/** Per bucket schema for the Document Service
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
	/** Per bucket schema for the Columnar Service
	 * @return the columnar_schema
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
	/** Per bucket schema for the Graph Service
	 * @return the graph_schema
	 */
	public GraphSchemaBean graph_schema() {
		return graph_schema;
	}
	/** Per bucket schema for the Data Warehouse Service
	 * @return the warehouse schema
	 */
	public DataWarehouseSchemaBean data_warehouse_schema() {
		return data_warehouse_schema;
	}
	
	private StorageSchemaBean storage_schema;
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
		
		protected StorageSchemaBean() {}
		
		/** User constructor
		 */
		public StorageSchemaBean(final Boolean enabled, 
				final String service_name,
				final String raw_grouping_time_period,
				final String raw_exist_age_max,
				final String json_grouping_time_period,
				final String json_exist_age_max,
				final String processed_grouping_time_period,
				final String processed_exist_age_max,
				final Map<String, Object> technology_override_schema) {
			this.enabled = enabled;
			this.service_name = service_name;
			this.raw_grouping_time_period = raw_grouping_time_period;
			this.raw_exist_age_max = raw_exist_age_max;
			this.json_grouping_time_period = json_grouping_time_period;
			this.json_exist_age_max = json_exist_age_max;
			this.processed_grouping_time_period = processed_grouping_time_period;
			this.processed_exist_age_max = processed_exist_age_max;
			this.technology_override_schema = technology_override_schema;
		}
		/** Describes if the archive service is used for this bucket
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
		/** A string describing the time period granularity for searches
		 *  (ie it will be possible to "search" over objects within each time period of this granularity by directory only) 
		 *  CURRENTLY SUPPORTED: "hour", "day", "week", "month", "year"
		 * @return the grouping_time_period
		 */
		public String raw_grouping_time_period() {
			return raw_grouping_time_period;
		}
		/** A string describing the age at which documents in this bucket are deleted
		 * (eg "10 days", "864000", "5 years")
		 * @return the exist_age_max
		 */
		public String raw_exist_age_max() {
			return raw_exist_age_max;
		}
		/** A string describing the time period granularity for searches
		 *  (ie it will be possible to "search" over objects within each time period of this granularity by directory only) 
		 *  CURRENTLY SUPPORTED: "hour", "day", "week", "month", "year"
		 * @return the grouping_time_period
		 */
		public String json_grouping_time_period() {
			return json_grouping_time_period;
		}
		/** A string describing the age at which documents in this bucket are deleted
		 * (eg "10 days", "864000", "5 years")
		 * @return the exist_age_max
		 */
		public String json_exist_age_max() {
			return json_exist_age_max;
		}
		/** A string describing the time period granularity for searches
		 *  (ie it will be possible to "search" over objects within each time period of this granularity by directory only) 
		 *  CURRENTLY SUPPORTED: "hour", "day", "week", "month", "year"
		 * @return the grouping_time_period
		 */
		public String processed_grouping_time_period() {
			return processed_grouping_time_period;
		}
		/** A string describing the age at which documents in this bucket are deleted
		 * (eg "10 days", "864000", "5 years")
		 * @return the exist_age_max
		 */
		public String processed_exist_age_max() {
			return processed_exist_age_max;
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
		private String raw_grouping_time_period;
		private String raw_exist_age_max;
		private String json_grouping_time_period;
		private String json_exist_age_max;
		private String processed_grouping_time_period;
		private String processed_exist_age_max;
		private Map<String, Object> technology_override_schema;
	}
	/** Per bucket schema for the Document Service
	 * @author acp
	 *
	 */
	public static class DocumentSchemaBean {
		
		protected DocumentSchemaBean() {}
		
		/** User constructor
		 */
		public DocumentSchemaBean(final Boolean enabled, 
				final String service_name,
				final Boolean deduplicate,
				final List<String> deduplication_fields,
				final Map<String, Object> technology_override_schema) {
			this.enabled = enabled;
			this.service_name = service_name;
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
		/** (OPTIONAL) Enables a non-default service to be used for this schema
		 * @return the overriding service_name
		 */
		public String service_name() {
			return service_name;
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
		
		protected SearchIndexSchemaBean() {}
		
		/** User constructor
		 */
		public SearchIndexSchemaBean(final Boolean enabled,
				final String service_name,
				final Map<String, Object> technology_override_schema) {
			super();
			this.enabled = enabled;
			this.service_name = service_name;
			this.technology_override_schema = technology_override_schema;
		}
		/** Describes if the search index service is used for this bucket
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
		
		//TODO (ALEPH-14): handle object format collisions - coexist/prevent/block
	}
	/** Per bucket schema for the Columnar Service
	 * @author acp
	 */
	public static class ColumnarSchemaBean {
		
		protected ColumnarSchemaBean() {}
		
		/** User constructor
		 */
		public ColumnarSchemaBean(final Boolean enabled,
				final String service_name,
				final List<String> field_include_list,
				final List<String> field_exclude_list, 
				final List<String> field_include_pattern_list,
				final List<String> field_exclude_pattern_list,
				final List<String> field_type_include_list,
				final List<String> field_type_exclude_list,
				final Map<String, Object> technology_override_schema) {
			this.enabled = enabled;
			this.service_name = service_name;
			this.field_include_list = field_include_list;
			this.field_exclude_list = field_exclude_list;
			this.field_include_pattern_list = field_include_pattern_list;
			this.field_exclude_pattern_list = field_exclude_pattern_list;
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
		/** (OPTIONAL) Enables a non-default service to be used for this schema
		 * @return the overriding service_name
		 */
		public String service_name() {
			return service_name;
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
		/** A set of patterns (this _might_ end up being technology dependent - ie regexes on some platforms, globs on others... for now they are globs), defines inclusion patterns
		 * @return the field_include_patterns
		 */
		public List<String> field_include_pattern_list() {
			return field_include_pattern_list;
		}
		/** A set of patterns (this _might_ end up being technology dependent - ie regexes on some platforms, globs on others... for now they are globs), defines exclusions patterns
		 * @return the field_exclude_patterns
		 */
		public List<String> field_exclude_pattern_list() {
			return field_exclude_pattern_list;
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
		private List<String> field_include_pattern_list;
		private List<String> field_exclude_pattern_list;
		private List<String> field_type_include_list;
		private List<String> field_type_exclude_list;
		private Map<String, Object> technology_override_schema;
	}
	/** Per bucket schema for the Temporal Service
	 * @author acp
	 */
	public static class TemporalSchemaBean {

		protected TemporalSchemaBean() {}
		
		/** User constructor
		 */
		public TemporalSchemaBean(Boolean enabled, 
				String service_name,
				String grouping_time_period,
				String hot_age_max, String warm_age_max, String cold_age_max,
				String exist_age_max,
				Map<String, Object> technology_override_schema) {
			this.enabled = enabled;
			this.service_name = service_name;
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
		/** (OPTIONAL) Enables a non-default service to be used for this schema
		 * @return the overriding service_name
		 */
		public String service_name() {
			return service_name;
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
		//TODO (ALEPH-16): define an initial set of geo-spatial schema
		//private Boolean enabled;
		//private String service_name;
		//private Map<String, Object> technology_override_schema;
	}
	/** Per bucket schema for the Graph DB Service
	 * @author acp
	 */
	public static class GraphSchemaBean {
		//TODO (ALEPH-15): define an initial set of graph schema 
		// (eg options: 1] use annotations, 2] link on specified field pairs within object or fields across object, 3] build 2-hop via objects) 
		//private Boolean enabled;
		//private String service_name;
		//private Map<String, Object> technology_override_schema;
	}

	/** Per bucket schema for the Data Warehouse service 
	 * @author acp
	 */
	public static class DataWarehouseSchemaBean {
		//TODO (ALEPH-17): "sql" (hive) view of the data
		//config: map JSON to sql fields ie allow generation of serde
		//also maps buckets to database/table format
		//private Boolean enabled;
		//private String service_name;
		//private Map<String, Object> technology_override_schema;
	}
	
}
