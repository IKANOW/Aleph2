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

import java.io.Serializable;
import java.util.List;
import java.util.Map;


/**
 * Immutable object describing the data from this bucket
 * @author acp
 *
 */
public class DataSchemaBean implements Serializable {
	private static final long serialVersionUID = 4176875714094668591L;

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
	public static class StorageSchemaBean implements Serializable {
		private static final long serialVersionUID = 6846462609660288814L;
		protected StorageSchemaBean() {}
		
		/** Covers the generic storage parameters for each of the raw/json/processed cases
		 * @author Alex
		 */
		static public class StorageSubSchemaBean implements Serializable {
			private static final long serialVersionUID = 867619105821121075L;
			protected StorageSubSchemaBean() {}
			
			public enum TimeSourcePolicy { clock_time, batch, record };
			
			/** User constructor
			 */
			public StorageSubSchemaBean(
					final Boolean enabled,
					final String grouping_time_period,
					final TimeSourcePolicy grouping_time_policy,
					final String exist_age_max,
					final String codec,
					final WriteSettings target_write_settings
					) 
			{
				this.enabled = enabled;
				this.grouping_time_period = grouping_time_period;
				this.grouping_time_policy =  grouping_time_policy;
				this.exist_age_max = exist_age_max;
				this.codec = codec;
				this.target_write_settings = target_write_settings;
			}
			
			/** Describes if the archive service is used for this storage context (Raw/processed/json) in this bucket
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
			/** Defaults to "batch": if time field specified (from temporal schema), then groups all data object by the first timestamp in the write batch,
			 *  if "clock_time" then just uses the current time to group them, if "record" (not always supported) then each record is written to the correct grouping based
			 *  on its timestamp. Uses "clock_time" if no temporal schema is specified.
			 * @return
			 */
			public TimeSourcePolicy grouping_time_policy() {
				return grouping_time_policy;
			}
			/** A string describing the age at which documents in this bucket are deleted
			 * (eg "10 days", "864000", "5 years")
			 * @return the exist_age_max
			 */
			public String exist_age_max() {
				return exist_age_max;
			}
			/** Returns the compression/encoding codec to be used for this storage context, defaults to plain
			 *  Currently supported: gz/gzip, sz/snappy, fr.sz/snappy_framed 
			 * @return
			 */
			public String codec() {
				return codec;
			}
			
			/** The write settings for this sub-storage
			 * @return
			 */
			public WriteSettings target_write_settings() {
				return target_write_settings;
			}			
			
			private Boolean enabled;
			private String grouping_time_period;
			private TimeSourcePolicy grouping_time_policy;
			private String exist_age_max;
			private String codec;			
			private WriteSettings target_write_settings;
		}
		
		/** User constructor
		 */
		public StorageSchemaBean(final Boolean enabled, 
				final String service_name,
				final StorageSubSchemaBean raw,
				final StorageSubSchemaBean json,
				final StorageSubSchemaBean processed,
				final Map<String, Object> technology_override_schema) {
			this.enabled = enabled;
			this.service_name = service_name;
			this.raw = raw;
			this.json = json;
			this.processed = processed;
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
		
		/** The storage sub schema for raw storage (eg the untouched underlying data)
		 * @return The storage sub schema for raw storage (eg the untouched underlying data)
		 */
		public StorageSubSchemaBean raw() { return raw; }
		/** The storage sub schema for json storage (eg the input data has been serialized into JSON but not enriched)
		 * @return The storage sub schema for json storage (eg the input data has been serialized into JSON but not enriched)
		 */
		public StorageSubSchemaBean json() { return json; }
		/** The JSON data post enrichment, eg a mirror of what's in the document/search index DB
		 * @return The JSON data post enrichment, eg a mirror of what's in the document/search index DB
		 */
		public StorageSubSchemaBean processed() { return processed; }
		
		
		/** Technology-specific settings for this schema - see the specific service implementation for details 
		 * USE WITH CAUTION
		 * @return the technology_override_schema
		 */
		public Map<String, Object> technology_override_schema() {
			return technology_override_schema;
		}
		private Boolean enabled;
		private String service_name;
		private StorageSubSchemaBean raw;
		private StorageSubSchemaBean json;
		private StorageSubSchemaBean processed;
		private Map<String, Object> technology_override_schema;
	}
	/** Per bucket schema for the Document Service
	 * @author acp
	 *
	 */
	public static class DocumentSchemaBean implements Serializable {
		private static final long serialVersionUID = 6407137348665175660L;
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
	public static class SearchIndexSchemaBean implements Serializable {
		private static final long serialVersionUID = -1023079126153099337L;
		protected SearchIndexSchemaBean() {}
		
		/** User constructor
		 */
		public SearchIndexSchemaBean(final Boolean enabled,
				final WriteSettings target_write_settings,
				final Long target_index_size_mb,
				final String service_name,				
				final Map<String, Object> technology_override_schema) {
			super();
			this.enabled = enabled;
			this.target_write_settings = target_write_settings;
			this.target_index_size_mb = target_index_size_mb;
			this.service_name = service_name;
			this.technology_override_schema = technology_override_schema;
		}
		/** Describes if the search index service is used for this bucket
		 * @return the enabled
		 */
		public Boolean enabled() {
			return enabled;
		}
		
		/** (OPTIONAL) A user preference for the various write settings (eg number of threads, flush size)
		 * @return the user preference for the various write settings (eg number of threads, flush size)
		 */
		public WriteSettings target_write_settings() {
			return target_write_settings;
		}
		
		/** (OPTIONAL) A user preference for the maximum size of the index files generated (in Megabytes)
		 * @return the user preference for the maximum size of the index files generated (in Megabytes)
		 */
		public Long target_index_size_mb() {
			return target_index_size_mb;
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
		private WriteSettings target_write_settings;
		private Long target_index_size_mb;
		private String service_name;
		private Map<String, Object> technology_override_schema;
	}
	/** Per bucket schema for the Columnar Service
	 * @author acp
	 */
	public static class ColumnarSchemaBean implements Serializable {
		private static final long serialVersionUID = -6651815017077463900L;
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
	public static class TemporalSchemaBean implements Serializable {
		private static final long serialVersionUID = -8025554290903106836L;
		protected TemporalSchemaBean() {}
		
		/** User constructor
		 */
		public TemporalSchemaBean(Boolean enabled, 
				String service_name,
				String grouping_time_period,
				String hot_age_max, String warm_age_max, String cold_age_max,
				String exist_age_max,
				String time_field,				
				Map<String, Object> technology_override_schema) {
			this.enabled = enabled;
			this.service_name = service_name;
			this.grouping_time_period = grouping_time_period;
			this.hot_age_max = hot_age_max;
			this.warm_age_max = warm_age_max;
			this.cold_age_max = cold_age_max;
			this.exist_age_max = exist_age_max;
			this.time_field = time_field;
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
		/** The field in the data object that determines the time of the object
		 * @return the time_field
		 */
		public String time_field() {
			return time_field;
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
		private String time_field;
		private Map<String, Object> technology_override_schema;
	}
	/** Per bucket schema for the Geospatial Service
	 * @author acp
	 */
	public static class GeospatialSchemaBean implements Serializable {
		private static final long serialVersionUID = -3380350300379967374L;
		//TODO (ALEPH-16): define an initial set of geo-spatial schema
		//private Boolean enabled;
		//private String service_name;
		//private Map<String, Object> technology_override_schema;
	}
	/** Per bucket schema for the Graph DB Service
	 * @author acp
	 */
	public static class GraphSchemaBean implements Serializable {
		private static final long serialVersionUID = -824592579880124213L;
		//TODO (ALEPH-15): define an initial set of graph schema 
		// (eg options: 1] use annotations, 2] link on specified field pairs within object or fields across object, 3] build 2-hop via objects) 
		//private Boolean enabled;
		//private String service_name;
		//private Map<String, Object> technology_override_schema;
	}

	/** Per bucket schema for the Data Warehouse service 
	 * @author acp
	 */
	public static class DataWarehouseSchemaBean implements Serializable {
		private static final long serialVersionUID = -5234936234777519175L;
		//TODO (ALEPH-17): "sql" (hive) view of the data
		//config: map JSON to sql fields ie allow generation of serde
		//also maps buckets to database/table format
		//private Boolean enabled;
		//private String service_name;
		//private Map<String, Object> technology_override_schema;
	}
	
	////////////////////////////////////////////////////////////////////////////////
	
	/** A class shared across multiple schema (currently: search_index_schema) that contains somewhat generic write options
	 * @author Alex
	 */
	public static class WriteSettings implements Serializable {
		private static final long serialVersionUID = -5900584828103066696L;		
		protected WriteSettings() {}
		
		/** User c'tor
		 * @param batch_max_objects
		 * @param batch_max_size_kb
		 * @param batch_flush_interval
		 * @param target_write_concurrency
		 */
		public WriteSettings(final Integer batch_max_objects, final Long batch_max_size_kb, final Integer batch_flush_interval, final Integer target_write_concurrency)
		{
			this.batch_max_objects = batch_max_objects;
			this.batch_max_size_kb = batch_max_size_kb;
			this.batch_flush_interval = batch_flush_interval;
			this.target_write_concurrency = target_write_concurrency;
		}
		
		/** (OPTIONAL) When writing data out in batches, the (ideal) max number of objects per batch write
		 * @return the (ideal) max number of objects per batch write
		 */
		public Integer batch_max_objects() {
			return batch_max_objects;
		}
		
		/** (OPTIONAL) When writing data out in batches, the (ideal) max size per batch write (in KB)
		 * @return the (ideal) max size per batch write (in KB)
		 */
		public Long batch_max_size_kb() {
			return batch_max_size_kb;
		}
		
		/** (OPTIONAL) When writing data out in batches, the (ideal) max time between batch writes (in seconds)
		 * @return the (ideal) max time between batch writes (in seconds)
		 */
		public Integer batch_flush_interval() {
			return batch_flush_interval;
		}
		
		/** (OPTIONAL) A user preference for the number of threads that will be used to write data
		 * @return the user preference for the number of threads that will be used to write data
		 */
		public Integer target_write_concurrency() {
			return target_write_concurrency;
		}
		private Integer target_write_concurrency;
		private Integer batch_max_objects;
		private Long batch_max_size_kb;
		private Integer batch_flush_interval;
	}
}
