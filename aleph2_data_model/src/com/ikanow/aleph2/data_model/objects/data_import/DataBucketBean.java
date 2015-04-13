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
import java.util.Date;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import com.ikanow.aleph2.data_model.interfaces.data_access.ISecurityService;

public class DataBucketBean {

	////////////////////////////////////////
	
	// General information
	
	/** The management DB id of the bucket (unchangeable, unlike the bucket name)
	 * @return the bucket_id
	 */
	public String id() {
		return _id;
	}
	/** The bucket name - is enforced unique across the cluster
	 * @return the bucket_name
	 */
	public String name() {
		return name;
	}
	/** The management DB id of the bucket's owner
	 * @return the owner_id
	 */
	public String owner_id() {
		return owner_id;
	}
	/** The path relative to the AlephDB data root path where the 
	 * @return the parent_path
	 */
	public String parent_path() {
		return parent_path;
	}
	/** A list of bucket tags - for display/search only
	 * @return the bucket_aliases
	 */
	public List<String> tags() {
		return tags;
	}
	/** A map of SecurityService specific tokens that control read/write/admin access to the bucket
	 * @return the access_groups
	 */
	public Map<String, ISecurityService.AccessType> access_groups() {
		return access_groups;
	}
	/** If non-null, this bucket has been quarantined and will not be processed until the 
	 *  specified date (at which point quarantined_until will be set to null)
	 * @return the quarantined_until
	 */
	public Date quarantined_until() {
		return quarantined_until;
	}
	
	private String _id;	
	private String name;
	private String owner_id;
	private String parent_path;
	private List<String> tags;
	private Map<String, ISecurityService.AccessType> access_groups;
	private Date quarantined_until;
	
	////////////////////////////////////////
	
	// Multi buckets
	
	// If the bucket is a multi bucket all the attributes after this section are ignored
	// (+ the access_groups parameter is intersected with each bucket's access_groups parameter)

	/** A list of bucket aliases - each alias defines a multi-bucket (as an alternative to specifically creating one, which is also possible)
	 * @return the bucket_aliases
	 */
	public List<String> aliases() {
		return aliases;
	}
	/** A list of buckets in this multi-buckets
	 *  (Nested multi-buckets are currently not supported)
	 * @return multi_group_children
	 */
	public List<String> multi_bucket_children() {
		return multi_bucket_children;
	}
	private List<String> multi_bucket_children;
	private List<String> aliases;
	
	////////////////////////////////////////
	
	// Harvest specific information
	
	/** The name or id of the harvest technology associated with this bucket
	 * @return the harvest_technology_name_or_id
	 */
	public String harvest_technology_name_or_id() {
		return harvest_technology_name_or_id;
	}
	/** A list of configurations that are specific to the technology that 
	 *  describe the precise import functionality applied by the data import manager
	 * @return the harvest_configs
	 */
	public List<HarvestControlMetadataBean> harvest_configs() {
		return harvest_configs;
	}
	
	private String harvest_technology_name_or_id;
	
	/** Bean controlling a harvester configuration
	 * @author acp
	 */
	public static class HarvestControlMetadataBean {
		
		public HarvestControlMetadataBean() {}
		
		public HarvestControlMetadataBean(@NonNull String name, @NonNull Boolean enabled, @Nullable List<String> library_ids_or_names,
				@NonNull Map<String, Object> config) {
			this.name = name;
			this.enabled = enabled;
			this.library_ids_or_names = library_ids_or_names;
			this.config = config;
		}
		/** A name for this harvest config - must be unique within the list of harvest configs in the bucket
		 *  (currently used for search/display only)
		 * @return the enabled
		 */
		public String name() {
			return name;
		}
		/** Whether this harvest block is currently enabled
		 * @return the enabled
		 */
		public Boolean enabled() {
			return enabled;
		}
		/** A list of ids or names pointing to the JAR library that is maintained either in
		 * the global list or the per-bucket list (in both cases accessible via the Management DB or the harvest context)
		 * The libraries can be arbitrary JARs
		 * @return the library_ids_or_names
		 */
		public List<String> library_ids_or_names() {
			return library_ids_or_names;
		}
		/** The harvest-technology-specific configuration that controls the per-bucket import
		 * @return the config
		 */
		public Map<String, Object> config() {
			return config;
		}
		private String name;
		private Boolean enabled;
		private List<String> library_ids_or_names;
		private Map<String, Object> config;
	}	
	private List<HarvestControlMetadataBean> harvest_configs;
	
	////////////////////////////////////////
	
	// Enrichment specific information
	
	/** A list of enrichments that are applied to the bucket after ingestion
	 * @return the enrichment_configs
	 */
	public List<EnrichmentControlMetadataBean> enrichment_configs() {
		return enrichment_configs;
	}
	
	/** Bean controlling an enrichment configuration
	 * @author acp
	 */
	public static class EnrichmentControlMetadataBean {
		
		public EnrichmentControlMetadataBean() {}
		
		/** User constructor
		 */
		public EnrichmentControlMetadataBean(@NonNull String name,
				@Nullable List<String> dependencies, @NonNull Boolean enabled,
				@Nullable List<String> library_ids_or_names, @Nullable Map<String, Object> config) {
			super();
			this.name = name;
			this.dependencies = dependencies;
			this.enabled = enabled;
			this.library_ids_or_names = library_ids_or_names;
			this.config = config;
		}
		/** The name of the enrichment - must be unique within the list of enrichments in this bucket (used for search/display/dependencies)
		 * @return the name
		 */
		public String name() {
			return name;
		}
		/** Defines the dependency order of enrichment - this can be used by the framework to optimize runtime
		 * @return the dependencies
		 */
		public List<String> dependencies() {
			return dependencies;
		}
		/** Returns if this enrichment is currently enabled - implicitly disables all dependent enrichments
		 * @return the enabled
		 */
		public Boolean enabled() {
			return enabled;
		}
		/** A list of ids or names (within either the bucket or global library) of enrichment JARs to be
		 *  used as part of this enrichment. Exactly on of the JARs must be of type IEnrichmentLibraryModule  
		 * @return the library_ids_or_names
		 */
		public List<String> library_ids_or_names() {
			return library_ids_or_names;
		}
		/** The enrichment-module-specific configuration that controls the per-bucket enrichment
		 * @return the config
		 */
		public Map<String, Object> getConfig() {
			return config;
		}
		private String name;
		private List<String> dependencies;
		private Boolean enabled;
		private List<String> library_ids_or_names;
		private Map<String, Object> config;
	}	
	private List<EnrichmentControlMetadataBean> enrichment_configs;
	
	////////////////////////////////////////
	
	// Data schema
	
	/** The data schema applied to all objects ingested into this bucket
	 * @return the data_schema
	 */
	public DataSchemaBean data_schema() {
		return data_schema;
	}
	/** The root path of this bucket
	 * @return the bucket_location
	 */
	public String bucket_location() {
		return bucket_location;
	}
	/** If the bucket's data schema is in the process of being changed, then 
	 *  this value is the value to which it is being changed. Otherwise null
	 * @return the future_data_schema
	 */
	public DataSchemaBean future_data_schema() {
		return future_data_schema;
	}
	/** If the bucket is being moved on the disk, then this is the
	 *  path to which it is being moved
	 * @return the future_bucket_location
	 */
	public String future_bucket_location() {
		return future_bucket_location;
	}
	
	private DataSchemaBean data_schema;
	private String bucket_location;
	
	private DataSchemaBean future_data_schema; 
	private String future_bucket_location;
	
}
