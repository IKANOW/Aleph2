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

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;

/** A very important bean that describes how a bucket gets and stores its data
 * @author acp
 *
 */
public class DataBucketBean {

	protected DataBucketBean() {}
	
	////////////////////////////////////////
	
	// General information
	
	/** The management DB id of the bucket (unchangeable, unlike the bucket name)
	 * @return the _id
	 */
	public String _id() {
		return _id;
	}
	/** When this bucket was first created
	 * @return created date
	 */
	public Date created() {
		return created;
	}
	/** When this bucket was last modified
	 * @return modified date
	 */
	public Date modified() {
		return modified;
	}
	/** The bucket name as a path relative to the root path (starting with /) - is enforced unique across the cluster
	 * eg /project1/web_crawl_1, /project2/log_files/netflow_logs
	 * @return the full_name
	 */
	public String full_name() {
		return full_name;
	}
	/** A user editable display name for the bucket - no enforced uniqueness. Is auto-generated from the lowest path name if not specified.
	 * @return the display_name
	 */
	public String display_name() {
		return display_name;
	}
	/** A user editable description
	 * @return the description
	 */
	public String description() {
		return description;
	}
	/** The management DB id of the bucket's owner. Auto generated by the system.
	 * @return the owner_id
	 */
	public String owner_id() {
		return owner_id;
	}
	/** A list of bucket tags - for display/search only
	 * @return the bucket_aliases
	 */
	public Set<String> tags() {
		return tags;
	}
	/** A map of SecurityService specific tokens that control read/write/admin access to the bucket
	 * @return the access_groups
	 */
	public AuthorizationBean access_rights() {
		return access_rights;
	}
	/** The frequency with which the harvester is polled for this bucket.
	 * @return the poll_frequency in some human readable format ("every 5 minutes", "hourly", "3600" etc)
	 */
	public String poll_frequency() {
		return poll_frequency;
	}
	
	private String _id;	
	private Date created;
	private Date modified;
	private String full_name;
	private String display_name;
	private String description;
	private String owner_id;
	private Set<String> tags;
	private AuthorizationBean access_rights;
	private String poll_frequency;

	////////////////////////////////////////

	// Distribution across nodes in the cluster
	
	// There is quite a lot of flexibility in exactly where the harvester associated with a bucket can run, and on how many nodes
	
	/** Determines whether the harvester associated with this bucket should run on a single node, or multiples nodes
	 *  - regardless, will only run on nodes meeting the rules specified in node_list_rules()
	 * @return whether the harvester associated with this bucket should run on a single node, or multiples nodes
	 */
	public Boolean multi_node_enabled() {
		return multi_node_enabled;
	}
	/** Each item is either a glob or regex (format: /regex/flags) which is compared against the nodes' hostnames to determine whether the associated bucket can run on that hostname
	 * If a string is prefaced with the '-' then it is an exclude rule; if there is no prefix, or the prefix is '+' then it is an include rule. 
	 * @return the node list rules
	 */
	public List<String> node_list_rules() {
		return node_list_rules == null ? node_list_rules : Collections.unmodifiableList(node_list_rules);
	}
	
	private Boolean multi_node_enabled;
	private List<String> node_list_rules;
	
	////////////////////////////////////////
	
	// Multi buckets
	
	// If the bucket is a multi bucket all the attributes after this section are ignored
	// (+ the access_groups parameter is intersected with each bucket's access_groups parameter)

	/** A list of bucket aliases - each alias defines a multi-bucket (as an alternative to specifically creating one, which is also possible)
	 * @return the bucket_aliases
	 */
	public Set<String> aliases() {
		return aliases == null ? aliases : Collections.unmodifiableSet(aliases);
	}
	/** A list of buckets in this multi-buckets
	 *  (Nested multi-buckets are currently not supported)
	 * @return multi_group_children
	 */
	public Set<String> multi_bucket_children() {
		return multi_bucket_children == null ? multi_bucket_children : Collections.unmodifiableSet(multi_bucket_children);
	}
	private Set<String> multi_bucket_children;
	private Set<String> aliases;
	
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
		return harvest_configs == null ? null : Collections.unmodifiableList(harvest_configs);
	}
	
	private String harvest_technology_name_or_id;
	
	private List<HarvestControlMetadataBean> harvest_configs;
	
	////////////////////////////////////////
	
	// Enrichment specific information
	
	/** A list of enrichments that are applied to the bucket after ingestion via batch
	 * @return the enrichment_configs
	 */
	public List<EnrichmentControlMetadataBean> batch_enrichment_configs() {
		return batch_enrichment_configs == null ? null : Collections.unmodifiableList(batch_enrichment_configs);
	}
	/** Instead of a list of modules that are applied to the bucket by the core, it is possible
	 *  to pass a single enrichment topology that is applied - this gives the developers much more control
	 *  Currently there is no batch enrichment topology supported however, so this is a placeholder.
	 * @return the enrichment_configs
	 */
	public EnrichmentControlMetadataBean batch_enrichment_topology() {
		return batch_enrichment_topology;
	}

	/** A list of enrichments that are applied to the bucket after ingestion via streaming
	 * @return the enrichment_configs
	 */
	public List<EnrichmentControlMetadataBean> streaming_enrichment_configs() {
		return streaming_enrichment_configs == null ? null : Collections.unmodifiableList(streaming_enrichment_configs);
	}
	
	/** Instead of a list of modules that are applied to the bucket by the core, it is possible
	 *  to pass a single enrichment topology that is applied - this gives the developers much more control
	 *  Currently the only streaming topology supported is Apache STORM
	 * @return the enrichment_configs
	 */
	public EnrichmentControlMetadataBean streaming_enrichment_topology() {
		return streaming_enrichment_topology;
	}

	/** Only objects from the specified (potentially empty) enrichment route are stored
	 *  Objects from the other enrichment route are broadcast to listening analytic or access modules and if not
	 *  subscribed to are discarded.
	 * @return
	 */
	public MasterEnrichmentType master_enrichment_type() {
		return master_enrichment_type;
	}

	// You can supply a list of modules....
	private List<EnrichmentControlMetadataBean> batch_enrichment_configs;
	private List<EnrichmentControlMetadataBean> streaming_enrichment_configs;
	// ...Or a single topology
	private EnrichmentControlMetadataBean streaming_enrichment_topology;
	private EnrichmentControlMetadataBean batch_enrichment_topology;
	
	public enum MasterEnrichmentType { streaming, batch, streaming_and_batch }
	private MasterEnrichmentType master_enrichment_type;
	
	////////////////////////////////////////
	
	// Data schema
	
	/** The data schema applied to all objects ingested into this bucket
	 * @return the data_schema
	 */
	public DataSchemaBean data_schema() {
		return data_schema;
	}
	private DataSchemaBean data_schema;	
	
	public Map<String, String> data_locations() {
		return data_locations == null ? data_locations : Collections.unmodifiableMap(data_locations);
	}	
	private Map<String, String> data_locations;
	
}
