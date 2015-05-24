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
package com.ikanow.aleph2.management_db.utils;

import com.google.common.collect.ImmutableMap;

/** Core Management DB errors
 * @author acp
 */
public class ManagementDbErrorUtils extends com.ikanow.aleph2.data_model.utils.ErrorUtils {

	// Simple field rules
	
	public static final String BUCKET_NO_ACCESS_RIGHTS = "Bucket {0} has no access_rights field";
	public static final String BUCKET_NO_OWNER_ID = "Bucket {0} has no owner_id field";
	public static final String BUCKET_NO_CREATED = "Bucket {0} has no created field";
	public static final String BUCKET_NO_MODIFIED = "Bucket {0} has no modified field";
	public static final String BUCKET_NO_DISPLAY_NAME = "Bucket {0} has no display_name field";
	public static final String BUCKET_NO_FULL_NAME = "Bucket {0} has no full_name field";	

	public static final ImmutableMap<String, String> ERROR_MAP = ImmutableMap.<String, String>builder()
			.put("access_rights", BUCKET_NO_ACCESS_RIGHTS)
			.put("owner_id", BUCKET_NO_ACCESS_RIGHTS)
			.put("created", BUCKET_NO_ACCESS_RIGHTS)
			.put("modified", BUCKET_NO_ACCESS_RIGHTS)
			.put("display_name", BUCKET_NO_ACCESS_RIGHTS)
			.put("full_name", BUCKET_NO_ACCESS_RIGHTS)
			.build();

	// More complex field rules
	
	// Other rules:
	
	// - if has enrichment then must have harvest_technology_name_or_id 
	// - if has harvest_technology_name_or_id and then harvest_configs
	// - if has enrichment then must have master_enrichment_type
	// - if master_enrichment_type == batch/both then must have either batch_enrichment_configs or batch_enrichment_topology
	// - if master_enrichment_type == streaming/both then must have either streaming_enrichment_configs or streaming_enrichment_topology

	public static final String ENRICHMENT_BUT_NO_HARVEST_TECH = "Bucket {0} has enrichment but no harvest_technology_name_or_id field";
	public static final String HARVEST_BUT_NO_HARVEST_CONFIG = "Bucket {0} has harvest but no harvest configuration elements";
	public static final String ENRICHMENT_BUT_NO_MASTER_ENRICHMENT_TYPE = "Bucket {0} has enrichment but no master_enrichment_type field";
	public static final String BATCH_ENRICHMENT_NO_CONFIGS = "Bucket {0} has batch enrichment but neither batch_enrichment_configs or batch_enrichment_topology";
	public static final String STREAMING_ENRICHMENT_NO_CONFIGS = "Bucket {0} has streaming enrichment but neither streaming_enrichment_configs or streaming_enrichment_topology";
	
	// Embedded object field rules
	
	// - each data schema must: either be enabled or disabled
	// - each harvest config must be: either be enabled or disabled
	// - each enrichment config must be: either be enabled or disabled, have at least 1 lib id or name

	public static final String INVALID_DATA_SCHEMA_ELEMENTS = "Bucket {0}, data schema {1} must be either enabled or disabled";
	public static final String INVALID_HARVEST_CONFIG_ELEMENTS = "Bucket {0}, harvest config {1} must be either enabled or disabled";
	public static final String INVALID_ENRICHMENT_CONFIG_ELEMENTS = "Bucket {0}, enrichment config {1} must be either enabled or disabled";
	public static final String INVALID_ENRICHMENT_CONFIG_ELEMENTS_NO_LIBS = "Bucket {0}, enrichment config {1} must contain >= library names/ids";
	
	// Multi bucket rules
	
	// - if a multi bucket than cannot have any of: enrichment or harvest
	// - multi-buckets cannot be nested

	public static final String MULTI_BUCKET_CANNOT_HARVEST = "Bucket {0} cannot be both a multi-bucket and a normal data bucket";
	public static final String MULTI_BUCKET_CANNOT_NESTED = "Bucket {0} - multi bucket(s) {1} cannot be nested";
	
	// Security validation:
	
	// - if a multi bucket, then must have access rights over multi buckets
	// - all enrichment or harvest modules or technologies - owner id must have access rights
	// - if an alias, then XXX 
	// - there cannot be any buckets higher up the file path that XXX
	// - there cannot be any buckets lower down the file path that XXX

	public static final String MULTI_BUCKET_AUTHORIZATION_ERROR = "Multi-bucket {0}: user {1} does not have access to these buckets: {2}";
	public static final String LIBRARY_AUTHORIZATION_ERROR = "Bucket {0}: user {1} does not have access to these libraries: {3}";
	//TODO: the other rules
}
