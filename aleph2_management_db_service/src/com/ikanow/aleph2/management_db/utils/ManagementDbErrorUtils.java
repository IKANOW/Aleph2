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
 *******************************************************************************/
package com.ikanow.aleph2.management_db.utils;

import com.google.common.collect.ImmutableMap;

/** Core Management DB errors
 * @author acp
 */
public class ManagementDbErrorUtils extends com.ikanow.aleph2.data_model.utils.ErrorUtils {

	///////////////////////////////////////////////////////////////////////////////////////////
	
	// BUSES
	
	public static final String NO_SUCH_MESSAGE_BUS = "No such message bus: {0}";
	
	///////////////////////////////////////////////////////////////////////////////////////////
	
	// BUCKET CREATION/UPDATES
	
	public static final String BUCKET_CANNOT_BE_CREATED_WITHOUT_BUCKET_STATUS = "Bucket {0} does not have a corresponding bucket status - this should be created first";	
	public static final String NO_DATA_IMPORT_MANAGERS_STARTED_SUSPENDED = "Bucket {0} was created, but no data import managers were available to handle it, so it was created in suspended mode";
	public static final String DELETE_TOUCH_FILE_PRESENT = "Bucket {0} was recently deleted and has not yet been cleaned up";
	public static final String BUCKET_FULL_NAME_UNIQUENESS = "Bucket {0} already exists, choose another full_name";
	public static final String BUCKET_FULL_NAME_FORMAT_ERROR = "Bucket {0} should have the format /path/to/here (as deep as desired) with leading /, no trailing / and no '//'s or '..'s or '.'s";
	public static final String BUCKET_FULL_NAME_RESERVED_ERROR = "Bucket {0} cannot start with \"/aleph2_\" - these are reserved for system use";
	
	// Simple field rules
	
	public static final String BUCKET_NO_ID_FIELD = "Bucket {0} has no (non-empty) _id field";
	public static final String BUCKET_NO_OWNER_ID = "Bucket {0} has no (non-empty) owner_id field";
	public static final String BUCKET_NO_CREATED = "Bucket {0} has no (non-empty) created field";
	public static final String BUCKET_NO_MODIFIED = "Bucket {0} has no (non-empty) modified field";
	public static final String BUCKET_NO_DISPLAY_NAME = "Bucket {0} has (non-empty) no display_name field";
	public static final String BUCKET_NO_FULL_NAME = "Bucket {0} has no (non-empty) full_name field";	

	public static final ImmutableMap<String, String> NEW_BUCKET_ERROR_MAP = ImmutableMap.<String, String>builder()
			.put("_id", BUCKET_NO_ID_FIELD)
			.put("owner_id", BUCKET_NO_OWNER_ID)
			.put("created", BUCKET_NO_CREATED)
			.put("modified", BUCKET_NO_MODIFIED)
			.put("display_name", BUCKET_NO_DISPLAY_NAME)
			.put("full_name", BUCKET_NO_FULL_NAME)
			.build();

	// More complex field rules
	
	public static final String BUCKET_INVALID_TIME = "Bucket {0}, field {1} has time-related error: {2}";
	
	// Other rules:
	
	// - if has enrichment then must have harvest_technology_name_or_id 
	// - if has harvest_technology_name_or_id then must have harvest_configs
	// - if has enrichment then must have master_enrichment_type
	// - if master_enrichment_type == batch/both then must have either batch_enrichment_configs or batch_enrichment_topology
	// - if master_enrichment_type == streaming/both then must have either streaming_enrichment_configs or streaming_enrichment_topology

	public static final String STREAMING_AND_BATCH_NOT_SUPPORTED = "Bucket {0}: streaming_and_batch master_enrichment_type or analytic_thread.jobs.analytic_type not currently supported";
	public static final String FIELD_MUST_NOT_HAVE_ZERO_LENGTH = "Bucket {0} has a zero-length field: {1}";
	public static final String ENRICHMENT_BUT_NO_HARVEST_TECH = "Bucket {0} has enrichment but no harvest_technology_name_or_id field";
	public static final String HARVEST_BUT_NO_HARVEST_CONFIG = "Bucket {0} has harvest but no harvest configuration elements";
	public static final String ENRICHMENT_BUT_NO_MASTER_ENRICHMENT_TYPE = "Bucket {0} has enrichment but no master_enrichment_type field";
	public static final String BATCH_ENRICHMENT_NO_CONFIGS = "Bucket {0} has batch enrichment but neither batch_enrichment_configs or batch_enrichment_topology";
	public static final String STREAMING_ENRICHMENT_NO_CONFIGS = "Bucket {0} has streaming enrichment but neither streaming_enrichment_configs or streaming_enrichment_topology";
	
	// Embedded object field rules
	
	// - each data schema must: either be enabled or disabled
	// - each harvest config must be: either be enabled or disabled
	// - each enrichment config must be: either be enabled or disabled, have at least 1 lib id or name

	public static final String INVALID_ENRICHMENT_CONFIG_ELEMENTS_NO_LIBS = "Bucket {0}, enrichment config {1} must contain >= 1 library names/ids";
	
	// Multi bucket rules
	
	// - if a multi bucket than cannot have any of: enrichment or harvest
	// - multi-buckets cannot be nested

	public static final String MULTI_BUCKET_CANNOT_HARVEST = "Bucket {0} cannot be both a multi-bucket and a normal data bucket";
	public static final String MULTI_BUCKET_CANNOT_NESTED = "Bucket {0} - multi bucket(s) {1} cannot be nested";
	
	// Security validation:
	
	// - if a multi bucket, then must have access rights over multi buckets
	// - all enrichment or harvest modules or technologies - owner id must have access rights
	// - if an alias, then TODO (ALEPH-19) 
	// - there cannot be any buckets higher up the file path that TODO (ALEPH-19)
	// - there cannot be any buckets lower down the file path that TODO (ALEPH-19)

	public static final String MULTI_BUCKET_AUTHORIZATION_ERROR = "Multi-bucket {0}: user {1} does not have access to these buckets: {2}";
	public static final String LIBRARY_AUTHORIZATION_ERROR = "Bucket {0}: user {1} does not have access to these libraries: {3}";
	//TODO (ALEPH-19): the other rules
	
	// Schema validation
	
	public static final String SCHEMA_ENABLED_BUT_SERVICE_NOT_PRESENT = "Bucket {0}: schema {1} enabled but not present";
	
	///////////////////////////////////////////////////////////////////////////////////////////
	
	// BUCKET ANALYTICS
	
	public static final String ANALYTIC_JOB_ENRICHMENT_SPECIFIED = "Bucket {0} has an analytic job but also enrichment";
	public static final String ANALYTIC_JOB_MALFORMED_NAME = "Bucket {0} has an analytic job with missing or malformed name {1}";
	public static final String ANALYTIC_BUT_NO_ANALYTIC_TECH = "Bucket {0} has analytic job {1} with no analytic_technology_name_or_id field";
	public static final String ANALYTIC_JOB_MUST_HAVE_TYPE = "Bucket {0} has an analytic job with missing type or set to 'none': {1}";
	public static final String ANALYTIC_INPUT_MALFORMED_DATA_SERVICE = "Bucket {0} job {1} has a missing or malformed data_service: {2}, supported values = {3}";
	public static final String ANALYTIC_INPUT_MALFORMED_RESOURCE_ID = "Bucket {0} job {1} has a missing or malformed resource_name_or_id: {2}, supported patterns = {3}";
	public static final String ANALYTIC_INPUT_MALFORMED_DATE = "Bucket {0} job {1} has a malformed date (field {2}): {3}, error = {4}";
	public static final String ANALYTIC_OUTPUT_TRANSIENT_MISSING_FIELD = "Bucket {0} job {1} in transient mode missing field {2}";
	public static final String ANALYTIC_OUTPUT_TRANSIENT_ILLEGAL_FIELD = "Bucket {0} job {1} in transient mode not allowed field {2}";
	public static final String ANALYTIC_TRIGGER_MALFORMED_DATE = "Bucket {0} has a malformed trigger schedule: {1}, error = {2}";
	public static final String ANALYTIC_TRIGGER_ILLEGAL_COMBO = "Bucket {0} has illegal trigger formation: must have _either_ 'op' _or_ 'resource_name_or_id'; if 'op' then 'dependency_list' must be non-empty; if 'resource_name_or_id' then type must be specified";
	public static final String ANALYTIC_CUSTOM_TRIGGER_NOT_COMPLETE = "Bucket {0} has custom trigger specified but not custom analytic technology to execute it";
	public static final String ANALYTIC_INVALID_LOCK_TO_NODES = "Bucket {0}: pure analytic threads can only have 'lock_to_nodes': true if they contain a single analytic technology: {1}";
	
	///////////////////////////////////////////////////////////////////////////////////////////
	
	// BUCKET UPDATE
	
	public static final String BUCKET_UPDATE_ID_CHANGED = "Bucket {0}: can't update _id";
	public static final String BUCKET_UPDATE_FULLNAME_CHANGED = "Bucket {0}: can't _currently_ update full_name (was {1})";
	public static final String BUCKET_UPDATE_OWNERID_CHANGED = "Bucket {0}: can't _currently_ update owner_id (was {1})";	
	public static final String BUCKET_UPDATE_ILLEGAL_FIELD_CHANGED_ACTIVE = "Bucket {0}: can only change field {1} when bucket is suspended";
	public static final String BUCKET_UPDATE_MULTI_NODE_ENABLED_WARNING = "WARNING (Bucket {0}). field 'multi_node_enabled' is set, but only one node is handling the bucket. If you are switching to multi-node, note that this will not function correctly unless the source is suspended first. If you just have a one node system, then carry on!";
	
	///////////////////////////////////////////////////////////////////////////////////////////
	
	// BUCKET STATUS CREATION
	
	public static final String ILLEGAL_STATUS_CREATION = "Bucket Statuses must have _id/bucket_path/suspended fields set ({0})";
	
	///////////////////////////////////////////////////////////////////////////////////////////
	
	// BUCKET STATUS UPDATES

	public static final String ILLEGAL_UPDATE_COMMAND = "Only a subset of update commands are supported: {0} and {1} are not in this subset.";
	public static final String MISSING_STATUS_BEAN_OR_BUCKET = "One of the bucket or bucket status could not be found for {0}";
	
	///////////////////////////////////////////////////////////////////////////////////////////
	
	// BUCKET ACTION REPLIES

	public static final String SOME_NODES_TIMED_OUT = "Bucket {0}: >0 Data Import Managers (= {1}) timed out trying to handle this bucket, eg: {2}";
	public static final String SOME_NODES_REJECTED_BUCKET = "Bucket {0}: >0 Data Import Managers (= {1}) reject this bucket due to either node rules or the harvest/analytic engine rules, eg: {2}";
	public static final String ALL_NODES_REJECTED_BUCKET = "Bucket {0}: all active Data Import Managers rejected this bucket (engine couldn't run on the nodes, or filtered by node_rules)";
	public static final String ALL_NODES_TIMED_OUT = "Bucket {0}: all active Data Import Managers timed out trying to handle the bucket (or catastrophically failed)";
	public static final String ALL_NODES_TIMED_OUT_OR_REJECTED = "Bucket {0}: all active Data Import Managers timed out/rejected trying to handle the bucket (or catastrophically failed): timed out={1}, rejected={2} (engine couldn't run on the nodes, or filtered by node_rules)"; 
	public static final String NO_NODES_AVAILABLE = "This cluster has no active Data Import Managers";
	
}
