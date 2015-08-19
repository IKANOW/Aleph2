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
package com.ikanow.aleph2.data_model.objects.data_analytics;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;

/** The configuration of an individual job within a larger analytic thread
 * @author Alex
 */
public class AnalyticThreadJobBean implements Serializable {
	private static final long serialVersionUID = 8404866462750489233L;
	
	protected AnalyticThreadJobBean() {}
	
	/** User constructor
	 * @param name - The name of the job - optional but required if it is needed as a dependency
	 * @param enabled - Whether the job is currently enabled, defaults to true
	 * @param analytic_type - Whether the job is streaming (eg Storm), batch (eg Hadoop) , both ("streaming_and_batch", currently undefined), or "none" (currently undefined)
	 * @param analytic_technology_name_or_id - The name or the id of the analytic technology that will handle this job
	 * @param module_names_or_ids - An optional list of addition modules required on the classpath
	 * @param config - The analytic technology specific module configuration JSON
	 * @param node_list_rules - the node list rules
	 * @param multi_node_enabled - Determines whether this analytic job should run on a single node, or multiples nodes
	 * @param dependencies - The list of internal dependencies (ie "name" fields from the job list of this thread) that need to be satisfied before this job is run
	 * @param inputs - The list of input configurations for this job
	 * @param output - The configuration for the job's output
	 */
	public AnalyticThreadJobBean(final String name, final Boolean enabled, 
									final String analytic_technology_name_or_id, final List<String> module_names_or_ids, 
									final LinkedHashMap<String, Object> config,
									final DataBucketBean.MasterEnrichmentType analytic_type,
									final List<String> node_list_rules, final Boolean multi_node_enabled,
									final List<String> dependencies,
									final List<AnalyticThreadJobInputBean> inputs,
									final AnalyticThreadJobOutputBean output
									)
	{
		this.name = name;
		this.enabled = enabled;
		this.analytic_technology_name_or_id = analytic_technology_name_or_id;
		this.module_names_or_ids = module_names_or_ids;
		this.config = config;
		this.node_list_rules = node_list_rules;
		this.multi_node_enabled = multi_node_enabled;
		this.dependencies = dependencies;
		this.inputs = inputs;
		this.output = output;
	}
										
	////////////////////////////////////////
	
	// Basic attributes
	
	/** The name of the job - optional but required if it is needed as a dependency
	 * @return The name of the job - optional but required if it is needed as a dependency
	 */
	public String name() { return name; }
	
	/** Whether the job is currently enabled (if not enabled cannot be used as a dependency anywhere), defaults to true
	 * @return Whether the job is currently enabled, defaults to true
	 */
	public Boolean enabled() { return enabled; }
	
	/** Whether the job is streaming (eg Storm), batch (eg Hadoop) , both ("streaming_and_batch", currently undefined), or "none" (currently undefined)
	 * @return streaming/batch/streaming_and_batch/none
	 */
	public DataBucketBean.MasterEnrichmentType analytic_type() { return analytic_type; }
	
	private String name;
	private Boolean enabled;	
	private DataBucketBean.MasterEnrichmentType analytic_type;		
	
	////////////////////////////////////////
	
	// Basic configuration
	
	/** The name or the id of the analytic technology that will handle this job
	 * @return The name or the id of the analytic technology that will handle this job
	 */
	public String analytic_technology_name_or_id() { return analytic_technology_name_or_id; }
	
	/** A list of addition modules required on the classpath
	 * @return An optional list of addition modules required on the classpath
	 */
	public List<String> module_names_or_ids() { return null == module_names_or_ids ? module_names_or_ids : Collections.unmodifiableList(module_names_or_ids); }
	
	/** The analytic technology specific module configuration JSON
	 * @return The analytic technology specific module configuration JSON
	 */
	public Map<String, Object> config() { return null == config ? config : Collections.unmodifiableMap(config); }
	
	private String analytic_technology_name_or_id;
	private List<String> module_names_or_ids;
	private LinkedHashMap<String, Object> config;	
	
	////////////////////////////////////////
	
	// Distribution
	
	/** Each item is either a glob or regex (format: /regex/flags) which is compared against the nodes' hostnames to determine whether the associated bucket can run on that hostname
	 * If a string is prefaced with the '-' then it is an exclude rule; if there is no prefix, or the prefix is '+' then it is an include rule. 
	 * @return the node list rules
	 */
	public List<String> node_list_rules() { return null == node_list_rules ? node_list_rules : Collections.unmodifiableList(node_list_rules); }
	
	/** Determines whether this analytic job should run on a single node, or multiples nodes
	 *  - regardless, will only run on nodes meeting the rules specified in node_list_rules()
	 *  If not set then defaults to single node
	 *  NOTE: that inherently distributed analytic technologies (eg Storm/Hadoop/Spark) are set to false, since they only need be started from a single node
	 *  This is for single-node technologies (eg a random external process) that you can "auto distribute" across multiple nodes (and feed via round-robin)
	 *  Therefore this will almost always be set to false
	 * @return whether this analytic job should run on a single node, or multiples nodes
	 */
	public Boolean multi_node_enabled() { return multi_node_enabled; }
	
	private List<String> node_list_rules;
	private Boolean multi_node_enabled;

	////////////////////////////////////////
	
	// Dependencies and inputs
	
	/** The list of internal dependencies (ie "name" fields from the job list of this thread) that need to be satisfied before this job is run (after a thread has been started - the thread is triggered by the top level triggers, not these job-specific ones)
	 * @return  a list of strings referring to "name" fields of other jobs
	 */
	public List<String> dependencies() { return null == dependencies ? dependencies : Collections.unmodifiableList(dependencies); }
	
	/** The list of input configurations for this job
	 * @return The list of input configurations for this job
	 */
	public List<AnalyticThreadJobInputBean> inputs() { return null == inputs ? inputs : Collections.unmodifiableList(inputs); }
	
	/** Defines an input to a job (each job can have multiple inputs)
	 * @author Alex
	 */
	public static class AnalyticThreadJobInputBean implements Serializable {
		private static final long serialVersionUID = -1406441626711464795L;
		
		protected AnalyticThreadJobInputBean() {}
		
		/** User c'tor
		 * @param resource_name_or_id - The resource name of the input (bucket path, bucket queue, or internal job "name")
		 * @param data_service - For bucket data access, the data service from which to retrieve the data
		 * @param filter - the filter to apply to the incoming data (CRUD or file-specific)
		 * @param config - Generic configuration for input data
		 */
		protected AnalyticThreadJobInputBean(final String resource_name_or_id, final String data_service, 
											final LinkedHashMap<String, Object> filter,
											final AnalyticThreadJobInputConfigBean config 
											)
		{
			this.resource_name_or_id = resource_name_or_id;
			this.data_service = data_service;
			this.filter = filter;
			this.config = config;
		}
		
		/** The resource name of the input: one of the bucket path/id, the bucket queue, the "name" of the internal dependency, an external file path, etc 
		 * @return The resource name of the input
		 */
		public String resource_name_or_id() { return resource_name_or_id; }
		
		/** For bucket data access, the data service from which to retrieve the data (search_index_service, storage_service, etc)
		 * @return For bucket data access, the data service from which to retrieve the data
		 */
		public String data_service() { return data_service; } 
		
		/** For bucket data access, an optional CRUD filter to apply to incoming data; for file access will be a file-specific JSON object
		 * @return the filter to apply to the incoming data (CRUD or file-specific)
		 */
		public Map<String, Object> filter() { return null == filter ? filter : Collections.unmodifiableMap(filter); }
		
		/** Generic configuration for input data
		 * @return Generic configuration for input data
		 */
		public AnalyticThreadJobInputConfigBean config() { return config; }
		
		private String resource_name_or_id; 
		private String data_service;	
		private LinkedHashMap<String, Object> filter;
		private AnalyticThreadJobInputConfigBean config;
		
		/** Defines generic properties of the input (where applicable) - eg batching/streaming 
		 * @author Alex
		 */
		public static class AnalyticThreadJobInputConfigBean implements Serializable {
			private static final long serialVersionUID = -5506063365294565996L;
			
			protected AnalyticThreadJobInputConfigBean() {}
			
			/** User c'tor
			 * @param new_data_only - Whether only new data is served to the job each time it runs
			 * @param self_join - Whether the existing output data from this job's previous run should be used as an extra input to the new job
			 * @param time_min - human readable string that applies an "oldest" time filter to the input data
			 * @param time_max -  human readable string that applies a "newest" time filter to the input data
			 * @param timed_batch_ms - When converting from streaming data to batch data, the max time period over which to collect batches
			 * @param size_batch_records - When converting from streaming data to batch data, the max number of records in each batch
			 * @param size_batch_kb - When converting from streaming data to batch data, the max size of each batch in KBytes of record JSON string
			 */
			public AnalyticThreadJobInputConfigBean(final Boolean new_data_only, final Boolean self_join, 
					final String time_min, final String time_max,
					final Long timed_batch_ms, final Long size_batch_records, final Long size_batch_kb)
			{
				this.new_data_only = new_data_only;
				this.self_join = self_join;
				this.time_min = time_min;
				this.time_max = time_max;
				this.timed_batch_ms = timed_batch_ms;
				this.size_batch_records = size_batch_records;
				this.size_batch_kb = size_batch_kb;
			}
			
			/** If true (defaults: false) then only new data is served to the job each time it runs (otherwise all data from the input matching the filter is served)
			 * @return Whether only new data is served to the job each time it runs
			 */
			public Boolean new_data_only() { return new_data_only; }
			
			/** If true (defaults: false) the existing output data from this job's previous run is used as an extra input to the new job
			 * @return Whether the existing output data from this job's previous run should be used as an extra input to the new job 
			 */
			public Boolean self_join() { return self_join; }
			
			/** An optional human readable string that applies a time filter (lower bound) to the input data
			 * @return - human readable string that applies an "oldest" time filter to the input data
			 */
			public String time_min() { return time_min; }
			
			/** An optional human readable string that applies a time filter (upper bound) to the input data
			 * @return - human readable string that applies a "newest" time filter to the input data
			 */
			public String time_max() { return time_max; }
			
			/** When converting from streaming data to batch data, the max time period over which to collect batches
			 * @return the max time period over which to collect batches
			 */
			public Long timed_batch_ms() { return timed_batch_ms; }
			
			/** When converting from streaming data to batch data, the max number of records in each batch
			 * @return the max number of records in each batch
			 */
			public Long size_batch_records() { return size_batch_records; }
			
			/** When converting from streaming data to batch data, the max size of each batch in KBytes of record JSON string
			 * @return the max size of each batch in KBytes of record JSON string
			 */
			public Long size_batch_kb() { return size_batch_kb; }
			
			private Boolean new_data_only;
			private Boolean self_join;
			private String time_min;
			private String time_max;
			private Long timed_batch_ms;
			private Long size_batch_records;
			private Long size_batch_kb;
		}
	}
	
	private List<String> dependencies;			
	private List<AnalyticThreadJobInputBean> inputs; 
	
	////////////////////////////////////////
	
	// Outputs
	
	/** The configuration for the job's output
	 * @return The configuration for the job's output
	 */
	public AnalyticThreadJobOutputBean output() { return output; }
	
	/** The configuration bean for the job's output
	 * @author Alex
	 */
	public static class AnalyticThreadJobOutputBean implements Serializable {
		private static final long serialVersionUID = 179619492596670425L;
		
		protected AnalyticThreadJobOutputBean() {}
		
		/** User c'tor
		 * @param preserve_existing_data - Whether to preserve existing data before adding the output of the new job
		 * @param is_transient - Whether the output data is written to a transient file store (default) or whether it's used as the output for this bucket (or a sub-bucket)
		 * @param sub_bucket_path - the optional sub-bucket to which to write bucket output data
		 * @param transient_type - the type (streaming/batch/both) of the transient output
		 */
		public AnalyticThreadJobOutputBean(final Boolean preserve_existing_data, final Boolean is_transient, 
				final String sub_bucket_path, final DataBucketBean.MasterEnrichmentType transient_type) {
			this.preserve_existing_data = preserve_existing_data;
			this.is_transient = is_transient;
			this.sub_bucket_path = sub_bucket_path;
			this.transient_type = transient_type;
		}
		
		/** If false (default) then delete any existing data before writing the output from the new job (if true then append/merge/deduplicate based on document_schema)   
		 * @return - Whether to preserve existing data before adding the output of the new job
		 */
		public Boolean preserve_existing_data() { return preserve_existing_data; }
		
		/** Whether the output data is written to a transient file store (default) or whether it's used as the output for this bucket (or a sub-bucket) 
		 * @return - whether the output data is transient or persisted in a bucket
		 */
		public Boolean is_transient() { return is_transient; }
		
		/** Optional - if writing the output to a bucket then by default it goes to the main bucket, this can optionally be set to a sub bucket 
		 * @return - the optional sub-bucket to which to write bucket output data
		 */
		public String sub_bucket_path() { return sub_bucket_path; }
		
		/** By default transient data is written to file - if this is set to "streaming" then it is instead written to a system queue  
		 * @return - the type (streaming/batch/both) of the transient output
		 */
		public DataBucketBean.MasterEnrichmentType transient_type() { return transient_type; }
		
		private Boolean preserve_existing_data;
		private Boolean is_transient;
		private String sub_bucket_path;
		private DataBucketBean.MasterEnrichmentType transient_type;		
	}
	private AnalyticThreadJobOutputBean output;	
}
