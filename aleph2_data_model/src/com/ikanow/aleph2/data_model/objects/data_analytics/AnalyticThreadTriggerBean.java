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

/** Defines a potentially complex trigger for the analytic bean
 * @author Alex
 */
public class AnalyticThreadTriggerBean implements Serializable {
	private static final long serialVersionUID = -8619378830306003059L;

	protected AnalyticThreadTriggerBean() {}
	
	/**
	 * @param schedule - If no complex trigger is set, then the analytic thread is run on this frequency, else determines the poll frequency of the complex trigger
	 * @param auto_calculate - whether the auto-calculate override is set
	 * @param trigger - The complex trigger or triggers for this thread (optional)
	 */
	public AnalyticThreadTriggerBean(final Boolean enabled, final String schedule, final Boolean auto_calculate, final AnalyticThreadComplexTriggerBean trigger) {
		this.enabled = enabled;
		this.schedule = schedule;
		this.auto_calculate = auto_calculate;
		this.trigger = trigger;
	}
	
	/** If true (default if a trigger object exists) then a trigger is specified, if false then this job will only be run manually
	 * @return
	 */
	public Boolean enabled() { return enabled; }
	
	/** If no complex trigger is set, then the analytic thread is run on this frequency 
	 *  (eg in some human readable format ("every 5 minutes", "hourly", "3600" etc)
	 *  If a complex trigger is set, then this field determines how often the trigger is tested
	 *  (In which case, it defaults to some sensible value (possibly changing with system load), order minutes not seconds or hours)
	 * @return the schedule in human readable format
	 */
	public String schedule() { return schedule; }
	
	/** If true (defaults to false if "trigger" is specified, true otherwise) ignores "trigger" and auto-generates the dependencies
	 *  as a set of ANDs of the external inputs in the job list
	 * @return whether the auto-calculate override is set
	 */
	public Boolean auto_calculate() { return auto_calculate; }
	
	/** The complex trigger or triggers for this thread (optional) 
	 * @return - The complex trigger or triggers for this thread (optional)
	 */
	public AnalyticThreadComplexTriggerBean trigger() { return trigger; }
	
	////////////////////////////////////////
	
	// Trigger configuration bean
	
	/** Either a specific trigger instance, or a boolean grouping of trigger instances (can nest indefinitely)
	 * @author Alex
	 */
	public static class AnalyticThreadComplexTriggerBean implements Serializable {
		private static final long serialVersionUID = 8747569087422807344L;

		protected AnalyticThreadComplexTriggerBean() {}
		
		/** User constructor for the grouped version of the trigger
		 * @param op
		 * @param dependency_list
		 */
		public AnalyticThreadComplexTriggerBean(final TriggerOperator op, final Boolean enabled, final List<AnalyticThreadComplexTriggerBean> dependency_list) {
			this.op = op;
			this.enabled = enabled;
			this.dependency_list = dependency_list; 
		}
		
		/** User constructor for the single version of the trigger
		 * @param type
		 * @param resource_name_or_id
		 * @param config
		 */
		public AnalyticThreadComplexTriggerBean(final TriggerType type, final Boolean enabled, 
				final String custom_analytic_technology_name_or_id, final List<String> custom_module_name_or_ids,
				final String data_service,
				final String resource_name_or_id,
				final Long resource_trigger_limit,
				final LinkedHashMap<String, Object> config) {			
			this.type = type;
			this.enabled = enabled;
			this.custom_analytic_technology_name_or_id = custom_analytic_technology_name_or_id;
			this.custom_module_name_or_ids = custom_module_name_or_ids;
			this.data_service = data_service;
			this.resource_name_or_id = resource_name_or_id;
			this.resource_trigger_limit = resource_trigger_limit;
			this.config = config;
		}
		
		////////////////////////////////////////
		
		// Trigger groupings
		
		public enum TriggerOperator { and, not, or };
		
		/** (EITHER SPECIFY op/dependency_list, OR type/resource_name_or_id/config)
		 *  The operation to apply to the list of triggers: and/or/not
		 *   (Note this can nest indefinitely, eg { "and": [ { "or": [ { "and": [...] } ] } ] })
		 * @return the operation to apply 
		 */
		public TriggerOperator op() { return op; }
				
		/** (EITHER SPECIFY op/dependency_list, OR type/resource_name_or_id/config)
		 *  The list of triggers to check before starting an analytic thread
		 * @return The list of triggers to check before starting an analytic thread
		 */
		public List<AnalyticThreadComplexTriggerBean> dependency_list() { return null == dependency_list ? dependency_list : Collections.unmodifiableList(dependency_list); }
		
		private TriggerOperator op;
		private List<AnalyticThreadComplexTriggerBean> dependency_list;
		
		//OR
		
		////////////////////////////////////////
		
		// Single trigger configuration
		
		/** Whether this trigger is enabled (defaults to true)
		 * @return
		 */
		public Boolean enabled() { return enabled; }
		
		public enum TriggerType { bucket, custom, file, time, none }
		
		/** The type of trigger dependency - bucket (if the specified bucket has updated its output), custom (calls the analytic technology to decide whether to trigger), 
		 *  file (checks if the specified directory has new data)
		 * @return the trigger type (bucket, custom, file)
		 */
		public TriggerType type() { return type; }
		
		/** For custom triggers, specifies the analytic technology to use
		 * @return specifies the analytic technology to use for the custom trigger
		 */
		public String custom_analytic_technology_name_or_id() { return custom_analytic_technology_name_or_id; }
		
		/** For custom triggers, any additional nodules required on the classpath
		 * @return  any additional nodules required on the classpath by this custom trigger
		 */
		public List<String> custom_module_name_or_ids() { return null == custom_module_name_or_ids ? custom_module_name_or_ids : Collections.unmodifiableList(custom_module_name_or_ids); } 

		/** By default, a dependency will trigger whenever a batch bucket completes a batch. Alternatively (eg for streaming buckets)
		 *  it is possible to specify a data service, and that's resource will be polled to determine when to trigger
		 * @return the optional data service to check against
		 */
		public String data_service() { return data_service; }
		
		/** The resource name or id for the trigger (eg bucket name/id, file path, arbitrary String passed to analytic technology module for custom) 
		 * @return The resource name or id for the trigger 
		 */
		public String resource_name_or_id() { return resource_name_or_id; }
		
		/** If comparing against a data service, this is the trigger threshold to use
		 *  It indicates #MB for the storage service, #records for other services
		 *  Defaults to 0 (ie trigger on any change)
		 * @return the trigger threshold for data services
		 */
		public Long resource_trigger_limit() { return resource_trigger_limit; }
		
		/** For custom triggers, any custom configuration to be passed to the analytic modle
		 * @return For custom triggers, any custom configuration to be passed to the analytic modle
		 */
		public Map<String, Object> config() { return null == config ? config : Collections.unmodifiableMap(config); }
		
		private Boolean enabled;
		private TriggerType type;		
		private String custom_analytic_technology_name_or_id;
		private List<String> custom_module_name_or_ids;
		private String data_service;
		private String resource_name_or_id;
		private Long resource_trigger_limit;
		private LinkedHashMap<String, Object> config;
	}
	
	private Boolean enabled;	
	private AnalyticThreadComplexTriggerBean trigger;
	private Boolean auto_calculate;	
	private String schedule;
}
