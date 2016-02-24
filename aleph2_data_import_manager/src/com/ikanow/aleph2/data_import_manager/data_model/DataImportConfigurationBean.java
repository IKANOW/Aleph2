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
package com.ikanow.aleph2.data_import_manager.data_model;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Configuration bean for the data import manager
 * @author Alex
 */
public class DataImportConfigurationBean {
	public static final String PROPERTIES_ROOT = "DataImportManager";
	
	protected DataImportConfigurationBean() {}
	
	/** User c'tor
	 * @param harvest_enabled
	 * @param streaming_enrichment_enabled
	 * @param batch_enrichment_enabled
	 * @param storm_debug_mode
	 */
	public DataImportConfigurationBean(
			Boolean harvest_enabled, Boolean analytics_enabled,  
			Boolean governance_enabled, Set<String> node_rules,
			Map<String, String> registered_technologies
			) {
		this.harvest_enabled = harvest_enabled;
		this.analytics_enabled = analytics_enabled;
		this.governance_enabled = governance_enabled;
		this.node_rules = node_rules;
		this.registered_technologies = registered_technologies;
	}
	public boolean harvest_enabled()  { return Optional.ofNullable(harvest_enabled).orElse(true); }
	public boolean analytics_enabled()  { return Optional.ofNullable(analytics_enabled).orElse(true); }
	public boolean governance_enabled() { return Optional.ofNullable(governance_enabled).orElse(true); }
	public Set<String> node_rules() { return Optional.ofNullable(node_rules).orElse(new HashSet<String>()); }
	
	public Map<String, String> registered_technologies() { return Optional.ofNullable(registered_technologies).orElse(Collections.emptyMap()); }
	
	private Boolean harvest_enabled;
	private Boolean analytics_enabled;
	private Boolean governance_enabled;
	private Set<String> node_rules;
	private Map<String, String> registered_technologies;
	
}
