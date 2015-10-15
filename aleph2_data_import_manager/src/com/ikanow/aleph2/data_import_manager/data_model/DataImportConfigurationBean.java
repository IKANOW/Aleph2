/*******************************************************************************
* Copyright 2015, The IKANOW Open Source Project.
* 
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License, version 3,
* as published by the Free Software Foundation.
* 
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Affero General Public License for more details.
* 
* You should have received a copy of the GNU Affero General Public License
* along with this program. If not, see <http://www.gnu.org/licenses/>.
******************************************************************************/
package com.ikanow.aleph2.data_import_manager.data_model;

import java.util.HashSet;
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
			Boolean governance_enabled, Set<String> node_rules) {
		this.harvest_enabled = harvest_enabled;
		this.analytics_enabled = analytics_enabled;
		this.governance_enabled = governance_enabled;
		this.node_rules = node_rules;
	}
	public boolean harvest_enabled()  { return Optional.ofNullable(harvest_enabled).orElse(true); }
	public boolean analytics_enabled()  { return Optional.ofNullable(analytics_enabled).orElse(true); }
	public boolean governance_enabled() { return Optional.ofNullable(governance_enabled).orElse(true); }
	public Set<String> node_rules() { return Optional.ofNullable(node_rules).orElse(new HashSet<String>()); }
	
	private Boolean harvest_enabled;
	private Boolean analytics_enabled;
	private Boolean governance_enabled;
	private Set<String> node_rules;
	
}
