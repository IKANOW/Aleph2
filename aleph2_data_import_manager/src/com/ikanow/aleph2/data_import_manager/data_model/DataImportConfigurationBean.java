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

import java.util.Optional;

/** Configuration bean for the data import manager
 * @author Alex
 */
public class DataImportConfigurationBean {
	public static final String PROPERTIES_ROOT = "DataImportManager";
	
	protected DataImportConfigurationBean() {}
	
	public DataImportConfigurationBean(Boolean v1_sync_service_enabled, Boolean streaming_enrichment_enabled, Boolean batch_enrichment_enabled, Boolean storm_debug_mode) {
		this.harvest_enabled = v1_sync_service_enabled;
		this.streaming_enrichment_enabled = streaming_enrichment_enabled;
		this.batch_enrichment_enabled = batch_enrichment_enabled;
		this.storm_debug_mode = storm_debug_mode;
	}
	public boolean harvest_enabled()  { return Optional.ofNullable(harvest_enabled).orElse(true); }
	public boolean streaming_enrichment_enabled()  { return Optional.ofNullable(streaming_enrichment_enabled).orElse(true); }
	public boolean batch_enrichment_enabled() { return Optional.ofNullable(batch_enrichment_enabled).orElse(true); }
	public boolean storm_debug_mode() { return Optional.ofNullable(storm_debug_mode).orElse(false); }
	
	private Boolean harvest_enabled;
	private Boolean streaming_enrichment_enabled;
	private Boolean batch_enrichment_enabled;
	private Boolean storm_debug_mode;
	
}
