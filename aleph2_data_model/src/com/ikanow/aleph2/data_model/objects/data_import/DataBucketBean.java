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

public class DataBucketBean {

	////////////////////////////////////////
	
	// General information
	
	private String bucket_name;
	private List<String> bucket_aliases;
	private String bucket_id;
	
	private List<String> access_groups; 
	
	//TODO multi-bucket? make a different bean type?	
	
	////////////////////////////////////////
	
	// Harvest specific information
	
	private String harvest_technology_name_or_id;
	
	public static class HarvestControlMetadataBean {
		private List<String> library_ids_or_names;
		private Map<String, Object> config;
	}	
	private List<HarvestControlMetadataBean> harvest_configs;
	
	////////////////////////////////////////
	
	// Enrichment specific information
	
	public static class EnrichmentControlMetadataBean {
		private List<String> library_ids_or_names;
		private Map<String, Object> config;
	}	
	private List<EnrichmentControlMetadataBean> enrichment_configs;
	
	//TODO: batch settings?
	
	////////////////////////////////////////
	
	// Data schema
	
	private DataSchemaBean data_schema;
	private String bucket_location;
	
	private DataSchemaBean future_data_schema; 
	private String future_bucket_location;
	
}
