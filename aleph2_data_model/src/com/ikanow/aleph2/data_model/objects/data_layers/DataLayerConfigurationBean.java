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
package com.ikanow.aleph2.data_model.objects.data_layers;

import java.util.Map;

import org.checkerframework.checker.nullness.qual.Nullable;

/** Bean containing the top level configuration for AlephDB core services
 * @author acp
 *
 */
public class DataLayerConfigurationBean {

	protected DataLayerConfigurationBean() {}
	
	/** User constructor
	 */
	public DataLayerConfigurationBean(@Nullable String storage_service,
			@Nullable Map<String, Object> storage_service_config,
			@Nullable String columnar_db_service,
			@Nullable Map<String, Object> columnar_db_service_config,
			@Nullable String graph_db_service,
			@Nullable Map<String, Object> graph_db_service_config,
			@Nullable String geospatial_service,
			@Nullable Map<String, Object> geospatial_service_config,
			@Nullable String management_db_service,
			@Nullable Map<String, Object> management_db_service_config,
			@Nullable String document_db_service,
			@Nullable Map<String, Object> document_db_service_config,
			@Nullable String search_index_service,
			@Nullable Map<String, Object> search_index_service_config,
			@Nullable String temporal_service, 
			@Nullable Map<String, Object> temporal_service_config)
	{
		this.storage_service = storage_service;
		this.storage_service_config = storage_service_config;
		this.columnar_db_service = columnar_db_service;
		this.columnar_db_service_config = columnar_db_service_config;
		this.graph_db_service = graph_db_service;
		this.graph_db_service_config = graph_db_service_config;
		this.geospatial_service = geospatial_service;
		this.geospatial_service_config = geospatial_service_config;
		this.management_db_service = management_db_service;
		this.management_db_service_config = management_db_service_config;
		this.document_db_service = document_db_service;
		this.document_db_service_config = document_db_service_config;
		this.search_index_service = search_index_service;
		this.search_index_service_config = search_index_service_config;
		this.temporal_service = temporal_service;
		this.temporal_service_config = temporal_service_config;
	}
	/** The implementation of the archive service, in format "alias:fully.qualified.class.name"
	 * @return the storage_service
	 */
	public String archive_service() {
		return storage_service;
	}
	/** General configuration for the archive service as a JSON object
	 * @return the storage_service_config
	 */
	public Map<String, Object> archive_service_config() {
		return storage_service_config;
	}
	/** The implementation of the columnar service, in format "alias:fully.qualified.class.name"
	 * @return the columnar_db_service
	 */
	public String columnar_service() {
		return columnar_db_service;
	}
	/** General configuration for the columnar service as a JSON object
	 * @return the columnar_db_service_config
	 */
	public Map<String, Object> columnar_service_config() {
		return columnar_db_service_config;
	}
	/** The implementation of the graph db service, in format "alias:fully.qualified.class.name"
	 * @return the graph_db_service
	 */
	public String graph_db_service() {
		return graph_db_service;
	}
	/** General configuration for the graph db service as a JSON object
	 * @return the graph_db_service_config
	 */
	public Map<String, Object> graph_db_service_config() {
		return graph_db_service_config;
	}
	/** The implementation of the geospatial service, in format "alias:fully.qualified.class.name"
	 * @return the geospatial_service
	 */
	public String geospatial_service() {
		return geospatial_service;
	}
	/** General configuration for the geospatial service as a JSON object
	 * @return the geospatial_service_config
	 */
	public Map<String, Object> geospatial_service_config() {
		return geospatial_service_config;
	}
	/** The implementation of the management db service, in format "alias:fully.qualified.class.name"
	 * @return the management_db_service
	 */
	public String management_db_service() {
		return management_db_service;
	}
	/** General configuration for the management db service as a JSON object
	 * @return the management_db_service_config
	 */
	public Map<String, Object> management_db_service_config() {
		return management_db_service_config;
	}
	/** The implementation of the object db service, in format "alias:fully.qualified.class.name"
	 * @return the object_db_service
	 */
	public String object_db_service() {
		return document_db_service;
	}
	/** General configuration for the object db service as a JSON object
	 * @return the object_db_service_config
	 */
	public Map<String, Object> object_db_service_config() {
		return document_db_service_config;
	}
	/** The implementation of the search index service, in format "alias:fully.qualified.class.name"
	 * @return the search_index_service
	 */
	public String search_index_service() {
		return search_index_service;
	}
	/** General configuration for the search index service as a JSON object
	 * @return the search_index_service_config
	 */
	public Map<String, Object> search_index_service_config() {
		return search_index_service_config;
	}
	/** The implementation of the temporal service, in format "alias:fully.qualified.class.name"
	 * @return the temporal_service
	 */
	public String temporal_service() {
		return temporal_service;
	}
	/** General configuration for the temporal service as a JSON object
	 * @return the temporal_service_config
	 */
	public Map<String, Object> temporal_service_config() {
		return temporal_service_config;
	}
	private String storage_service;
	private Map<String, Object> storage_service_config;
	private String columnar_db_service;
	private Map<String, Object> columnar_db_service_config;
	private String graph_db_service;
	private Map<String, Object> graph_db_service_config;
	private String geospatial_service;
	private Map<String, Object> geospatial_service_config;
	private String management_db_service;
	private Map<String, Object> management_db_service_config;
	private String document_db_service;
	private Map<String, Object> document_db_service_config;
	private String search_index_service;
	private Map<String, Object> search_index_service_config;
	private String temporal_service;
	private Map<String, Object> temporal_service_config;
}
