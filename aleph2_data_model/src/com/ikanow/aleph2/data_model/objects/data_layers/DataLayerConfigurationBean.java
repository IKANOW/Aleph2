package com.ikanow.aleph2.data_model.objects.data_layers;

import java.util.Map;

public class DataLayerConfigurationBean {

	private String archive_service;
	private Map<String, Object> archive_service_config;
	private String columnar_service;
	private Map<String, Object> columnar_service_config;
	private String graph_db_service;
	private Map<String, Object> graph_db_service_config;
	private String geospatial_service;
	private Map<String, Object> geospatial_service_config;
	private String management__db_service;
	private Map<String, Object> management__db_service_config;
	private String object_db_service;
	private Map<String, Object> object_db_service_config;
	private String search_index_service;
	private Map<String, Object> search_index_service_config;
	private String temporal_service;
	private Map<String, Object> temporal_service_config;
}
