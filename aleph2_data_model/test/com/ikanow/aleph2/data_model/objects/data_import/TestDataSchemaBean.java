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

import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class TestDataSchemaBean {

	@Test 
	public void test_accessors() {
		
		// Storage Schema Bean
		
		DataSchemaBean.StorageSchemaBean empty_storage_bean = new DataSchemaBean.StorageSchemaBean();
		assertEquals("Empty Storage Bean", empty_storage_bean.service_name(), null);		
		
		DataSchemaBean.StorageSchemaBean.StorageSubSchemaBean raw = new DataSchemaBean.StorageSchemaBean.StorageSubSchemaBean(
				true,
				"raw_grouping_time_period",
				DataSchemaBean.StorageSchemaBean.StorageSubSchemaBean.TimeSourcePolicy.batch,
				"raw_exist_age_max",
				"raw_codec",
				null
				);
		DataSchemaBean.StorageSchemaBean.StorageSubSchemaBean json = new DataSchemaBean.StorageSchemaBean.StorageSubSchemaBean(
				false,
				"json_grouping_time_period",
				DataSchemaBean.StorageSchemaBean.StorageSubSchemaBean.TimeSourcePolicy.clock_time,
				"json_exist_age_max",
				null,
				null
				);
		DataSchemaBean.StorageSchemaBean.StorageSubSchemaBean processed = new DataSchemaBean.StorageSchemaBean.StorageSubSchemaBean(
				null,
				"processed_grouping_time_period",
				null,
				"processed_exist_age_max",
				"processed_codec",
				null
				);
		
		DataSchemaBean.StorageSchemaBean storage_bean =
				new DataSchemaBean.StorageSchemaBean(
						true,
						"service_name",
						raw,
						json,
						processed,
						ImmutableMap.<String, Object>builder().put("technology_override", "schema").build()
						);
		
		assertEquals("Storage bean enabled", storage_bean.enabled(), true);
		assertEquals("Storage bean service_name", storage_bean.service_name(), "service_name");
		assertTrue("Storage bean raw enabled", storage_bean.raw().enabled());
		assertFalse("Storage bean json enabled", storage_bean.json().enabled());
		assertEquals("Storage bean processed enabled", null, storage_bean.processed().enabled());
		assertEquals("Storage bean raw_grouping_time_period", storage_bean.raw().grouping_time_period(), "raw_grouping_time_period");
		assertEquals("Storage bean json_grouping_time_period", storage_bean.json().grouping_time_period(), "json_grouping_time_period");
		assertEquals("Storage bean processed_grouping_time_period", storage_bean.processed().grouping_time_period(), "processed_grouping_time_period");
		assertEquals("Storage bean raw grouping_time_policy", storage_bean.raw().grouping_time_policy(), DataSchemaBean.StorageSchemaBean.StorageSubSchemaBean.TimeSourcePolicy.batch);
		assertEquals("Storage bean json grouping_time_policy", storage_bean.json().grouping_time_policy(), DataSchemaBean.StorageSchemaBean.StorageSubSchemaBean.TimeSourcePolicy.clock_time);
		assertEquals("Storage bean processed grouping_time_policy", storage_bean.processed().grouping_time_policy(), null);

		assertEquals("Storage bean raw_exist_age_max", storage_bean.raw().exist_age_max(), "raw_exist_age_max");
		assertEquals("Storage bean json_exist_age_max", storage_bean.json().exist_age_max(), "json_exist_age_max");
		assertEquals("Storage bean processed_exist_age_max", storage_bean.processed().exist_age_max(), "processed_exist_age_max");
		assertEquals("Storage bean raw codec", storage_bean.raw().codec(), "raw_codec");
		assertEquals("Storage bean json codec", storage_bean.json().codec(), null);
		assertEquals("Storage bean processed codec", storage_bean.processed().codec(), "processed_codec");
		assertEquals("Storage bean technology_override_schema", storage_bean.technology_override_schema(), ImmutableMap.<String, Object>builder().put("technology_override", "schema").build());
		
		// Document Bean
		
		DataSchemaBean.DocumentSchemaBean empty_document_bean = new DataSchemaBean.DocumentSchemaBean();
		assertEquals("Empty Document Bean", empty_document_bean.service_name(), null);		
		
		DataSchemaBean.DocumentSchemaBean document_bean =
				new DataSchemaBean.DocumentSchemaBean(
						true,
						"service_name",
						false,
						Arrays.asList("deduplication_fields"),
						ImmutableMap.<String, Object>builder().put("technology_override", "schema").build()
						);
		
		assertEquals("Document bean enabled", document_bean.enabled(), true);
		assertEquals("Document bean service_name", document_bean.service_name(), "service_name");
		assertEquals("Document bean deduplicate", document_bean.deduplicate(), false);
		assertEquals("Document bean deduplication_fields", document_bean.deduplication_fields(), Arrays.asList("deduplication_fields"));
		assertEquals("Document bean technology_override_schema", document_bean.technology_override_schema(), ImmutableMap.<String, Object>builder().put("technology_override", "schema").build());
		
		// WriteSettings (shared across schemas - currenly only search_index_bean though)
		
		DataSchemaBean.WriteSettings empty_write_settings =  new DataSchemaBean.WriteSettings();
		assertEquals("Empty Search Index Bean", empty_write_settings.target_write_concurrency(), null);		
		
		DataSchemaBean.WriteSettings write_settings =  new DataSchemaBean.WriteSettings(1, 10L, 100, 1000);
		assertEquals(1, (int)write_settings.batch_max_objects());
		assertEquals(10, write_settings.batch_max_size_kb().longValue());
		assertEquals(100, (int)write_settings.batch_flush_interval());
		assertEquals(1000, (int)write_settings.target_write_concurrency());
		
		// Search Index Bean
		
		DataSchemaBean.SearchIndexSchemaBean empty_search_index_bean = new DataSchemaBean.SearchIndexSchemaBean();
		assertEquals("Empty Search Index Bean", empty_search_index_bean.service_name(), null);		
		
		DataSchemaBean.SearchIndexSchemaBean search_index_bean =
				new DataSchemaBean.SearchIndexSchemaBean(
						true,
						write_settings, 100L,
						"service_name",
						ImmutableMap.<String, Object>builder().put("technology_override", "schema").build()
						);

		assertEquals("Search Index bean enabled", search_index_bean.enabled(), true);
		assertEquals("Search Index bean service_name", search_index_bean.service_name(), "service_name");
		assertEquals("Search Index bean concurrency", 1000, (int)search_index_bean.target_write_settings().target_write_concurrency());
		assertEquals("Search Index bean max size", 100, (long)search_index_bean.target_index_size_mb());
		assertEquals("Search Index bean technology_override_schema", search_index_bean.technology_override_schema(), ImmutableMap.<String, Object>builder().put("technology_override", "schema").build());
		
		// Columnar Bean:
		
		DataSchemaBean.ColumnarSchemaBean empty_columnar_bean = new DataSchemaBean.ColumnarSchemaBean();
		assertEquals("Empty Columnar Bean", empty_columnar_bean.service_name(), null);		
		
		DataSchemaBean.ColumnarSchemaBean columnar_bean =
				new DataSchemaBean.ColumnarSchemaBean(
						true,
						"service_name",
						Arrays.asList("field_include_list"),
						Arrays.asList("field_exclude_list"),
						Arrays.asList("field_include_pattern_list"),
						Arrays.asList("field_exclude_pattern_list"),
						Arrays.asList("field_type_include_list"),
						Arrays.asList("field_type_exclude_list"),
						ImmutableMap.<String, Object>builder().put("technology_override", "schema").build()
						);
		
		assertEquals("Columnar bean enabled", columnar_bean.enabled(), true);
		assertEquals("Columnar bean service_name", columnar_bean.service_name(), "service_name");
		assertEquals("Columnar bean field_include_list", columnar_bean.field_include_list(), Arrays.asList("field_include_list"));
		assertEquals("Columnar bean field_exclude_list", columnar_bean.field_exclude_list(), Arrays.asList("field_exclude_list"));
		assertEquals("Columnar bean field_include_pattern_list", columnar_bean.field_include_pattern_list(), Arrays.asList("field_include_pattern_list"));
		assertEquals("Columnar bean field_exclude_pattern_list", columnar_bean.field_exclude_pattern_list(), Arrays.asList("field_exclude_pattern_list"));
		assertEquals("Columnar bean field_type_include_list", columnar_bean.field_type_include_list(), Arrays.asList("field_type_include_list"));
		assertEquals("Columnar bean field_type_exclude_list", columnar_bean.field_type_exclude_list(), Arrays.asList("field_type_exclude_list"));
		assertEquals("Columnar bean technology_override_schema", columnar_bean.technology_override_schema(), ImmutableMap.<String, Object>builder().put("technology_override", "schema").build());
		
		// Temporal
		
		DataSchemaBean.TemporalSchemaBean empty_temporal_bean = new DataSchemaBean.TemporalSchemaBean();
		assertEquals("Empty Temporal Bean", empty_temporal_bean.service_name(), null);		
		
		DataSchemaBean.TemporalSchemaBean temporal_bean =
				new DataSchemaBean.TemporalSchemaBean(
						true,
						"service_name",
						"grouping_time_period",
						"hot_age_max",
						"warm_age_max",
						"cold_age_max",
						"exist_age_max",
						"@timestamp",
						ImmutableMap.<String, Object>builder().put("technology_override", "schema").build()
						);

		assertEquals("Temporal bean enabled", temporal_bean.enabled(), true);
		assertEquals("Temporal bean service_name", temporal_bean.service_name(), "service_name");
		assertEquals("Temporal bean grouping_time_period", temporal_bean.grouping_time_period(), "grouping_time_period");
		assertEquals("Temporal bean hot_age_max", temporal_bean.hot_age_max(), "hot_age_max");
		assertEquals("Temporal bean warm_age_max", temporal_bean.warm_age_max(), "warm_age_max");
		assertEquals("Temporal bean cold_age_max", temporal_bean.cold_age_max(), "cold_age_max");
		assertEquals("Temporal bean exist_age_max", temporal_bean.exist_age_max(), "exist_age_max");
		assertEquals("Temporal bean timestamp", temporal_bean.time_field(), "@timestamp");
		assertEquals("Temporal bean technology_override_schema", temporal_bean.technology_override_schema(), ImmutableMap.<String, Object>builder().put("technology_override", "schema").build());		
		
		// Geospatial

		DataSchemaBean.GeospatialSchemaBean geospatial_bean = new DataSchemaBean.GeospatialSchemaBean();
		
		// Graph

		DataSchemaBean.GraphSchemaBean graph_bean = new DataSchemaBean.GraphSchemaBean();
		
		// Data warehouse

		DataSchemaBean.DataWarehouseSchemaBean warehouse_bean = new DataSchemaBean.DataWarehouseSchemaBean();
		
		// Parent DataSchemaBean
		
		DataSchemaBean empty_schema_bean = new DataSchemaBean();
		assertEquals("Empty Schema Bean", empty_schema_bean.storage_schema(), null);				
		
		DataSchemaBean schema_bean = new DataSchemaBean(storage_bean, document_bean, search_index_bean,
				columnar_bean, temporal_bean, geospatial_bean, graph_bean, warehouse_bean
				);
		
		assertEquals("Schema bean - storage_bean", schema_bean.storage_schema(), storage_bean);
		assertEquals("Schema bean - document_bean", schema_bean.document_schema(), document_bean);
		assertEquals("Schema bean - search_index_bean", schema_bean.search_index_schema(), search_index_bean);
		assertEquals("Schema bean - columnar_bean", schema_bean.columnar_schema(), columnar_bean);
		assertEquals("Schema bean - temporal_bean", schema_bean.temporal_schema(), temporal_bean);
		assertEquals("Schema bean - geospatial_bean", schema_bean.geospatial_schema(), geospatial_bean);
		assertEquals("Schema bean - graph_bean", schema_bean.graph_schema(), graph_bean);
		assertEquals("Schema bean - warehouse_bean", schema_bean.data_warehouse_schema(), warehouse_bean);
	}
}
