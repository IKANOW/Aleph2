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
package com.ikanow.aleph2.data_model.utils;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Test;

import com.ikanow.aleph2.data_model.interfaces.shared_services.MockManagementCrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.MockServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.SearchIndexSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.TemporalSchemaBean;

public class TestMultiBucketUtils {

	@Test
	public void test_readOnlyCrud() throws InterruptedException {
		
		// Create a few buckets 
		final DataBucketBean single_fixed = BeanTemplateUtils.build(DataBucketBean.class)
				.with("_id", "single_fixed")
				.with("full_name", "/test/single/fixed")
				.with(DataBucketBean::data_schema,
						BeanTemplateUtils.build(DataSchemaBean.class)
							.with(DataSchemaBean::search_index_schema,
								BeanTemplateUtils.build(SearchIndexSchemaBean.class)
								.done().get()
							)
						.done().get()
						)
				.done().get();
		
		final DataBucketBean single_timed = BeanTemplateUtils.build(DataBucketBean.class)
				.with("_id", "single_timed")
				.with("full_name", "/test/single/timed")
				.with("multi_bucket_children", new HashSet<String>())
				.with(DataBucketBean::data_schema,
						BeanTemplateUtils.build(DataSchemaBean.class)
							.with(DataSchemaBean::search_index_schema,
								BeanTemplateUtils.build(SearchIndexSchemaBean.class)
								.done().get()
							)
							.with(DataSchemaBean::temporal_schema,
								BeanTemplateUtils.build(TemporalSchemaBean.class)
									.with(TemporalSchemaBean::grouping_time_period, "daily")
								.done().get()
							)
						.done().get()
						)
				.done().get();

		//(just to check we do ignore some buckets!)
		final DataBucketBean single_unused = BeanTemplateUtils.build(DataBucketBean.class)
				.with("_id", "single_unused")
				.with("full_name", "/test/single/unused")
				.with(DataBucketBean::data_schema,
						BeanTemplateUtils.build(DataSchemaBean.class)
							.with(DataSchemaBean::search_index_schema,
								BeanTemplateUtils.build(SearchIndexSchemaBean.class)
								.done().get()
							)
							.with(DataSchemaBean::temporal_schema,
								BeanTemplateUtils.build(TemporalSchemaBean.class)
									.with(TemporalSchemaBean::grouping_time_period, "daily")
								.done().get()
							)
						.done().get()
						)
				.done().get();
		
		// Create a few buckets 
		final DataBucketBean multi_parent = BeanTemplateUtils.build(DataBucketBean.class)
				.with("_id", "multi_parent")
				.with("full_name", "/test/multi/parent")
				.with("multi_bucket_children", new HashSet<String>(Arrays.asList("/test/multi/**", "/test/other_multi/child/fixed", "/test/multi/child/not_auth")))
				.done().get();	
		//(multi-bucket children includes itself but will be ignored because no nested multi-buckets 

		final DataBucketBean multi_no_wildcards = BeanTemplateUtils.build(DataBucketBean.class)
				.with("_id", "multi_parent")
				.with("full_name", "/test/multi/parent")
				.with("multi_bucket_children", new HashSet<String>(Arrays.asList("/test/other_multi/child/fixed", "/test/multi/child/not_auth")))
				.done().get();	
		//(multi-bucket children includes itself but will be ignored because no nested multi-buckets 
		
		
		final DataBucketBean multi_missing_parent = BeanTemplateUtils.build(DataBucketBean.class)
				.with("_id", "multi_parent")
				.with("full_name", "/test/multi/missing/parent")
				.with("multi_bucket_children", new HashSet<String>(Arrays.asList("/test/multi/missing/**", "/test/multi/child/not_auth")))
				.done().get();	
		
		
		final DataBucketBean multi_fixed = BeanTemplateUtils.build(DataBucketBean.class)
				.with("_id", "multi_fixed")
				.with("full_name", "/test/multi/child/fixed")
				.with("owner_id", "test")
				.with(DataBucketBean::data_schema,
						BeanTemplateUtils.build(DataSchemaBean.class)
							.with(DataSchemaBean::search_index_schema,
								BeanTemplateUtils.build(SearchIndexSchemaBean.class)
								.done().get()
							)
						.done().get()
						)
				.done().get();

		final DataBucketBean other_multi_fixed = BeanTemplateUtils.build(DataBucketBean.class)
				.with("_id", "other_multi_fixed")
				.with("full_name", "/test/other_multi/child/fixed")
				.with(DataBucketBean::data_schema,
						BeanTemplateUtils.build(DataSchemaBean.class)
							.with(DataSchemaBean::search_index_schema,
								BeanTemplateUtils.build(SearchIndexSchemaBean.class)
								.done().get()
							)
						.done().get()
						)
				.done().get();
		
		final DataBucketBean multi_timed = BeanTemplateUtils.build(DataBucketBean.class)
				.with("_id", "multi_timed")
				.with("full_name", "/test/multi/child/timed")
				.with(DataBucketBean::data_schema,
						BeanTemplateUtils.build(DataSchemaBean.class)
							.with(DataSchemaBean::search_index_schema,
								BeanTemplateUtils.build(SearchIndexSchemaBean.class)
								.done().get()
							)
							.with(DataSchemaBean::temporal_schema,
								BeanTemplateUtils.build(TemporalSchemaBean.class)
									.with(TemporalSchemaBean::grouping_time_period, "daily")
								.done().get()
							)
						.done().get()
						)
				.done().get();
		
		//(just to check we do ignore some buckets!)
		final DataBucketBean other_multi_unused = BeanTemplateUtils.build(DataBucketBean.class)
				.with("_id", "other_multi_unused")
				.with("full_name", "/test/other_multi/unused")
				.with(DataBucketBean::data_schema,
						BeanTemplateUtils.build(DataSchemaBean.class)
							.with(DataSchemaBean::search_index_schema,
								BeanTemplateUtils.build(SearchIndexSchemaBean.class)
								.done().get()
							)
							.with(DataSchemaBean::temporal_schema,
								BeanTemplateUtils.build(TemporalSchemaBean.class)
									.with(TemporalSchemaBean::grouping_time_period, "daily")
								.done().get()
							)
						.done().get()
						)
				.done().get();		
		
		// 1) Write some data into each real bucket:

		final MockManagementCrudService<DataBucketBean> mock_bucket_store = new MockManagementCrudService<>();		
		final MockServiceContext mock_service = new MockServiceContext();
		
		mock_bucket_store.storeObjects(
				Arrays.asList(
						single_fixed,
						single_timed,
						single_unused,
						multi_fixed,
						other_multi_fixed,
						multi_timed,
						other_multi_unused
						))
						;
		
		// 2) Grab a readable CRUD service and do some operations

		final Map<String, DataBucketBean> main_test = MultiBucketUtils.expandMultiBuckets(
				Arrays.asList(single_fixed, single_timed, multi_parent), 
				mock_bucket_store, 
				mock_service);

		assertEquals(Arrays.asList(
				"/test/multi/child/fixed", 
				"/test/multi/child/timed", 
				"/test/multi/parent", 
				"/test/other_multi/child/fixed", 
				"/test/single/fixed",
				"/test/single/timed" 
				), 
				main_test.keySet().stream().sorted().collect(Collectors.toList()));
		
		
		// 3) A few edge cases:

		final Map<String, DataBucketBean> no_wildcards = MultiBucketUtils.expandMultiBuckets(
				Arrays.asList(multi_no_wildcards, single_timed),
				mock_bucket_store, 
				mock_service);

		assertEquals(Arrays.asList(
				"/test/multi/parent", 
				"/test/other_multi/child/fixed", 
				"/test/single/timed" 
				), 
				no_wildcards.keySet().stream().sorted().collect(Collectors.toList()));
		
		final Map<String, DataBucketBean> read_crud_nothing = MultiBucketUtils.expandMultiBuckets(
				Arrays.asList(multi_missing_parent), 
				mock_bucket_store, 
				mock_service);
		
		assertEquals(Arrays.asList("/test/multi/missing/parent"), read_crud_nothing.keySet().stream().sorted().collect(Collectors.toList()));
		assertEquals(Arrays.asList(), read_crud_nothing.entrySet().stream().filter(kv -> null == kv.getValue().multi_bucket_children()).collect(Collectors.toList()));
		
		final Map<String, DataBucketBean> read_crud_really_nothing = MultiBucketUtils.expandMultiBuckets(
				Arrays.asList(), 
				mock_bucket_store, 
				mock_service);
		
		assertEquals(0, read_crud_really_nothing.size());
		
		final Map<String, DataBucketBean> read_crud_no_multi_no_fixed = MultiBucketUtils.expandMultiBuckets(
				Arrays.asList(single_timed, multi_missing_parent), 
				mock_bucket_store, 
				mock_service);

		assertEquals(Arrays.asList(
				"/test/multi/missing/parent", 
				"/test/single/timed" 
				), 
				read_crud_no_multi_no_fixed.keySet().stream().sorted().collect(Collectors.toList()));
		
		final Map<String, DataBucketBean> read_crud_no_multi_no_timed = MultiBucketUtils.expandMultiBuckets(
				Arrays.asList(single_timed, multi_missing_parent), 
				mock_bucket_store, 
				mock_service);
				
		assertEquals(Arrays.asList(
				"/test/multi/missing/parent", 
				"/test/single/timed" 
				), 
				read_crud_no_multi_no_timed.keySet().stream().sorted().collect(Collectors.toList()));
	}
}
