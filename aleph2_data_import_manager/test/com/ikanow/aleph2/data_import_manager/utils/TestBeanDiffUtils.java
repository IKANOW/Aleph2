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
package com.ikanow.aleph2.data_import_manager.utils;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ikanow.aleph2.data_model.objects.data_import.BucketDiffBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.SearchIndexSchemaBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;

public class TestBeanDiffUtils {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testCreateDiffBean() {
		{
			//single field specified in both that changed
			final DataBucketBean original = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::full_name, "a")
					.done().get();
			final DataBucketBean updated = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::full_name, "b")
					.done().get();
			final Optional<BucketDiffBean> diff = BeanDiffUtils.createDiffBean(updated, original);
			final BucketDiffBean diffs = diff.get();
			
			assertEquals(true, diffs.diffs().get("full_name"));
		}
		{
			//single field exists in original, removed in modified
			final DataBucketBean original = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::full_name, "a")
					.done().get();
			final DataBucketBean updated = BeanTemplateUtils.build(DataBucketBean.class)
					.done().get();
			final Optional<BucketDiffBean> diff = BeanDiffUtils.createDiffBean(updated, original);
			final BucketDiffBean diffs = diff.get();
			
			assertEquals(true, diffs.diffs().get("full_name"));
		}	
		{
			//single field exists in modified, removed in original
			final DataBucketBean original = BeanTemplateUtils.build(DataBucketBean.class)
					.done().get();
			final DataBucketBean updated = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::full_name, "b")
					.done().get();
			final Optional<BucketDiffBean> diff = BeanDiffUtils.createDiffBean(updated, original);
			final BucketDiffBean diffs = diff.get();
			
			assertEquals(true, diffs.diffs().get("full_name"));
		}	
		{
			//single field unchanged in both
			final DataBucketBean original = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::full_name, "b")
					.done().get();
			final DataBucketBean updated = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::full_name, "b")
					.done().get();
			final Optional<BucketDiffBean> diff = BeanDiffUtils.createDiffBean(updated, original);
			final BucketDiffBean diffs = diff.get();
			
			assertEquals(false, diffs.diffs().get("full_name"));
		}
		{
			//array field unchanged in both
			final DataBucketBean original = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::aliases, new HashSet<String>(Arrays.asList("a")))
					.done().get();
			final DataBucketBean updated = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::aliases, new HashSet<String>(Arrays.asList("a")))
					.done().get();
			final Optional<BucketDiffBean> diff = BeanDiffUtils.createDiffBean(updated, original);
			final BucketDiffBean diffs = diff.get();
			
			assertEquals(false, diffs.diffs().get("aliases"));
		}
		{
			//array field changed in modified
			final DataBucketBean original = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::aliases, new HashSet<String>(Arrays.asList("a")))
					.done().get();
			final DataBucketBean updated = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::aliases, new HashSet<String>(Arrays.asList("b")))
					.done().get();
			final Optional<BucketDiffBean> diff = BeanDiffUtils.createDiffBean(updated, original);
			final BucketDiffBean diffs = diff.get();
			
			assertEquals(true, diffs.diffs().get("aliases"));
		}
		{
			//multiple fields unchanged in both
			final DataBucketBean original = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::full_name, "a")
					.with(DataBucketBean::display_name, "b")
					.with(DataBucketBean::owner_id, "c")
					.done().get();
			final DataBucketBean updated = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::full_name, "a")
					.with(DataBucketBean::display_name, "b")
					.with(DataBucketBean::owner_id, "c")
					.done().get();
			final Optional<BucketDiffBean> diff = BeanDiffUtils.createDiffBean(updated, original);
			final BucketDiffBean diffs = diff.get();
			
			assertEquals(false, diffs.diffs().get("full_name"));
			assertEquals(false, diffs.diffs().get("display_name"));
			assertEquals(false, diffs.diffs().get("owner_id"));
		}
		{
			//multiple fields changed in both
			final DataBucketBean original = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::full_name, "a")
					.with(DataBucketBean::display_name, "b")
					.with(DataBucketBean::owner_id, "c")
					.done().get();
			final DataBucketBean updated = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::full_name, "x")
					.with(DataBucketBean::display_name, "y")
					.with(DataBucketBean::owner_id, "z")
					.done().get();
			final Optional<BucketDiffBean> diff = BeanDiffUtils.createDiffBean(updated, original);
			final BucketDiffBean diffs = diff.get();
			
			assertEquals(true, diffs.diffs().get("full_name"));
			assertEquals(true, diffs.diffs().get("display_name"));
			assertEquals(true, diffs.diffs().get("owner_id"));
		}
		{
			//multiple fields with 1 changed in both
			final DataBucketBean original = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::full_name, "a")
					.with(DataBucketBean::display_name, "b")
					.with(DataBucketBean::owner_id, "c")
					.done().get();
			final DataBucketBean updated = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::full_name, "a")
					.with(DataBucketBean::display_name, "y")
					.with(DataBucketBean::owner_id, "c")
					.done().get();
			final Optional<BucketDiffBean> diff = BeanDiffUtils.createDiffBean(updated, original);
			final BucketDiffBean diffs = diff.get();
			
			assertEquals(false, diffs.diffs().get("full_name"));
			assertEquals(true, diffs.diffs().get("display_name"));
			assertEquals(false, diffs.diffs().get("owner_id"));
		}
		{
			//complex fields changed in updated
			final DataBucketBean original = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::data_schema, BeanTemplateUtils.build(DataSchemaBean.class)
							.with(DataSchemaBean::search_index_schema, BeanTemplateUtils.build(SearchIndexSchemaBean.class)
									.with(SearchIndexSchemaBean::service_name, "a")
									.done().get())
							.done().get())
					.done().get();
			final DataBucketBean updated = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::data_schema, BeanTemplateUtils.build(DataSchemaBean.class)
							.with(DataSchemaBean::search_index_schema, BeanTemplateUtils.build(SearchIndexSchemaBean.class)
									.with(SearchIndexSchemaBean::service_name, "b")
									.done().get())
							.done().get())
					.done().get();
			final Optional<BucketDiffBean> diff = BeanDiffUtils.createDiffBean(updated, original);
			final BucketDiffBean diffs = diff.get();
			
			assertEquals(true, diffs.diffs().get("data_schema"));
		}
		{
			//complex fields changed in updated
			final DataBucketBean original = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::data_schema, BeanTemplateUtils.build(DataSchemaBean.class)
							.with(DataSchemaBean::search_index_schema, BeanTemplateUtils.build(SearchIndexSchemaBean.class)
									.with(SearchIndexSchemaBean::service_name, "a")
									.done().get())
							.done().get())
					.done().get();
			final DataBucketBean updated = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::data_schema, BeanTemplateUtils.build(DataSchemaBean.class)
							.with(DataSchemaBean::search_index_schema, BeanTemplateUtils.build(SearchIndexSchemaBean.class)
									.with(SearchIndexSchemaBean::service_name, "a")
									.done().get())
							.done().get())
					.done().get();
			final Optional<BucketDiffBean> diff = BeanDiffUtils.createDiffBean(updated, original);
			final BucketDiffBean diffs = diff.get();
			
			assertEquals(false, diffs.diffs().get("data_schema"));
		}
	}

}
