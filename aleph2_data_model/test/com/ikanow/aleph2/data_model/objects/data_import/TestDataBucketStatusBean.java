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
package com.ikanow.aleph2.data_model.objects.data_import;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Date;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean.MasterEnrichmentType;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;

public class TestDataBucketStatusBean {

	@Test
	public void testDataBucketStatusBean() {
		DataBucketStatusBean bean = new DataBucketStatusBean(
				"test_id", "/test/bucket/path/", false, new Date(), 0L, Arrays.asList("host"),
				true, false, MasterEnrichmentType.batch,
				ImmutableMap.<String, BasicMessageBean>builder().put("a", BeanTemplateUtils.build(BasicMessageBean.class).done().get()).put("b", BeanTemplateUtils.build(BasicMessageBean.class).done().get()).build(), 
				ImmutableMap.<String, BasicMessageBean>builder().put("a", BeanTemplateUtils.build(BasicMessageBean.class).done().get()).put("b", BeanTemplateUtils.build(BasicMessageBean.class).done().get()).build(), 
				ImmutableMap.<String, BasicMessageBean>builder().put("a/b", BeanTemplateUtils.build(BasicMessageBean.class).done().get()).put("c/d", BeanTemplateUtils.build(BasicMessageBean.class).done().get()).build() 
				);
		
		assertEquals("test_id", bean._id());
		assertEquals("/test/bucket/path/", bean.bucket_path());
		assertFalse(bean.suspended());
		assertNotNull(bean.quarantined_until());
		assertEquals(bean.node_affinity(), Arrays.asList("host"));
		assertEquals(true, bean.confirmed_suspended());
		assertEquals(false, bean.confirmed_multi_node_enabled());
		assertEquals(MasterEnrichmentType.batch, bean.confirmed_master_enrichment_type());
		assertTrue(bean.num_objects().equals(0L));
		assertEquals(bean.last_harvest_status_messages().get("a").command(), ImmutableMap.<String, BasicMessageBean>builder().put("a", BeanTemplateUtils.build(BasicMessageBean.class).done().get()).put("b", BeanTemplateUtils.build(BasicMessageBean.class).done().get()).build().get("a").command());
		assertEquals(bean.last_enrichment_status_messages().get("a").command(), ImmutableMap.<String, BasicMessageBean>builder().put("a", BeanTemplateUtils.build(BasicMessageBean.class).done().get()).put("b", BeanTemplateUtils.build(BasicMessageBean.class).done().get()).build().get("a").command());
		assertEquals(bean.last_storage_status_messages().get("a/b").command(), ImmutableMap.<String, BasicMessageBean>builder().put("a/b", BeanTemplateUtils.build(BasicMessageBean.class).done().get()).put("c/d", BeanTemplateUtils.build(BasicMessageBean.class).done().get()).build().get("a/b").command());
	}

}
