package com.ikanow.aleph2.data_model.objects.data_import;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Date;

import org.junit.Test;

import scala.Tuple2;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;

public class TestDataBucketStatusBean {

	@Test
	public void testDataBucketStatusBean() {
		DataBucketStatusBean bean = new DataBucketStatusBean(
				"test_id", false, new Date(), 0L, Arrays.asList("host"),
				ImmutableMap.<String, BasicMessageBean>builder().put("a", BeanTemplateUtils.build(BasicMessageBean.class).done().get()).put("b", BeanTemplateUtils.build(BasicMessageBean.class).done().get()).build(), 
				ImmutableMap.<String, BasicMessageBean>builder().put("a", BeanTemplateUtils.build(BasicMessageBean.class).done().get()).put("b", BeanTemplateUtils.build(BasicMessageBean.class).done().get()).build(), 
				ImmutableMap.<Tuple2<String, String>, BasicMessageBean>builder().put(new Tuple2<String,String>("a", "b"), BeanTemplateUtils.build(BasicMessageBean.class).done().get()).put(new Tuple2<String,String>("c", "d"), BeanTemplateUtils.build(BasicMessageBean.class).done().get()).build(), 
				ImmutableMultimap.<String, BasicMessageBean>builder().put("a", BeanTemplateUtils.build(BasicMessageBean.class).done().get()).put("b", BeanTemplateUtils.build(BasicMessageBean.class).done().get()).build(), 
				ImmutableMultimap.<String, BasicMessageBean>builder().put("a", BeanTemplateUtils.build(BasicMessageBean.class).done().get()).put("b", BeanTemplateUtils.build(BasicMessageBean.class).done().get()).build(), 
				ImmutableMultimap.<String, BasicMessageBean>builder().put("a", BeanTemplateUtils.build(BasicMessageBean.class).done().get()).put("b", BeanTemplateUtils.build(BasicMessageBean.class).done().get()).build());
		
		assertEquals("test_id", bean._id());
		assertFalse(bean.suspended());
		assertNotNull(bean.quarantined_until());
		assertEquals(bean.node_affinity(), Arrays.asList("host"));
		assertTrue(bean.num_objects().equals(0L));
		assertEquals(bean.last_harvest_status_messages().get("a").command(), ImmutableMap.<String, BasicMessageBean>builder().put("a", BeanTemplateUtils.build(BasicMessageBean.class).done().get()).put("b", BeanTemplateUtils.build(BasicMessageBean.class).done().get()).build().get("a").command());
		assertEquals(bean.last_enrichment_status_messages().get("a").command(), ImmutableMap.<String, BasicMessageBean>builder().put("a", BeanTemplateUtils.build(BasicMessageBean.class).done().get()).put("b", BeanTemplateUtils.build(BasicMessageBean.class).done().get()).build().get("a").command());
		assertEquals(bean.last_storage_status_messages().get(new Tuple2<String, String>("a", "b")).command(), ImmutableMap.<Tuple2<String, String>, BasicMessageBean>builder().put(new Tuple2<String,String>("a", "b"), BeanTemplateUtils.build(BasicMessageBean.class).done().get()).put(new Tuple2<String,String>("c", "d"), BeanTemplateUtils.build(BasicMessageBean.class).done().get()).build().get(new Tuple2<String, String>("a", "b")).command());
		bean.harvest_log_messages().get("a").forEach(messageBean -> assertEquals(messageBean.command(), BeanTemplateUtils.build(BasicMessageBean.class).done().get().command()));
		bean.enrichment_log_messages().get("a").forEach(messageBean -> assertEquals(messageBean.command(), BeanTemplateUtils.build(BasicMessageBean.class).done().get().command()));
		bean.storage_log_messages().get("a").forEach(messageBean -> assertEquals(messageBean.command(), BeanTemplateUtils.build(BasicMessageBean.class).done().get().command()));		
	}

}
