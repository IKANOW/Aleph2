package com.ikanow.aleph2.data_model.objects.data_import;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Date;

import org.junit.Test;

import scala.Tuple2;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;

public class TestDataBucketStatusBean {

	@Test
	public void testDataBucketStatusBean() {
		DataBucketStatusBean bean = new DataBucketStatusBean(
				"test_id", false, new Date(), 0L, Arrays.asList("host"),
				ImmutableMap.<String, BasicMessageBean>builder().put("a", new BasicMessageBean()).put("b", new BasicMessageBean()).build(), 
				ImmutableMap.<String, BasicMessageBean>builder().put("a", new BasicMessageBean()).put("b", new BasicMessageBean()).build(), 
				ImmutableMap.<Tuple2<String, String>, BasicMessageBean>builder().put(new Tuple2<String,String>("a", "b"), new BasicMessageBean()).put(new Tuple2<String,String>("c", "d"), new BasicMessageBean()).build(), 
				ImmutableMultimap.<String, BasicMessageBean>builder().put("a", new BasicMessageBean()).put("b", new BasicMessageBean()).build(), 
				ImmutableMultimap.<String, BasicMessageBean>builder().put("a", new BasicMessageBean()).put("b", new BasicMessageBean()).build(), 
				ImmutableMultimap.<String, BasicMessageBean>builder().put("a", new BasicMessageBean()).put("b", new BasicMessageBean()).build());
		
		assertEquals("test_id", bean._id());
		assertFalse(bean.suspended());
		assertNotNull(bean.quarantined_until());
		assertEquals(bean.node_affinity(), Arrays.asList("host"));
		assertTrue(bean.num_objects().equals(0L));
		assertEquals(bean.last_harvest_status_messages().get("a").command(), ImmutableMap.<String, BasicMessageBean>builder().put("a", new BasicMessageBean()).put("b", new BasicMessageBean()).build().get("a").command());
		assertEquals(bean.last_enrichment_status_messages().get("a").command(), ImmutableMap.<String, BasicMessageBean>builder().put("a", new BasicMessageBean()).put("b", new BasicMessageBean()).build().get("a").command());
		assertEquals(bean.last_storage_status_messages().get(new Tuple2<String, String>("a", "b")).command(), ImmutableMap.<Tuple2<String, String>, BasicMessageBean>builder().put(new Tuple2<String,String>("a", "b"), new BasicMessageBean()).put(new Tuple2<String,String>("c", "d"), new BasicMessageBean()).build().get(new Tuple2<String, String>("a", "b")).command());
		bean.harvest_log_messages().get("a").forEach(messageBean -> assertEquals(messageBean.command(), new BasicMessageBean().command()));
		bean.enrichment_log_messages().get("a").forEach(messageBean -> assertEquals(messageBean.command(), new BasicMessageBean().command()));
		bean.storage_log_messages().get("a").forEach(messageBean -> assertEquals(messageBean.command(), new BasicMessageBean().command()));		
	}

}
