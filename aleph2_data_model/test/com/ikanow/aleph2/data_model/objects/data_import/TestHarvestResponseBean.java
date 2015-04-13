package com.ikanow.aleph2.data_model.objects.data_import;

import static org.junit.Assert.*;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class TestHarvestResponseBean {

	@Test
	public void testHarvestResponseBean() {
		
		// Create using public constructor
		
		HarvestResponseBean bean = new HarvestResponseBean(true, "test_command", 200, "test_message",
				ImmutableMap.<String, String>builder().put("a", "1").put("b", "2").build()
				);
		
		assertEquals(bean.success(), true);
		assertEquals(bean.command(), "test_command");
		assertEquals(bean.code(), (Integer)200);
		assertEquals(bean.message(), "test_message");
		assertEquals(bean.details(), ImmutableMap.<String, String>builder().put("a", "1").put("b", "2").build());
		
	}
	
}
