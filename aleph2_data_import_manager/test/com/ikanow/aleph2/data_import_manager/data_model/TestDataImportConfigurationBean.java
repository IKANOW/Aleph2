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
package com.ikanow.aleph2.data_import_manager.data_model;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.elasticsearch.common.collect.ImmutableMap;
import org.junit.Test;

public class TestDataImportConfigurationBean {

	@Test
	public void test_dataImportConfigBean() {
		
		assertEquals("DataImportManager", DataImportConfigurationBean.PROPERTIES_ROOT);
		
		{
			final DataImportConfigurationBean x = new DataImportConfigurationBean(true, false, true, new HashSet<String>(), Collections.emptyMap());
			
			assertEquals(true, x.harvest_enabled());
			assertEquals(false, x.analytics_enabled());
			assertEquals(true, x.governance_enabled());
			assertEquals(0, x.node_rules().size());
			assertEquals(0, x.registered_technologies().size());
		}
		{
			final DataImportConfigurationBean x = new DataImportConfigurationBean(false, true, false, new HashSet<String>(Arrays.asList("a","b")), null);
			
			assertEquals(false, x.harvest_enabled());
			assertEquals(true, x.analytics_enabled());
			assertEquals(false, x.governance_enabled());
			assertEquals(2, x.node_rules().size());
			assertEquals(0, x.registered_technologies().size());
		}
		{
			final DataImportConfigurationBean x = new DataImportConfigurationBean(false, false, null, new HashSet<String>(Arrays.asList("a")), ImmutableMap.of("a", "b"));
			
			assertEquals(false, x.harvest_enabled());
			assertEquals(false, x.analytics_enabled());
			assertEquals(true, x.governance_enabled());
			assertEquals(1, x.node_rules().size());
			assertEquals(1, x.registered_technologies().size());
		}
		{
			final DataImportConfigurationBean x = new DataImportConfigurationBean(false, false, true, new HashSet<String>(Arrays.asList("a","b","a")), null);
			
			assertEquals(false, x.harvest_enabled());
			assertEquals(false, x.analytics_enabled());
			assertEquals(true, x.governance_enabled());
			assertEquals(2, x.node_rules().size());
			assertEquals(0, x.registered_technologies().size());
		}
		{
			final DataImportConfigurationBean x = new DataImportConfigurationBean(null, null, null, null, null);
			
			assertEquals(true, x.harvest_enabled());
			assertEquals(true, x.analytics_enabled());
			assertEquals(true, x.governance_enabled());
			assertEquals(0, x.node_rules().size());
			assertEquals(0, x.registered_technologies().size());
		}
		
	}
	
}
