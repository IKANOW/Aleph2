/*******************************************************************************
* Copyright 2015, The IKANOW Open Source Project.
* 
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License, version 3,
* as published by the Free Software Foundation.
* 
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Affero General Public License for more details.
* 
* You should have received a copy of the GNU Affero General Public License
* along with this program. If not, see <http://www.gnu.org/licenses/>.
******************************************************************************/
package com.ikanow.aleph2.data_import_manager.data_model;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashSet;

import org.junit.Test;

public class TestDataImportConfigurationBean {

	@Test
	public void test_dataImportConfigBean() {
		
		assertEquals("DataImportManager", DataImportConfigurationBean.PROPERTIES_ROOT);
		
		{
			final DataImportConfigurationBean x = new DataImportConfigurationBean(true, false, true, new HashSet<String>());
			
			assertEquals(true, x.harvest_enabled());
			assertEquals(false, x.analytics_enabled());
			assertEquals(true, x.governance_enabled());
			assertEquals(0, x.node_rules().size());
		}
		{
			final DataImportConfigurationBean x = new DataImportConfigurationBean(false, true, false, new HashSet<String>(Arrays.asList("a","b")));
			
			assertEquals(false, x.harvest_enabled());
			assertEquals(true, x.analytics_enabled());
			assertEquals(false, x.governance_enabled());
			assertEquals(2, x.node_rules().size());
		}
		{
			final DataImportConfigurationBean x = new DataImportConfigurationBean(false, false, null, new HashSet<String>(Arrays.asList("a")));
			
			assertEquals(false, x.harvest_enabled());
			assertEquals(false, x.analytics_enabled());
			assertEquals(true, x.governance_enabled());
			assertEquals(1, x.node_rules().size());
		}
		{
			final DataImportConfigurationBean x = new DataImportConfigurationBean(false, false, true, new HashSet<String>(Arrays.asList("a","b","a")));
			
			assertEquals(false, x.harvest_enabled());
			assertEquals(false, x.analytics_enabled());
			assertEquals(true, x.governance_enabled());
			assertEquals(2, x.node_rules().size());
		}
		{
			final DataImportConfigurationBean x = new DataImportConfigurationBean(null, null, null, null);
			
			assertEquals(true, x.harvest_enabled());
			assertEquals(true, x.analytics_enabled());
			assertEquals(true, x.governance_enabled());
			assertEquals(0, x.node_rules().size());
		}
		
	}
	
}
