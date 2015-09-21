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

import org.junit.Test;

public class TestDataImportConfigurationBean {

	@Test
	public void test_dataImportConfigBean() {
		
		assertEquals("DataImportManager", DataImportConfigurationBean.PROPERTIES_ROOT);
		
		{
			final DataImportConfigurationBean x = new DataImportConfigurationBean(true, false, false, true);
			
			assertEquals(true, x.harvest_enabled());
			assertEquals(false, x.streaming_enrichment_enabled());
			assertEquals(false, x.batch_enrichment_enabled());
			assertEquals(true, x.governance_enabled());
		}
		{
			final DataImportConfigurationBean x = new DataImportConfigurationBean(false, true, false, false);
			
			assertEquals(false, x.harvest_enabled());
			assertEquals(true, x.streaming_enrichment_enabled());
			assertEquals(false, x.batch_enrichment_enabled());
			assertEquals(false, x.governance_enabled());
		}
		{
			final DataImportConfigurationBean x = new DataImportConfigurationBean(false, false, true, null);
			
			assertEquals(false, x.harvest_enabled());
			assertEquals(false, x.streaming_enrichment_enabled());
			assertEquals(true, x.batch_enrichment_enabled());
			assertEquals(true, x.governance_enabled());
		}
		{
			final DataImportConfigurationBean x = new DataImportConfigurationBean(false, false, false, true);
			
			assertEquals(false, x.harvest_enabled());
			assertEquals(false, x.streaming_enrichment_enabled());
			assertEquals(false, x.batch_enrichment_enabled());
			assertEquals(true, x.governance_enabled());
		}
		{
			final DataImportConfigurationBean x = new DataImportConfigurationBean(null, null, null, null);
			
			assertEquals(true, x.harvest_enabled());
			assertEquals(true, x.streaming_enrichment_enabled());
			assertEquals(true, x.batch_enrichment_enabled());
			assertEquals(true, x.governance_enabled());
		}
		
	}
	
}
