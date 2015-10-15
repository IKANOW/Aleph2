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
package com.ikanow.aleph2.data_import_manager.analytics.utils;

import java.util.Optional;
import java.util.concurrent.ExecutionException;

import org.junit.Before;
import org.junit.Test;

import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerStateBean;
import com.ikanow.aleph2.shared.crud.mongodb.services.MockMongoDbCrudServiceFactory;

public class TestAnalyticTriggerCrudUtils {

	ICrudService<AnalyticTriggerStateBean> _test_crud;
	
	@Before
	public void setup() throws InterruptedException, ExecutionException {
		
		MockMongoDbCrudServiceFactory factory = new MockMongoDbCrudServiceFactory();
		_test_crud = factory.getMongoDbCrudService(AnalyticTriggerStateBean.class, String.class, factory.getMongoDbCollection("test.trigger_crud"), Optional.empty(), Optional.empty(), Optional.empty());
		_test_crud.deleteDatastore().get();
	}
	
	@Test
	public void test_createActiveJobRecord() {
		
		final AnalyticThreadJobBean job = 
				BeanTemplateUtils.build(AnalyticThreadJobBean.class)
				.done().get();
		
		final DataBucketBean bucket =
				BeanTemplateUtils.build(DataBucketBean.class)
				.done().get();				
		
	}
	
	//TODO (ALEPH-12) testing
}
