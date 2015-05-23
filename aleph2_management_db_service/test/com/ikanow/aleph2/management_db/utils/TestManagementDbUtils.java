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
 ******************************************************************************/
package com.ikanow.aleph2.management_db.utils;

import static org.junit.Assert.*;

import java.util.Optional;
import java.util.concurrent.ExecutionException;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.FutureUtils.ManagementFuture;
import com.ikanow.aleph2.shared.crud.mongodb.services.MockMongoDbCrudServiceFactory;
import com.mongodb.DBCollection;

public class TestManagementDbUtils {

	public static class TestBean {
		String _id;
		String test;
	};
	
	@Test
	public void testManagementDbProxy() throws InterruptedException, ExecutionException {
		
		MockMongoDbCrudServiceFactory mock_crud_service_factory = new MockMongoDbCrudServiceFactory();
		
		ICrudService<TestBean> basic_service = 
				mock_crud_service_factory.getMongoDbCrudService(TestBean.class, String.class, 
					mock_crud_service_factory.getMongoDbCollection("test.test"), 
					Optional.empty(), Optional.empty(), Optional.empty());
		
		basic_service.deleteDatastore();
		
		assertEquals(0L, (long)basic_service.countObjects().get());
		
		// Quick check:
		
		TestBean test = BeanTemplateUtils.build(TestBean.class).with("_id", "test").with("test", "string").done().get();
		
		basic_service.storeObject(test);
		
		assertEquals(1L, (long)basic_service.countObjects().get());
		
		// Wrap
		
		IManagementCrudService<TestBean> wrapped_service = ManagementDbUtils.wrap(basic_service);

		// (response with future - wrapped)
		
		assertEquals(1L, (long)wrapped_service.countObjects().get());
		
		ManagementFuture<Optional<TestBean>> response = wrapped_service.getObjectById("test");
		
		assertTrue("Is a management future", response instanceof ManagementFuture);
		
		assertTrue("Management utils aren't null", null != response.getManagementResults());
		
		assertTrue("Management utils are empty", response.getManagementResults().get().isEmpty());
		
		assertEquals("test", response.get().get()._id);
		
		// (response with no future - left alone or wrapped...)
		
		IManagementCrudService<JsonNode> wrapped_raw_service = wrapped_service.getRawCrudService();
		
		assertEquals(1L, (long)wrapped_raw_service.countObjects().get());
		
		ManagementFuture<Optional<JsonNode>> response_2 = wrapped_raw_service.getObjectById("test");
		
		assertTrue("Is a management future", response_2 instanceof ManagementFuture);
		
		assertTrue("Management utils aren't null", null != response_2.getManagementResults());
		
		assertTrue("Management utils are empty", response_2.getManagementResults().get().isEmpty());
		
		assertEquals("test", response_2.get().get().get("_id").asText());
		
		// (finally - left alone altogether)
		
		DBCollection dbc = wrapped_service.getUnderlyingPlatformDriver(DBCollection.class, Optional.empty());
		
		assertEquals(dbc.getName(), "test");
	}
	
}
