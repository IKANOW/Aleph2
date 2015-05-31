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
package com.ikanow.aleph2.management_db.services;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Optional;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.MockServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.UuidUtils;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionRetryMessage;
import com.ikanow.aleph2.management_db.mongodb.data_model.MongoDbManagementDbConfigBean;
import com.ikanow.aleph2.management_db.mongodb.services.MockMongoDbManagementDbService;
import com.ikanow.aleph2.shared.crud.mongodb.services.MockMongoDbCrudServiceFactory;

public class TestCoreManagementDbModule {

	// This is everything DI normally does for you!
	private MockMongoDbManagementDbService _underlying_db_service;
	private CoreManagementDbService _core_db_service;
	private MockServiceContext _mock_service_context;
	private MockMongoDbCrudServiceFactory _crud_factory;
	private DataBucketCrudService _bucket_crud;
	private DataBucketStatusCrudService _bucket_status_crud;
	private SharedLibraryCrudService _shared_library_crud;
	
	@Test
	public void testRetryDataStore() throws Exception {
		
		// A bunch of DI related setup:
		// Here's the setup that Guice normally gives you....
		_mock_service_context = new MockServiceContext();		
		_crud_factory = new MockMongoDbCrudServiceFactory();
		_underlying_db_service = new MockMongoDbManagementDbService(_crud_factory, new MongoDbManagementDbConfigBean(false), null, null);
		_mock_service_context.addService(GlobalPropertiesBean.class, Optional.empty(), new GlobalPropertiesBean(null, null, null, null));
		_mock_service_context.addService(IManagementDbService.class, Optional.empty(), _underlying_db_service);		
		_core_db_service = new CoreManagementDbService(_mock_service_context, _bucket_crud, _bucket_status_crud, _shared_library_crud);
		_mock_service_context.addService(IManagementDbService.class, Optional.of("CoreManagementDbService"), _core_db_service);		
		
		ICrudService<BucketActionRetryMessage> retry_service = _core_db_service.getRetryStore(BucketActionRetryMessage.class);
		
		assertTrue("Retry service non null", retry_service != null);
		
		// Build the message to store:
		
		final DataBucketBean bucket = BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::_id, "id" + 1)
				.with(DataBucketBean::full_name, "/bucket/path/here/" + 1)
				.with(DataBucketBean::display_name, "Test Bucket ")
				.with(DataBucketBean::harvest_technology_name_or_id, "/app/aleph2/library/import/harvest/tech/here/" + 1)
				.with(DataBucketBean::multi_node_enabled, false)
				.with(DataBucketBean::tags, Collections.emptySet())
				.with(DataBucketBean::owner_id, UuidUtils.get().getRandomUuid())
				.with(DataBucketBean::access_rights, new AuthorizationBean(ImmutableMap.<String, String>builder().put("auth_token", "r").build()))
				.done().get();
		
		final BucketActionMessage.DeleteBucketActionMessage test_message = new 
				BucketActionMessage.DeleteBucketActionMessage(bucket, Collections.emptySet());

		retry_service.deleteDatastore().get(); // (just clear it out from the past test)
		
		assertEquals(0L, (long)retry_service.countObjects().get());
		
		retry_service.storeObject(
				new BucketActionRetryMessage("test1",
						// (can't clone the message because it's not a bean, but the c'tor is very simple)
						new BucketActionMessage.DeleteBucketActionMessage(
								test_message.bucket(),
								new HashSet<String>(Arrays.asList("test1"))
								)										
						)).get(); // (get just to ensure completes)
		
		assertEquals(1L, (long)retry_service.countObjects().get());
		
		final Optional<BucketActionRetryMessage> retry = retry_service.getObjectBySpec(CrudUtils.anyOf(BucketActionRetryMessage.class)).get();
		assertTrue("Finds an object", retry.isPresent());
		assertTrue("Has an _id", retry.get()._id() != null);
		assertEquals((double)(new Date().getTime()), (double)retry.get().inserted().getTime(), 1000.0); 
		assertEquals((double)(new Date().getTime()), (double)retry.get().last_checked().getTime(), 1000.0);
		assertEquals(BucketActionMessage.DeleteBucketActionMessage.class.getName(), retry.get().message_clazz());
		assertEquals("test1", retry.get().source());
		final BucketActionMessage to_retry = (BucketActionMessage)BeanTemplateUtils.from(retry.get().message(), Class.forName(retry.get().message_clazz())).get();
		assertEquals("id1", to_retry.bucket()._id());
		assertEquals(new HashSet<String>(Arrays.asList("test1")), to_retry.handling_clients());
	}
}
