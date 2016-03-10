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
package com.ikanow.aleph2.management_db.services;

import static org.junit.Assert.*;

import java.util.Optional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import akka.actor.ActorRef;
import akka.event.japi.LookupEventBus;

import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.MockSecurityService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.MockServiceContext;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.distributed_services.services.MockCoreDistributedServices;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage.BucketActionEventBusWrapper;
import com.ikanow.aleph2.management_db.mongodb.data_model.MongoDbManagementDbConfigBean;
import com.ikanow.aleph2.management_db.mongodb.services.MockMongoDbManagementDbService;
import com.ikanow.aleph2.management_db.utils.ActorUtils;
import com.ikanow.aleph2.shared.crud.mongodb.services.MockMongoDbCrudServiceFactory;
import com.ikanow.aleph2.storage_service_hdfs.services.MockHdfsStorageService;

public class TestManagementDbActorContext {

	// This is everything DI normally does for you!
	private MockMongoDbManagementDbService _underlying_db_service;
	private CoreManagementDbService _core_db_service;
	private MockServiceContext _mock_service_context;
	private MockHdfsStorageService _mock_storage_service;
	private MockMongoDbCrudServiceFactory _crud_factory;
	private DataBucketCrudService _bucket_crud;
	private DataBucketStatusCrudService _bucket_status_crud;
	private SharedLibraryCrudService _shared_library_crud;
	private MockCoreDistributedServices _cds;
	private ManagementDbActorContext _actor_context;
	
	@After
	public void tidyUp() {
		// (kill current kafka queue)
		ManagementDbActorContext.get().getServiceContext().getService(ICoreDistributedServices.class, Optional.empty())
			.filter(x -> MockCoreDistributedServices.class.isAssignableFrom(x.getClass()))
			.map(x -> (MockCoreDistributedServices)x)
			.ifPresent(x -> x.kill());
			;
	}
	
	@SuppressWarnings("deprecation")
	@Before
	public void test_Setup() throws Exception {
		
		// A bunch of DI related setup:
		// Here's the setup that Guice normally gives you....
		_mock_service_context = new MockServiceContext();		
		_crud_factory = new MockMongoDbCrudServiceFactory();
		_underlying_db_service = new MockMongoDbManagementDbService(_crud_factory, new MongoDbManagementDbConfigBean(false), null, null, null, null);
		_mock_service_context.addGlobals(new GlobalPropertiesBean(null, null, null, null));
		_mock_storage_service = new MockHdfsStorageService(_mock_service_context.getGlobalProperties());
		_mock_service_context.addService(IStorageService.class, Optional.empty(), _mock_storage_service);		
		_mock_service_context.addService(IManagementDbService.class, Optional.empty(), _underlying_db_service);
		_cds = new MockCoreDistributedServices();
		_mock_service_context.addService(ICoreDistributedServices.class, Optional.empty(), _cds);
		_mock_service_context.addService(ISecurityService.class, Optional.empty(), new MockSecurityService());
		_actor_context = new ManagementDbActorContext(_mock_service_context, true);
		_bucket_crud = new DataBucketCrudService(_mock_service_context, _actor_context);
		_bucket_status_crud = new DataBucketStatusCrudService(_mock_service_context, _actor_context); 
		_shared_library_crud = new SharedLibraryCrudService(_mock_service_context);		
		
		_core_db_service = new CoreManagementDbService(_mock_service_context, 
				_bucket_crud, _bucket_status_crud, _shared_library_crud, _actor_context);
		_mock_service_context.addService(IManagementDbService.class, IManagementDbService.CORE_MANAGEMENT_DB, _core_db_service);		
	}
	
	@Test
	public void test__buses() {
		assertTrue("Lazy initialization: bucket action bus", !_actor_context._bucket_action_bus.isSet());
		assertTrue("Lazy initialization: streaming action bus", !_actor_context._analytics_bus.isSet());
		
		LookupEventBus<BucketActionEventBusWrapper, ActorRef, String>  bus1 = _actor_context.getBucketActionMessageBus();
		assertTrue("Lazy initialization: bucket action bus", _actor_context._bucket_action_bus.isSet());
		
		LookupEventBus<BucketActionEventBusWrapper, ActorRef, String>  bus2 = _actor_context.getAnalyticsMessageBus();
		assertTrue("Lazy initialization: streaming action bus", _actor_context._analytics_bus.isSet());		
		
		LookupEventBus<BucketActionEventBusWrapper, ActorRef, String>  bus3 = _actor_context.getMessageBus(ActorUtils.BUCKET_ACTION_EVENT_BUS);
		LookupEventBus<BucketActionEventBusWrapper, ActorRef, String>  bus4 = _actor_context.getMessageBus(ActorUtils.BUCKET_ANALYTICS_EVENT_BUS);
		
		assertEquals(bus1, bus3);
		assertEquals(bus2, bus4);
		
		try {
			_actor_context.getMessageBus("fail");
			fail("Should have thrown exception");
		}
		catch (Exception e) {}
	}
	
}
