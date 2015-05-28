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

import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.MockServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.distributed_services.services.MockCoreDistributedServices;
import com.ikanow.aleph2.management_db.data_model.BucketActionRetryMessage;
import com.ikanow.aleph2.management_db.mongodb.data_model.MongoDbManagementDbConfigBean;
import com.ikanow.aleph2.management_db.mongodb.services.MockMongoDbManagementDbService;
import com.ikanow.aleph2.shared.crud.mongodb.services.MockMongoDbCrudServiceFactory;
import com.ikanow.aleph2.storage_service_hdfs.services.MockHdfsStorageService;
import com.sun.istack.internal.logging.Logger;

public class TestDataBucketStatusCrudService {

	public static final Logger _logger = Logger.getLogger(TestDataBucketStatusCrudService.class);	
	
	/////////////////////////////////////////////////////////////
	
	// Some test infrastructure
	
	// This is everything DI normally does for you!
	public GlobalPropertiesBean _globals;
	public MockHdfsStorageService _storage_service;
	public MockMongoDbManagementDbService _underlying_db_service;
	public CoreManagementDbService _core_db_service;
	public MockServiceContext _mock_service_context;
	public MockMongoDbCrudServiceFactory _crud_factory;
	public DataBucketCrudService _bucket_crud;
	public DataBucketStatusCrudService _bucket_status_crud;
	public SharedLibraryCrudService _shared_library_crud;
	public ManagementDbActorContext _db_actor_context;
	public ICoreDistributedServices _core_distributed_services;
	public ICrudService<DataBucketBean> _underlying_bucket_crud;
	public ICrudService<DataBucketStatusBean> _underlying_bucket_status_crud;
	public ICrudService<BucketActionRetryMessage> _bucket_action_retry_store;
	
	@Before
	public void setup() throws Exception {
		
		// Here's the setup that Guice normally gives you....
		final String tmpdir = System.getProperty("java.io.tmpdir");
		_globals = new GlobalPropertiesBean(tmpdir, tmpdir, tmpdir, tmpdir);
		_storage_service = new MockHdfsStorageService(_globals);
		_mock_service_context = new MockServiceContext();		
		_crud_factory = new MockMongoDbCrudServiceFactory();
		_underlying_db_service = new MockMongoDbManagementDbService(_crud_factory, new MongoDbManagementDbConfigBean(false), null);
		_core_distributed_services = new MockCoreDistributedServices();
		_mock_service_context.addService(GlobalPropertiesBean.class, Optional.empty(), new GlobalPropertiesBean(null, null, null, null));
		_mock_service_context.addService(IManagementDbService.class, Optional.empty(), _underlying_db_service);
		_mock_service_context.addService(ICoreDistributedServices.class, Optional.empty(), _core_distributed_services);
		_mock_service_context.addService(IStorageService.class, Optional.empty(),_storage_service);
		_db_actor_context = new ManagementDbActorContext(_mock_service_context, new LocalBucketActionMessageBus());
		_bucket_crud = new DataBucketCrudService(_mock_service_context, _db_actor_context);
		_bucket_status_crud = new DataBucketStatusCrudService(_mock_service_context, _db_actor_context);
		_shared_library_crud = new SharedLibraryCrudService(_mock_service_context);
		_core_db_service = new CoreManagementDbService(_mock_service_context, _bucket_crud, _bucket_status_crud, _shared_library_crud);
		_mock_service_context.addService(IManagementDbService.class, Optional.of("CoreManagementDbService"), _core_db_service);		
		
		_underlying_bucket_crud = _bucket_crud._underlying_data_bucket_db;
		_underlying_bucket_status_crud = _bucket_crud._underlying_data_bucket_status_db;
		_bucket_action_retry_store = _bucket_crud._bucket_action_retry_store;
	}	
	
	// Bucket insertion
	
	public void cleanDatabases() {
		
		_underlying_bucket_crud.deleteDatastore();
		_underlying_bucket_status_crud.deleteDatastore();
		_bucket_action_retry_store.deleteDatastore();
	}
	
	/////////////////////////////////////////////////////////////	
	/////////////////////////////////////////////////////////////	
	/////////////////////////////////////////////////////////////	
	
	// Store bucket
	
	@Test
	public void testValidateInsert() throws Exception {
		cleanDatabases();
		
		final DataBucketStatusBean valid_status = 
				BeanTemplateUtils.build(DataBucketStatusBean.class)
				.with(DataBucketStatusBean::_id, "id1")
				.with(DataBucketStatusBean::bucket_path, "name1")
				.with(DataBucketStatusBean::suspended, false)
				.done().get();

		// Will fail (no _id)
		
		final DataBucketStatusBean invalid_status1 = BeanTemplateUtils.clone(valid_status).with(DataBucketStatusBean::_id, null).done();
		
		assertEquals(0L, (long)_bucket_status_crud.countObjects().get());
		try {
			_bucket_status_crud.storeObject(invalid_status1).get();
			fail("Should throw exception");
		}
		catch (Exception e) {}
		assertEquals(0L, (long)_bucket_status_crud.countObjects().get());		
		
		// Will fail (no suspended)
		
		final DataBucketStatusBean invalid_status2 = BeanTemplateUtils.clone(valid_status).with(DataBucketStatusBean::suspended, null).done();
		
		assertEquals(0L, (long)_bucket_status_crud.countObjects().get());
		try {
			_bucket_status_crud.storeObject(invalid_status2).get();
			fail("Should throw exception");
		}
		catch (Exception e) {}
		assertEquals(0L, (long)_bucket_status_crud.countObjects().get());		
		
		// Will fail (no bucket path)
		
		final DataBucketStatusBean invalid_status3 = BeanTemplateUtils.clone(valid_status).with(DataBucketStatusBean::bucket_path, null).done();
		
		assertEquals(0L, (long)_bucket_status_crud.countObjects().get());
		try {
			_bucket_status_crud.storeObject(invalid_status3).get();
			fail("Should throw exception");
		}
		catch (Exception e) {}
		assertEquals(0L, (long)_bucket_status_crud.countObjects().get());		
	}
	
	@Test
	public void testValidateUpdate() throws Exception {
		cleanDatabases();
		
		final DataBucketStatusBean valid_status = 
				BeanTemplateUtils.build(DataBucketStatusBean.class)
				.with(DataBucketStatusBean::_id, "id1")
				.with(DataBucketStatusBean::bucket_path, "name1")
				.with(DataBucketStatusBean::suspended, false)
				.done().get();
		
		assertEquals(0L, (long)_bucket_status_crud.countObjects().get());
		_bucket_status_crud.storeObject(valid_status).get();
		assertEquals(1L, (long)_bucket_status_crud.countObjects().get());		
		
		//TODO - set number of objects
		
		//TODO - set any of: last_harvest_status_messages, last_enrichment_status_messages, last_storage_status_messages
		
		//TODO - other fields - generate an illegal command (eg bucket_path)
	}
	
	//TODO update status (quarantine and suspended)
	
	//TODO multi-insert (lower prio)
	
	//TODO: pass through functions for coverage
}
