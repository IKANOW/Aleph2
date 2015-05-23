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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import org.junit.Before;
import org.junit.Test;

import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.MockServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.FutureUtils.ManagementFuture;
import com.ikanow.aleph2.data_model.utils.UuidUtils;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.distributed_services.services.MockCoreDistributedServices;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionRetryMessage;
import com.ikanow.aleph2.management_db.mongodb.services.MockMongoDbManagementDbService;
import com.ikanow.aleph2.shared.crud.mongodb.services.MockMongoDbCrudServiceFactory;
import com.mongodb.DBCollection;

public class TestDataBucketCrudService {

	/////////////////////////////////////////////////////////////
	
	// Some test infrastructure
	
	// This is everything DI normally does for you!
	public MockMongoDbManagementDbService _underlying_db_service;
	public CoreManagementDbService _core_db_service;
	public MockServiceContext _mock_service_context;
	public MockMongoDbCrudServiceFactory _crud_factory;
	public DataBucketCrudService _bucket_crud;
	public ManagementDbActorContext _db_actor_context;
	public ICoreDistributedServices _core_distributed_services;
	public ICrudService<DataBucketBean> _underlying_bucket_crud;
	public ICrudService<DataBucketStatusBean> _underlying_bucket_status_crud;
	public ICrudService<BucketActionRetryMessage> _bucket_action_retry_store;
	
	@Before
	public void setup() throws Exception {
		
		// Here's the setup that Guice normally gives you....
		_mock_service_context = new MockServiceContext();		
		_crud_factory = new MockMongoDbCrudServiceFactory();
		_underlying_db_service = new MockMongoDbManagementDbService(_crud_factory);
		_core_distributed_services = new MockCoreDistributedServices();
		_mock_service_context.addService(GlobalPropertiesBean.class, Optional.empty(), new GlobalPropertiesBean(null, null, null, null));
		_mock_service_context.addService(IManagementDbService.class, Optional.empty(), _underlying_db_service);		
		_db_actor_context = new ManagementDbActorContext(_core_distributed_services, _mock_service_context, new LocalBucketActionMessageBus());
		_bucket_crud = new DataBucketCrudService(_mock_service_context, _db_actor_context);
		_core_db_service = new CoreManagementDbService(_mock_service_context, _bucket_crud);
		_mock_service_context.addService(IManagementDbService.class, Optional.of("CoreManagementDbService"), _core_db_service);		
		
		_underlying_bucket_crud = _bucket_crud._underlying_data_bucket_db;
		_underlying_bucket_status_crud = _bucket_crud._underlying_data_bucket_status_db;
		_bucket_action_retry_store = _bucket_crud._bucket_action_retry_store;
	}	
	
	//TODO add actor

	// Bucket insertion
	
	public void cleanDatabases() {
		
		_underlying_bucket_crud.deleteDatastore();
		_underlying_bucket_status_crud.deleteDatastore();
	}
	
	/**
	 * @param id
	 * @param multi_node_enabled
	 * @param node_affinity - leave this null to create a bucket _without_ its corresponding status object
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	public void insertBucket(final int id, final boolean multi_node_enabled, List<String> node_affinity, boolean suspended, Date quarantined) throws InterruptedException, ExecutionException
	{
		final DataBucketBean bucket = BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::_id, "id" + id)
				.with(DataBucketBean::full_name, "/bucket/path/here/" + id)
				.with(DataBucketBean::display_name, "Test Bucket ")
				.with(DataBucketBean::harvest_technology_name_or_id, "/app/aleph2/library/import/harvest/tech/here/" + id)
				.with(DataBucketBean::multi_node_enabled, multi_node_enabled)
				.with(DataBucketBean::tags, Collections.emptySet())
				.with(DataBucketBean::owner_id, UuidUtils.get().getRandomUuid())
				.with(DataBucketBean::access_rights, new AuthorizationBean())
				.done().get();
		
		_underlying_bucket_crud.storeObject(bucket).get(); // (ensure exception on failure)
		
		//DEBUG
		//System.out.println(raw_bucket_crud.getRawCrudService().getObjectBySpec(CrudUtils.anyOf()).get().get());
		
		if (null != node_affinity) {

			final DataBucketStatusBean status = BeanTemplateUtils.build(DataBucketStatusBean.class)
													.with(DataBucketStatusBean::_id, bucket._id())
													.with(DataBucketStatusBean::suspended, suspended)
													.with(DataBucketStatusBean::quarantined_until, quarantined)
													.with(DataBucketStatusBean::num_objects, 0L)
													.with(DataBucketStatusBean::node_affinity, node_affinity)
													.done().get();													
			
			_underlying_bucket_status_crud.storeObject(status).get(); // (ensure exception on failure)
		}
	}
	
	/////////////////////////////////////////////////////////////
	
	// General idea in each case:
	
	// Going to manipulate a DB entry via the core management DB
	// The "test infrastructure" actor is going to listen in and respond
	// Check that - 1) a response was retrieved, 2) the underlying DB entry was updated (except possibly where an error occurred)
	
	@Test
	public void testSingleDeleteById() throws InterruptedException, ExecutionException, ClassNotFoundException {
	
		cleanDatabases();
		
		assertEquals(0L, (long)_underlying_bucket_crud.countObjects().get());
		assertEquals(0L, (long)_underlying_bucket_status_crud.countObjects().get());				
		
		insertBucket(1, true, Arrays.asList("host1"), false, null);
		
		assertEquals(1L, (long)_underlying_bucket_crud.countObjects().get());
		assertEquals(1L, (long)_underlying_bucket_status_crud.countObjects().get());				
		
		ManagementFuture<Boolean> ret_val = _bucket_crud.deleteObjectById("id1");
		
		/**/
		//DEBUG
		ret_val.getManagementResults().get().stream().map(b -> BeanTemplateUtils.toJson(b)).forEach(bj -> System.out.println("REPLY MESSAGE: " + bj.toString()));
		
		assertTrue("Delete succeeds", ret_val.get());
		
		//TODO other stuff relating to actors...

		// After deletion:
		
		assertEquals(0L, (long)_underlying_bucket_crud.countObjects().get());
		assertEquals(0L, (long)_underlying_bucket_status_crud.countObjects().get());				
		
		//TODO: move this to the timeout version
		
		assertEquals(1L, (long)_bucket_action_retry_store.countObjects().get());
		
		/**/
		System.out.println("??? " + _bucket_action_retry_store.getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).getName());
		
		//TODO: should put this in the 
		/**/
		//DEBUG
		BucketActionRetryMessage retry = _bucket_action_retry_store.getObjectBySpec(CrudUtils.anyOf(BucketActionRetryMessage.class)).get().get();
		System.out.println("retry=" + BeanTemplateUtils.toJson(retry));
		System.out.println("retry message=" + retry.message());
		BucketActionMessage to_retry = (BucketActionMessage)BeanTemplateUtils.from(retry.message(), Class.forName(retry.message_clazz())).get();
		System.out.println("Actual class=" + to_retry.getClass());
		System.out.println("Bucket id=" + to_retry.bucket()._id());
		System.out.println("Back other way=" + BeanTemplateUtils.toJson(to_retry));
	}
	
	//TODO don't forget to test "not present" cases (ie wrong id)
	//TODO test deletion attempts when nobody is listening (what _should_ it return?!)
	
	@Test
	public void testSingleDeleteBySpec() {
		//TODO
	}
	
	@Test
	public void testMultiDelete() {
		//TODO
	}
	
	/////////////////////////////////////////////////////////////	
		
	//TODO test random things that work (eg count)
	
	//TODO test random things that don't work, just to get coverage
}
