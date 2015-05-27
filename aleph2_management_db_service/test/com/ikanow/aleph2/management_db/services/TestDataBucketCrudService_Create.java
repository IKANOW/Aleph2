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

import java.util.Date;
import java.util.Optional;

import org.junit.Before;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;

import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.MockServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.UuidUtils;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.distributed_services.services.MockCoreDistributedServices;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionRetryMessage;
import com.ikanow.aleph2.management_db.mongodb.services.MockMongoDbManagementDbService;
import com.ikanow.aleph2.management_db.utils.ActorUtils;
import com.ikanow.aleph2.shared.crud.mongodb.services.MockMongoDbCrudServiceFactory;
import com.sun.istack.internal.logging.Logger;

public class TestDataBucketCrudService_Create {

	public static final Logger _logger = Logger.getLogger(TestDataBucketCrudService_Create.class);	
	
	/////////////////////////////////////////////////////////////
	
	// Some test infrastructure
	
	// This is everything DI normally does for you!
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
		_mock_service_context = new MockServiceContext();		
		_crud_factory = new MockMongoDbCrudServiceFactory();
		_underlying_db_service = new MockMongoDbManagementDbService(_crud_factory);
		_core_distributed_services = new MockCoreDistributedServices();
		_mock_service_context.addService(GlobalPropertiesBean.class, Optional.empty(), new GlobalPropertiesBean(null, null, null, null));
		_mock_service_context.addService(IManagementDbService.class, Optional.empty(), _underlying_db_service);
		_mock_service_context.addService(ICoreDistributedServices.class, Optional.empty(), _core_distributed_services);
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
	
	// Actors:
	
	// Test actors:
	// This one always refuses
	public static class TestActor_Refuser extends UntypedActor {
		public TestActor_Refuser(String uuid) {
			this.uuid = uuid;
		}
		private final String uuid;
		@Override
		public void onReceive(Object arg0) throws Exception {
			_logger.info("Refuse from: " + uuid);
			
			this.sender().tell(new BucketActionReplyMessage.BucketActionIgnoredMessage(uuid), this.self());
		}		
	}

	// This one always accepts, and returns a message
	public static class TestActor_Accepter extends UntypedActor {
		public TestActor_Accepter(String uuid) {
			this.uuid = uuid;
		}
		private final String uuid;
		@Override
		public void onReceive(Object arg0) throws Exception {
			_logger.info("Accept from: " + uuid);
			
			this.sender().tell(
					new BucketActionReplyMessage.BucketActionHandlerMessage(uuid, 
							new BasicMessageBean(
									new Date(),
									true,
									uuid + "replaceme", // (this gets replaced by the bucket)
									arg0.getClass().getSimpleName(),
									null,
									"handled",
									null									
									)),
					this.self());
		}		
	}	
	
	public String insertActor(Class<? extends UntypedActor> actor_clazz) throws Exception {
		String uuid = UuidUtils.get().getRandomUuid();
		ManagementDbActorContext.get().getDistributedServices()
			.getCuratorFramework().create().creatingParentsIfNeeded()
			.forPath(ActorUtils.BUCKET_ACTION_ZOOKEEPER + "/" + uuid);
		
		ActorRef handler = ManagementDbActorContext.get().getActorSystem().actorOf(Props.create(actor_clazz, uuid), uuid);
		ManagementDbActorContext.get().getBucketActionMessageBus().subscribe(handler, ActorUtils.BUCKET_ACTION_EVENT_BUS);
		return uuid;
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
	
	//TODO don't forget to check node affinity has been set

	public void testCreateValidationErrors() {

		// 0) Missing field
		
		//final DataBucketBean bucket0 = BeanTemplateUtils.build(DataBucketBean.class).done().get();
		
	}
	
	public void testUpdateValidationErrors() {
		
	}
	//TODO: tests
}
