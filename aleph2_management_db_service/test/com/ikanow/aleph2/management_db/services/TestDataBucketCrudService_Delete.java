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

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;

import com.google.common.collect.ImmutableMap;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.MockServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.FutureUtils.ManagementFuture;
import com.ikanow.aleph2.data_model.utils.UuidUtils;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.distributed_services.services.MockCoreDistributedServices;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionRetryMessage;
import com.ikanow.aleph2.management_db.data_model.BucketMgmtMessage.BucketDeletionMessage;
import com.ikanow.aleph2.management_db.mongodb.data_model.MongoDbManagementDbConfigBean;
import com.ikanow.aleph2.management_db.mongodb.services.MockMongoDbManagementDbService;
import com.ikanow.aleph2.management_db.utils.ActorUtils;
import com.ikanow.aleph2.shared.crud.mongodb.services.MockMongoDbCrudServiceFactory;
import com.ikanow.aleph2.storage_service_hdfs.services.MockHdfsStorageService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;

public class TestDataBucketCrudService_Delete {

	public static final Logger _logger = LogManager.getLogger(TestDataBucketCrudService_Delete.class);	
	
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
	public ICrudService<BucketDeletionMessage> _bucket_deletion_queue;
	
	@Before
	public void setup() throws Exception {
		
		// Here's the setup that Guice normally gives you....
		final String tmpdir = System.getProperty("java.io.tmpdir") + File.separator;
		_globals = new GlobalPropertiesBean(tmpdir, tmpdir, tmpdir, tmpdir);
		_storage_service = new MockHdfsStorageService(_globals);
		_mock_service_context = new MockServiceContext();		
		_crud_factory = new MockMongoDbCrudServiceFactory();
		_underlying_db_service = new MockMongoDbManagementDbService(_crud_factory, new MongoDbManagementDbConfigBean(false), null, null);
		_core_distributed_services = new MockCoreDistributedServices();
		_mock_service_context.addGlobals(new GlobalPropertiesBean(null, null, null, null));
		_mock_service_context.addService(IManagementDbService.class, Optional.empty(), _underlying_db_service);
		_mock_service_context.addService(ICoreDistributedServices.class, Optional.empty(), _core_distributed_services);
		_mock_service_context.addService(IStorageService.class, Optional.empty(),_storage_service);
		_db_actor_context = new ManagementDbActorContext(_mock_service_context);
		_bucket_crud = new DataBucketCrudService(_mock_service_context, _db_actor_context);
		_bucket_status_crud = new DataBucketStatusCrudService(_mock_service_context, _db_actor_context);
		_shared_library_crud = new SharedLibraryCrudService(_mock_service_context);
		_core_db_service = new CoreManagementDbService(_mock_service_context, _bucket_crud, _bucket_status_crud, _shared_library_crud);
		_mock_service_context.addService(IManagementDbService.class, IManagementDbService.CORE_MANAGEMENT_DB, _core_db_service);		
		
		_underlying_bucket_crud = _bucket_crud._underlying_data_bucket_db;
		_underlying_bucket_status_crud = _bucket_crud._underlying_data_bucket_status_db;
		_bucket_action_retry_store = _bucket_crud._bucket_action_retry_store;
		_bucket_deletion_queue = _bucket_crud._bucket_deletion_queue;
	}	
	
	@Test
	public void testSetup() {
		if (File.separator.equals("\\")) { // windows mode!
			assertTrue("WINDOWS MODE: hadoop home needs to be set (use -Dhadoop.home.dir={HADOOP_HOME} in JAVA_OPTS)", null != System.getProperty("hadoop.home.dir"));
			assertTrue("WINDOWS MODE: hadoop home needs to exist: " + System.getProperty("hadoop.home.dir"), null != System.getProperty("hadoop.home.dir"));
		}		
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
			.getCuratorFramework().create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
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
		_bucket_deletion_queue.deleteDatastore();
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
				.with(DataBucketBean::access_rights, new AuthorizationBean(ImmutableMap.<String, String>builder().put("auth_token", "r").build()))
				.done().get();
		
		//(create a file path also)
		try {
			FileUtils.deleteDirectory(new File(System.getProperty("java.io.tmpdir") + File.separator + bucket.full_name()));
		}
		catch (Exception e) {} // (fine, dir prob dones't delete)
		try {
			new File(System.getProperty("java.io.tmpdir") + File.separator +  bucket.full_name()).mkdirs();
		}
		catch (Exception e) {} // (fine, dir prob dones't delete)
		
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
	/////////////////////////////////////////////////////////////	
	/////////////////////////////////////////////////////////////	
	
	// Single delete	
	
	// General idea in each case:
	
	// Going to manipulate a DB entry via the core management DB
	// The "test infrastructure" actor is going to listen in and respond
	// Check that - 1) a response was retrieved, 2) the underlying DB entry was updated (except possibly where an error occurred)
	
	@Test
	public void testSingleDeleteById_timeout() throws InterruptedException, ExecutionException, ClassNotFoundException {
	
		cleanDatabases();
		
		assertEquals(0L, (long)_underlying_bucket_crud.countObjects().get());
		assertEquals(0L, (long)_underlying_bucket_status_crud.countObjects().get());				
		assertEquals(0L, (long)_bucket_action_retry_store.countObjects().get());
		assertEquals(0L, (long)_bucket_deletion_queue.countObjects().get());
		
		insertBucket(1, true, Arrays.asList("host1"), false, null);
		
		assertEquals(1L, (long)_underlying_bucket_crud.countObjects().get());
		assertEquals(1L, (long)_underlying_bucket_status_crud.countObjects().get());				
		
		final Date approx_now = new Date();
		final ManagementFuture<Boolean> ret_val = _bucket_crud.deleteObjectById("id1");
		
		assertEquals(1L, ret_val.getManagementResults().get().size());		
		ret_val.getManagementResults().get().stream().
			forEach(b -> {
				assertFalse("Failed", b.success());
			});
		
		//DEBUG
		//ret_val.getManagementResults().get().stream().map(b -> BeanTemplateUtils.toJson(b)).forEach(bj -> System.out.println("REPLY MESSAGE: " + bj.toString()));
		
		assertTrue("Delete succeeds", ret_val.get());
		
		// After deletion:
		
		assertEquals(0L, (long)_underlying_bucket_crud.countObjects().get());
		assertEquals(0L, (long)_underlying_bucket_status_crud.countObjects().get());				
		
		// (element added to retry queue)
		assertEquals(1L, (long)_bucket_action_retry_store.countObjects().get());
		final BucketActionRetryMessage retry = _bucket_action_retry_store.getObjectBySpec(CrudUtils.anyOf(BucketActionRetryMessage.class)).get().get();
		final BucketActionMessage to_retry = (BucketActionMessage)BeanTemplateUtils.from(retry.message(), Class.forName(retry.message_clazz())).get();
		assertEquals("id1", to_retry.bucket()._id());
				
		// (should still have added to delete queue)
		assertEquals(1L, _bucket_deletion_queue.countObjects().get().intValue());
		final BucketDeletionMessage deletion_msg = _bucket_deletion_queue.getObjectById("/bucket/path/here/1").get().get();
		assertEquals(deletion_msg._id(), deletion_msg.bucket().full_name());
		assertEquals(Long.valueOf(deletion_msg.delete_on().getTime()).doubleValue(), Long.valueOf(approx_now.getTime() + 60L*1000L), 5.0*1000.0); // ie within 5s of when it was added)		
	}

	@Test
	public void testSingleDeleteById_partialTimeout() throws Exception {
	
		String host2 = insertActor(TestActor_Accepter.class);
		
		cleanDatabases();
		
		assertEquals(0L, (long)_underlying_bucket_crud.countObjects().get());
		assertEquals(0L, (long)_underlying_bucket_status_crud.countObjects().get());				
		assertEquals(0L, (long)_bucket_action_retry_store.countObjects().get());
		
		insertBucket(1, true, Arrays.asList("host1", host2), false, null);
		
		assertEquals(1L, (long)_underlying_bucket_crud.countObjects().get());
		assertEquals(1L, (long)_underlying_bucket_status_crud.countObjects().get());				
		
		final ManagementFuture<Boolean> ret_val = _bucket_crud.deleteObjectById("id1");
		
		assertEquals(2L, ret_val.getManagementResults().get().size());		
		ret_val.getManagementResults().get().stream()
			.forEach(b -> {
				if ( b.source().equals(host2) ) {
					assertTrue("Succeeded", b.success());				
				}
				else if (b.source().equals("host1")) {
					assertFalse("Failed", b.success());
				}
				else {
					fail("Unrecognized host: " + b.source());
				}
			});
		
		//DEBUG
		//ret_val.getManagementResults().get().stream().map(b -> BeanTemplateUtils.toJson(b)).forEach(bj -> System.out.println("REPLY MESSAGE: " + bj.toString()));
		
		assertTrue("Delete succeeds", ret_val.get());
		
		// After deletion:
		
		assertEquals(0L, (long)_underlying_bucket_crud.countObjects().get());
		assertEquals(0L, (long)_underlying_bucket_status_crud.countObjects().get());				
		
		// (element added to retry queue)
		assertEquals(1L, (long)_bucket_action_retry_store.countObjects().get());
		final BucketActionRetryMessage retry = _bucket_action_retry_store.getObjectBySpec(CrudUtils.anyOf(BucketActionRetryMessage.class)).get().get();
		final BucketActionMessage to_retry = (BucketActionMessage)BeanTemplateUtils.from(retry.message(), Class.forName(retry.message_clazz())).get();
		assertEquals("id1", to_retry.bucket()._id());
	}
	
	@Test
	public void testSingleDeleteById() throws Exception {
	
		String host1 = insertActor(TestActor_Accepter.class);
		String host2 = insertActor(TestActor_Accepter.class);
		
		cleanDatabases();
		
		assertEquals(0L, (long)_underlying_bucket_crud.countObjects().get());
		assertEquals(0L, (long)_underlying_bucket_status_crud.countObjects().get());				
		assertEquals(0L, (long)_bucket_action_retry_store.countObjects().get());
		assertEquals(0L, (long)_bucket_deletion_queue.countObjects().get());
		
		insertBucket(1, true, Arrays.asList(host1, host2), false, null);
		
		assertEquals(1L, (long)_underlying_bucket_crud.countObjects().get());
		assertEquals(1L, (long)_underlying_bucket_status_crud.countObjects().get());				
		
		final Date approx_now = new Date();
		final ManagementFuture<Boolean> ret_val = _bucket_crud.deleteObjectById("id1");
		
		assertEquals(2L, ret_val.getManagementResults().get().size());		
		ret_val.getManagementResults().get().stream()
			.forEach(b -> {
				if ( b.source().equals(host2) ) {
					assertTrue("Succeeded", b.success());				
				}
				else if (b.source().equals(host1)) {
					assertTrue("Succeeded", b.success());				
				}
				else {
					fail("Unrecognized host: " + b.source());
				}
			});
		
		//DEBUG
		//ret_val.getManagementResults().get().stream().map(b -> BeanTemplateUtils.toJson(b)).forEach(bj -> System.out.println("REPLY MESSAGE: " + bj.toString()));
		
		assertTrue("Delete succeeds", ret_val.get());
		
		// After deletion:
		
		assertEquals(0L, (long)_underlying_bucket_crud.countObjects().get());
		assertEquals(0L, (long)_underlying_bucket_status_crud.countObjects().get());				
		
		assertEquals(0L, (long)_bucket_action_retry_store.countObjects().get());		
		
		assertEquals(1L, _bucket_deletion_queue.countObjects().get().intValue());
		final BucketDeletionMessage deletion_msg = _bucket_deletion_queue.getObjectById("/bucket/path/here/1").get().get();
		assertEquals(deletion_msg._id(), deletion_msg.bucket().full_name());
		assertEquals(Long.valueOf(deletion_msg.delete_on().getTime()).doubleValue(), Long.valueOf(approx_now.getTime() + 60L*1000L), 5.0*1000.0); // ie within 5s of when it was added)		
	}
	
	@Test
	public void testSingleDeleteById_partialIgnore() throws Exception {
	
		String host1 = insertActor(TestActor_Refuser.class);
		String host2 = insertActor(TestActor_Accepter.class);
		
		cleanDatabases();
		
		assertEquals(0L, (long)_underlying_bucket_crud.countObjects().get());
		assertEquals(0L, (long)_underlying_bucket_status_crud.countObjects().get());				
		assertEquals(0L, (long)_bucket_action_retry_store.countObjects().get());
		
		insertBucket(1, true, Arrays.asList(host1, host2), false, null);
		
		assertEquals(1L, (long)_underlying_bucket_crud.countObjects().get());
		assertEquals(1L, (long)_underlying_bucket_status_crud.countObjects().get());				
		
		final ManagementFuture<Boolean> ret_val = _bucket_crud.deleteObjectById("id1");
		
		assertEquals(1L, ret_val.getManagementResults().get().size());		
		ret_val.getManagementResults().get().stream()
			.forEach(b -> {
				if ( b.source().equals(host2) ) {
					assertTrue("Succeeded", b.success());				
				}
				else {
					fail("Unrecognized host: " + b.source());
				}
			});
		
		//DEBUG
		//ret_val.getManagementResults().get().stream().map(b -> BeanTemplateUtils.toJson(b)).forEach(bj -> System.out.println("REPLY MESSAGE: " + bj.toString()));
		
		assertTrue("Delete succeeds", ret_val.get());
		
		// After deletion:
		
		assertEquals(0L, (long)_underlying_bucket_crud.countObjects().get());
		assertEquals(0L, (long)_underlying_bucket_status_crud.countObjects().get());				
		
		assertEquals(0L, (long)_bucket_action_retry_store.countObjects().get());
	}

	@Test
	public void testSingleDeleteBySpec() throws Exception {
	
		String host1 = insertActor(TestActor_Accepter.class);
		String host2 = insertActor(TestActor_Accepter.class);
		
		cleanDatabases();
		
		assertEquals(0L, (long)_underlying_bucket_crud.countObjects().get());
		assertEquals(0L, (long)_underlying_bucket_status_crud.countObjects().get());				
		assertEquals(0L, (long)_bucket_action_retry_store.countObjects().get());
		
		insertBucket(1, true, Arrays.asList(host1, host2), false, null);
		
		assertEquals(1L, (long)_underlying_bucket_crud.countObjects().get());
		assertEquals(1L, (long)_underlying_bucket_status_crud.countObjects().get());				
		
		final ManagementFuture<Boolean> ret_val = _bucket_crud.deleteObjectBySpec(CrudUtils.anyOf(DataBucketBean.class).when(DataBucketBean::full_name, "/bucket/path/here/1"));
		
		assertEquals(2L, ret_val.getManagementResults().get().size());		
		ret_val.getManagementResults().get().stream()
			.forEach(b -> {
				if ( b.source().equals(host2) ) {
					assertTrue("Succeeded", b.success());				
				}
				else if (b.source().equals(host1)) {
					assertTrue("Succeeded", b.success());				
				}
				else {
					fail("Unrecognized host: " + b.source());
				}
			});
		
		//DEBUG
		//ret_val.getManagementResults().get().stream().map(b -> BeanTemplateUtils.toJson(b)).forEach(bj -> System.out.println("REPLY MESSAGE: " + bj.toString()));
		
		assertTrue("Delete succeeds", ret_val.get());
		
		// After deletion:
		
		assertEquals(0L, (long)_underlying_bucket_crud.countObjects().get());
		assertEquals(0L, (long)_underlying_bucket_status_crud.countObjects().get());				
		
		assertEquals(0L, (long)_bucket_action_retry_store.countObjects().get());
	}
	
	@Test
	public void testSingleDeleteById_wrongId() throws Exception {
	
		String host1 = insertActor(TestActor_Accepter.class);
		String host2 = insertActor(TestActor_Accepter.class);
		
		cleanDatabases();
		
		assertEquals(0L, (long)_underlying_bucket_crud.countObjects().get());
		assertEquals(0L, (long)_underlying_bucket_status_crud.countObjects().get());				
		assertEquals(0L, (long)_bucket_action_retry_store.countObjects().get());
		
		insertBucket(1, true, Arrays.asList(host1, host2), false, null);
		
		assertEquals(1L, (long)_underlying_bucket_crud.countObjects().get());
		assertEquals(1L, (long)_underlying_bucket_status_crud.countObjects().get());				
		
		final ManagementFuture<Boolean> ret_val = _bucket_crud.deleteObjectById("id2");		
		
		assertFalse("Didn't delete, no matching buckets", ret_val.get());
		
		// After deletion:
		
		assertEquals(1L, (long)_underlying_bucket_crud.countObjects().get());
		assertEquals(1L, (long)_underlying_bucket_status_crud.countObjects().get());				
		
		assertEquals(0L, (long)_bucket_action_retry_store.countObjects().get());
	}
	
	@Test
	public void testSingleDeleteBySpec_wrongQuery() throws Exception {
	
		String host1 = insertActor(TestActor_Accepter.class);
		String host2 = insertActor(TestActor_Accepter.class);
		
		cleanDatabases();
		
		assertEquals(0L, (long)_underlying_bucket_crud.countObjects().get());
		assertEquals(0L, (long)_underlying_bucket_status_crud.countObjects().get());				
		assertEquals(0L, (long)_bucket_action_retry_store.countObjects().get());
		
		insertBucket(1, true, Arrays.asList(host1, host2), false, null);
		
		assertEquals(1L, (long)_underlying_bucket_crud.countObjects().get());
		assertEquals(1L, (long)_underlying_bucket_status_crud.countObjects().get());				
		
		final ManagementFuture<Boolean> ret_val = _bucket_crud.deleteObjectBySpec(CrudUtils.anyOf(DataBucketBean.class).when(DataBucketBean::full_name, "/bucket/path/here/2"));
		
		assertFalse("Didn't delete, no matching buckets", ret_val.get());
		
		// After deletion:
		
		assertEquals(1L, (long)_underlying_bucket_crud.countObjects().get());
		assertEquals(1L, (long)_underlying_bucket_status_crud.countObjects().get());				
		
		assertEquals(0L, (long)_bucket_action_retry_store.countObjects().get());
	}
	
	/////////////////////////////////////////////////////////////	
	/////////////////////////////////////////////////////////////	
	/////////////////////////////////////////////////////////////	
	
	// Multi delete
	
	@Test
	public void testMultiDelete_noTimeouts() throws Exception {
		
		String host1 = insertActor(TestActor_Accepter.class);
		String host2 = insertActor(TestActor_Accepter.class);
		
		cleanDatabases();
		
		assertEquals(0L, (long)_underlying_bucket_crud.countObjects().get());
		assertEquals(0L, (long)_underlying_bucket_status_crud.countObjects().get());				
		assertEquals(0L, (long)_bucket_action_retry_store.countObjects().get());
		
		insertBucket(1, true, Arrays.asList(host1, host2), false, null);
		insertBucket(2, true, Arrays.asList(host1, host2), false, null);
		insertBucket(3, true, Arrays.asList(host1), false, null);
		insertBucket(4, true, Arrays.asList(host1, host2), false, null);
		
		assertEquals(4L, (long)_underlying_bucket_crud.countObjects().get());
		assertEquals(4L, (long)_underlying_bucket_status_crud.countObjects().get());				
		
		ManagementFuture<Long> ret_val = _bucket_crud.deleteObjectsBySpec(CrudUtils.anyOf(DataBucketBean.class)
									.rangeAbove(DataBucketBean::full_name, "/bucket/path/here/2", true));
		
		assertEquals(2L, (long)ret_val.get());
		
		assertEquals(3L, ret_val.getManagementResults().get().size());		
		ret_val.getManagementResults().get().stream()
			.forEach(b -> {
				if ( b.source().equals(host2) ) {
					assertTrue("Succeeded", b.success());				
				}
				else if (b.source().equals(host1)) {
					assertTrue("Succeeded", b.success());				
				}
				else {
					fail("Unrecognized host: " + b.source());
				}
			});
		
		//DEBUG
		//ret_val.getManagementResults().get().stream().map(b -> BeanTemplateUtils.toJson(b)).forEach(bj -> System.out.println("REPLY MESSAGE: " + bj.toString()));
		
		// After deletion:
		
		assertEquals(2L, (long)_underlying_bucket_crud.countObjects().get());
		assertEquals(2L, (long)_underlying_bucket_status_crud.countObjects().get());				
		
		assertEquals(0L, (long)_bucket_action_retry_store.countObjects().get());
	}
	
	@Test
	public void testMultiDelete_2Timeouts() throws Exception {
		
		String host1 = insertActor(TestActor_Accepter.class);
		String host2 = insertActor(TestActor_Accepter.class);
		
		cleanDatabases();
		
		assertEquals(0L, (long)_underlying_bucket_crud.countObjects().get());
		assertEquals(0L, (long)_underlying_bucket_status_crud.countObjects().get());				
		assertEquals(0L, (long)_bucket_action_retry_store.countObjects().get());
		
		insertBucket(1, true, Arrays.asList(host1, host2), false, null);
		insertBucket(2, true, Arrays.asList(host1, host2), false, null);
		insertBucket(3, true, Arrays.asList(host1, "host3"), false, null);
		insertBucket(4, true, Arrays.asList(host1, host2, "host3", "host4"), false, null);
		
		assertEquals(4L, (long)_underlying_bucket_crud.countObjects().get());
		assertEquals(4L, (long)_underlying_bucket_status_crud.countObjects().get());				
		
		ManagementFuture<Long> ret_val = _bucket_crud.deleteObjectsBySpec(CrudUtils.anyOf(DataBucketBean.class)
									.rangeAbove(DataBucketBean::full_name, "/bucket/path/here/2", true));
		
		assertEquals(2L, (long)ret_val.get());
		
		assertEquals(6L, ret_val.getManagementResults().get().size());		
		ret_val.getManagementResults().get().stream()
			.forEach(b -> {
				if ( b.source().equals(host2) ) {
					assertTrue("Succeeded", b.success());				
				}
				else if (b.source().equals(host1)) {
					assertTrue("Succeeded", b.success());				
				}
				else if ( b.source().equals("host3") ) {
					assertFalse("Failed", b.success());				
				}
				else if (b.source().equals("host4")) {
					assertFalse("Failed", b.success());				
				}
				else {
					fail("Unrecognized host: " + b.source());
				}
			});
		
		//DEBUG
		//ret_val.getManagementResults().get().stream().map(b -> BeanTemplateUtils.toJson(b)).forEach(bj -> System.out.println("REPLY MESSAGE: " + bj.toString()));
		
		// After deletion:
		
		assertEquals(2L, (long)_underlying_bucket_crud.countObjects().get());
		assertEquals(2L, (long)_underlying_bucket_status_crud.countObjects().get());				
		
		assertEquals(3L, (long)_bucket_action_retry_store.countObjects().get());
	}
	
	/////////////////////////////////////////////////////////////	
		
	//TODO test random things that work (eg count)
	
	//TODO test random things that don't work, just to get coverage
}
