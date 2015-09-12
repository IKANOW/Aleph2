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
package com.ikanow.aleph2.management_db.controllers.actors;

import static org.junit.Assert.*;

import java.io.File;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import akka.actor.UntypedActor;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.data_model.utils.UuidUtils;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.distributed_services.services.MockCoreDistributedServices;
import com.ikanow.aleph2.management_db.data_model.BucketMgmtMessage.BucketTimeoutMessage;
import com.ikanow.aleph2.management_db.services.ManagementDbActorContext;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

public class TestBucketTestCycleSingletonActor {
	private static final Logger _logger = LogManager.getLogger();	

	@Inject 
	protected IServiceContext _service_context = null;	
	
	protected ICoreDistributedServices _cds = null;
	protected IManagementDbService _core_mgmt_db = null;
	protected ManagementDbActorContext _actor_context = null;
	
	// This one always accepts, but then refuses when it comes down to it...
	public static class TestActor extends UntypedActor {
		public TestActor() {
		}
		@Override
		public void onReceive(Object arg0) throws Exception {
			_logger.info("Received message from singleton! " + arg0.getClass().toString());
			
			if (arg0 instanceof BucketTimeoutMessage) {
				final BucketTimeoutMessage msg = (BucketTimeoutMessage) arg0;
				if (msg.bucket().full_name().endsWith("3") || msg.bucket().full_name().endsWith("5") || msg.bucket().full_name().endsWith("7")) {
					// (do nothing)
				}
				else {
					this.sender().tell(msg, this.self());
				}
			}
		}
	};
	
	@SuppressWarnings("deprecation")
	@Before
	public void testSetup() throws Exception {
		
		if (null != _service_context) {
			return;
		}
		final String temp_dir = System.getProperty("java.io.tmpdir") + File.separator;
		
		// OK we're going to use guice, it was too painful doing this by hand...				
		Config config = ConfigFactory.parseReader(new InputStreamReader(this.getClass().getResourceAsStream("actor_test.properties")))
							.withValue("globals.local_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
							.withValue("globals.local_cached_jar_dir", ConfigValueFactory.fromAnyRef(temp_dir))
							.withValue("globals.distributed_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
							.withValue("globals.local_yarn_config_dir", ConfigValueFactory.fromAnyRef(temp_dir));
		
		Injector app_injector = ModuleUtils.createTestInjector(Arrays.asList(), Optional.of(config));	
		app_injector.injectMembers(this);
		
		_cds = _service_context.getService(ICoreDistributedServices.class, Optional.empty()).get();
		MockCoreDistributedServices mcds = (MockCoreDistributedServices) _cds;
		mcds.setApplicationName("DataImportManager");
		
		new ManagementDbActorContext(_service_context, true);		
		_actor_context = ManagementDbActorContext.get();
		
		_core_mgmt_db = _service_context.getCoreManagementDbService();		
	}
	
	/**
	 * Puts items on the test queue and checks that they have been
	 * picked up by the actor appropriately.
	 * 
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	@Test
	public void test_bucketTestCycleSingletonActor() throws InterruptedException, ExecutionException {					
		//(test queue)
		final ICrudService<BucketTimeoutMessage> test_queue = _core_mgmt_db.getBucketTestQueue(BucketTimeoutMessage.class);
		//clear test queue
		test_queue.deleteDatastore().get();
		
		BucketTestCycleSingletonActor.setTestMode(true);
		
		//add message to test queue that will expire now
		System.out.println("TEST 1:");
		test_queue.storeObject(createTestBucketTimeoutMessage(new Date()));				
		assertEquals(1, test_queue.countObjects().get().longValue());				
		Thread.sleep(3000);
		assertEquals(0, test_queue.countObjects().get().longValue());
		
		//test multiple things are gathered at once
		System.out.println("TEST 2:");
		test_queue.storeObject(createTestBucketTimeoutMessage(new Date()));
		test_queue.storeObject(createTestBucketTimeoutMessage(new Date()));
		assertEquals(2, test_queue.countObjects().get().longValue());		
		Thread.sleep(3000);
		assertEquals(0, test_queue.countObjects().get().longValue());
		
		//test an item that expires in the future stays around
		System.out.println("TEST 3:");
		test_queue.storeObject(createTestBucketTimeoutMessage(new Date()));
		test_queue.storeObject(createTestBucketTimeoutMessage(new Date(System.currentTimeMillis()+10000)));
		assertEquals(2, test_queue.countObjects().get().longValue());		
		Thread.sleep(3000);
		assertEquals(1, test_queue.countObjects().get().longValue());
		
		//TODO test error code somehow?
	}
	
	private BucketTimeoutMessage createTestBucketTimeoutMessage(final Date date_to_expire_on) {
		final DataBucketBean bucket = createBucket();
		final BucketTimeoutMessage test_msg = new BucketTimeoutMessage(bucket, date_to_expire_on, null);
		return test_msg;
	}

	@After
	public void cleanupTest() {
		_actor_context.onTestComplete();
	}
	
	protected DataBucketBean createBucket() {
		return BeanTemplateUtils.build(DataBucketBean.class)
							.with(DataBucketBean::_id, UuidUtils.get().getRandomUuid())
							.with(DataBucketBean::full_name, "/test/path/" + UuidUtils.get().getRandomUuid())
							.with(DataBucketBean::owner_id, "test_owner_id")
							.done().get();
	}
}
