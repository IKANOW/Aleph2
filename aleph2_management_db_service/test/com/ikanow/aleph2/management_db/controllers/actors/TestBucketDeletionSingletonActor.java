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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import scala.concurrent.duration.Duration;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.japi.LookupEventBus;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.FutureUtils;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.distributed_services.services.MockCoreDistributedServices;
import com.ikanow.aleph2.management_db.data_model.BucketMgmtMessage.BucketDeletionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketMgmtMessage.BucketMgmtEventBusWrapper;
import com.ikanow.aleph2.management_db.services.ManagementDbActorContext;
import com.ikanow.aleph2.management_db.utils.ActorUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

public class TestBucketDeletionSingletonActor {
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
			
			if (arg0 instanceof BucketDeletionMessage) {
				final BucketDeletionMessage msg = (BucketDeletionMessage) arg0;
				if (msg.bucket().full_name().endsWith("3") || msg.bucket().full_name().endsWith("5") || msg.bucket().full_name().endsWith("7")) {
					// (do nothing)
				}
				else {
					this.sender().tell(msg, this.self());
				}
			}
		}
	};
	
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
		
		Injector app_injector = ModuleUtils.createInjector(Arrays.asList(), Optional.of(config));	
		app_injector.injectMembers(this);
		
		_cds = _service_context.getService(ICoreDistributedServices.class, Optional.empty()).get();
		MockCoreDistributedServices mcds = (MockCoreDistributedServices) _cds;
		mcds.setApplicationName("DataImportManager");
		
		new ManagementDbActorContext(_service_context);		
		_actor_context = ManagementDbActorContext.get();
		
		_core_mgmt_db = _service_context.getCoreManagementDbService();		
	}

	@Test
	public void test_bucketDeletionSingletonActor() throws InterruptedException, ExecutionException {
		_cds.waitForAkkaJoin(Optional.of(Duration.create(10, TimeUnit.SECONDS)));
		Thread.sleep(2000L);
		
		//(delete queue)
		final ICrudService<BucketDeletionMessage> delete_queue = _core_mgmt_db.getBucketDeletionQueue(BucketDeletionMessage.class);

		//(delete bus)
		final LookupEventBus<BucketMgmtEventBusWrapper, ActorRef, String> delete_bus = _actor_context.getDeletionMgmtBus();
		final ActorRef test_deleter = _cds.getAkkaSystem().actorOf(Props.create(TestActor.class), "test_deleter");
		delete_bus.subscribe(test_deleter, ActorUtils.BUCKET_DELETION_BUS);

		// Ugh because our akka implementation doesn't support injection we have to remove the existing system generated deletor (vs change its type to the TestActor)
		scala.concurrent.Future<ActorRef> system_deleter_f = _cds.getAkkaSystem().actorSelection("akka://default/user/" + ActorUtils.BUCKET_DELETION_WORKER_ACTOR).resolveOne(Duration.create(10, TimeUnit.SECONDS));
		CompletableFuture<ActorRef> system_deleter_f_cf = FutureUtils.efficientWrap(system_deleter_f, _cds.getAkkaSystem().dispatcher());
		try {
			final ActorRef system_deleter = system_deleter_f_cf.get();
			delete_bus.unsubscribe(system_deleter);
		}
		catch (Exception e) {
			_logger.warn("For some reason /user/deletion_worker not found hence doesn't need to unsubscribe");
		}
		
		
		//(check if the fields are optimized - can only do that by 
		//DBCollection test_db = (DBCollection) delete_queue.getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).get();
		//System.out.println(test_db.getIndexInfo().stream().map(x -> x.toString()).collect(Collectors.joining("\n")));
		
		assertEquals(true, delete_queue.deregisterOptimizedQuery(Arrays.asList("delete_on")));
		assertEquals(true, delete_queue.deregisterOptimizedQuery(Arrays.asList("bucket.full_name")));
		delete_queue.deleteDatastore().get();
		assertEquals(0, delete_queue.countObjects().get().intValue());
		
		delete_queue.optimizeQuery(Arrays.asList("delete_on")).get();
		delete_queue.optimizeQuery(Arrays.asList("bucket.full_name")).get();		
		
		// Create 10 delete bucket messages to process now, 4 to process later
		final List<BucketDeletionMessage> to_insert = IntStream.range(0, 14).boxed()
			.map(i -> Tuples._2T(
					BeanTemplateUtils.build(DataBucketBean.class).with(DataBucketBean::full_name, "/test/delete/" + i).done().get()
					,
					(i < 10) ? new Date() : new Date(new Date().getTime() + 10*3600000L)))
					.map(bucket_date -> new BucketDeletionMessage(bucket_date._1(), bucket_date._2(), false))
					.collect(Collectors.toList())
					;
		
		delete_queue.storeObjects(to_insert).get();
		
		assertEquals(14, delete_queue.countObjects().get().intValue());
		
		for (int i = 0; i < 20; ++i) {
			Thread.sleep(1000L);
			if (delete_queue.countObjects().get().intValue() <= 7) {
				break;
			}
		}
		assertEquals(7, delete_queue.countObjects().get().intValue());
		// shunted "failed" deletions forward one hour
		assertEquals(3, delete_queue.countObjectsBySpec(
				CrudUtils.allOf(BucketDeletionMessage.class)
					.rangeIn(BucketDeletionMessage::delete_on, new Date(new Date().getTime() + 3000L*1000L), true, new Date(new Date().getTime() + 4000L*1000L), true)
				)
				.get().intValue());
		// also they have deletion_attempts == 1
		assertEquals(3, delete_queue.countObjectsBySpec(
				CrudUtils.allOf(BucketDeletionMessage.class)
					.rangeIn(BucketDeletionMessage::delete_on, new Date(new Date().getTime() + 3000L*1000L), true, new Date(new Date().getTime() + 4000L*1000L), true)
					.when(BucketDeletionMessage::deletion_attempts, 1)
				)
				.get().intValue());
		
		// (others are ~10 hours aheade still)
		assertEquals(4, delete_queue.countObjectsBySpec(
				CrudUtils.allOf(BucketDeletionMessage.class)
					.rangeAbove(BucketDeletionMessage::delete_on, new Date(new Date().getTime() + 30000L*1000L), true)
				) 
				.get().intValue());
		// also they have deletion_attempts == 0
		assertEquals(4, delete_queue.countObjectsBySpec(
				CrudUtils.allOf(BucketDeletionMessage.class)
					.rangeAbove(BucketDeletionMessage::delete_on, new Date(new Date().getTime() + 30000L*1000L), true)
					.when(BucketDeletionMessage::deletion_attempts, 0)
				) 
				.get().intValue());
		
		// Tidy up for this test:
		delete_bus.unsubscribe(test_deleter);
	}
	
	@After
	public void cleanupTest() {
		_actor_context.onTestComplete();
	}
}
