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
package com.ikanow.aleph2.data_import_manager.analytics.actors;

import static org.junit.Assert.*;

import java.io.File;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.distributed_services.data_model.DistributedServicesPropertyBean;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.distributed_services.services.MockCoreDistributedServices;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerMessage;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerMessage.AnalyticsTriggerEventBusWrapper;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerStateBean;
import com.ikanow.aleph2.management_db.services.ManagementDbActorContext;
import com.ikanow.aleph2.management_db.utils.ActorUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

public class TestAnalyticsTriggerSupervisorActor {

	private static final Logger _logger = LogManager.getLogger();	

	@Inject 
	protected IServiceContext _service_context = null;	
	
	protected ICoreDistributedServices _cds = null;
	protected IManagementDbService _core_mgmt_db = null;
	protected IManagementDbService _under_mgmt_db = null;
	protected ManagementDbActorContext _actor_context = null;
	
	protected static AtomicLong _num_received = new AtomicLong();
	
	// This one always accepts, but then refuses when it comes down to it...
	public static class TestActor extends UntypedActor {
		public TestActor() {
		}
		@Override
		public void onReceive(Object arg0) throws Exception {
			_logger.info("Received message from singleton! " + arg0.getClass().toString());
			
			if (arg0 instanceof AnalyticTriggerMessage) {
				_num_received.incrementAndGet();
			}
		}
	};
	
	@SuppressWarnings("deprecation")
	@Before
	public void test_Setup() throws Exception {
		
		if (null != _service_context) {
			return;
		}
		final String temp_dir = System.getProperty("java.io.tmpdir") + File.separator;
		
		// OK we're going to use guice, it was too painful doing this by hand...				
		Config config = ConfigFactory.parseReader(new InputStreamReader(this.getClass().getResourceAsStream("test_data_bucket_change.properties")))
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
		_under_mgmt_db = _service_context.getService(IManagementDbService.class, Optional.empty()).get();
	}

	Optional<ActorRef> _test_actor;
	
	@Test
	public void test_analyticsTriggerSupervisor() throws InterruptedException {
		_cds.waitForAkkaJoin(Optional.of(Duration.create(10, TimeUnit.SECONDS)));
		Thread.sleep(2000L);

		//(trigger bus)
		final LookupEventBus<AnalyticsTriggerEventBusWrapper, ActorRef, String> trigger_bus = _actor_context.getAnalyticsTriggerBus();
		final ActorRef test_deleter = _cds.getAkkaSystem().actorOf(Props.create(TestActor.class), "test_triggerer");
		trigger_bus.subscribe(test_deleter, ActorUtils.ANALYTICS_TRIGGER_BUS);

		// wake up the supervisor
		
		_test_actor = _cds.createSingletonActor(ActorUtils.ANALYTICS_TRIGGER_SINGLETON_ACTOR,
					ImmutableSet.<String>builder().add(DistributedServicesPropertyBean.ApplicationNames.DataImportManager.toString()).build(), 
					Props.create(AnalyticsTriggerSupervisorActor.class));
				
		assertTrue("Created singleton actor", _test_actor.isPresent());
		Thread.sleep(1000L); // (wait a second or so for it to start up)
		System.out.println("Sending messages");
		
		// send it a couple of pings
		
		_test_actor.get().tell("Ping!", test_deleter);
		_test_actor.get().tell("Ping!", test_deleter);
		
		for (int ii = 0; (ii < 20) && (_num_received.get() <= 2); ++ii) Thread.sleep(500L);
		assertTrue("Got some messages: " + _num_received.get(), _num_received.get() > 2); // (should have gotten mine + at least one from the scheduled)
		
		// Check the DB was optimized:
		assertTrue(_under_mgmt_db.getAnalyticBucketTriggerState(AnalyticTriggerStateBean.class).deregisterOptimizedQuery(Arrays.asList("is_active")));
		assertTrue(_under_mgmt_db.getAnalyticBucketTriggerState(AnalyticTriggerStateBean.class).deregisterOptimizedQuery(Arrays.asList("next_check")));
	}	
	
	@After
	public void cleanupTest() {
		System.out.println("Shutting down actor: " + _test_actor.toString());
		_test_actor.ifPresent(actor -> actor.tell(akka.actor.PoisonPill.getInstance(), actor));
		try { Thread.sleep(1000L); } catch (Exception e) {}
	}	
}
