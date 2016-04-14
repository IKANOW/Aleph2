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
package com.ikanow.aleph2.data_import_manager.analytics.actors;

import static org.junit.Assert.*;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

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
import com.ikanow.aleph2.data_import_manager.services.DataImportActorContext;
import com.ikanow.aleph2.data_import_manager.utils.ActorNameUtils;
import com.ikanow.aleph2.distributed_services.data_model.DistributedServicesPropertyBean;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerMessage;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerMessage.AnalyticsTriggerEventBusWrapper;
import com.ikanow.aleph2.management_db.utils.ActorUtils;

public class TestAnalyticsTriggerSupervisorActor extends TestAnalyticsTriggerWorkerCommon {

	private static final Logger _logger = LogManager.getLogger();	

	// This one always accepts, but then refuses when it comes down to it...
	public static class TestActor extends UntypedActor {
		public TestActor() {
			System.out.println("Starting TestActor");
		}
		@Override
		public void onReceive(Object arg0) throws Exception {
			_logger.info("Received message from singleton! " + arg0.getClass().toString());
			
			if (arg0 instanceof AnalyticTriggerMessage) {
				_num_received.incrementAndGet();
			}
		}
	};
	
	protected Optional<ActorRef> _test_actor;
	
	@Before
	@Override
	public void test_Setup() throws Exception {
		_logger.info("running child test_Setup");
		
		super.test_Setup();
			
		//(currently nothing else to do here)
	}	
	
	@Test
	public void test_analyticsTriggerSupervisor() throws InterruptedException {
		System.out.println("Starting test_analyticsTriggerSupervisor");
		
		_cds.waitForAkkaJoin(Optional.of(Duration.create(10, TimeUnit.SECONDS)));
		Thread.sleep(2000L);

		//(trigger bus)
		final LookupEventBus<AnalyticsTriggerEventBusWrapper, ActorRef, String> trigger_bus = _actor_context.getAnalyticsTriggerBus();
		final String hostname = DataImportActorContext.get().getInformationService().getHostname();
		final ActorRef test_deleter = _cds.getAkkaSystem().actorOf(
				Props.create(TestActor.class),
				hostname + ActorNameUtils.ANALYTICS_TRIGGER_WORKER_SUFFIX
				);
		trigger_bus.subscribe(test_deleter, ActorUtils.ANALYTICS_TRIGGER_BUS);

		// wake up the supervisor
		
		_test_actor = _cds.createSingletonActor("ANALYTICS_TRIGGER_SINGLETON_ACTOR",
					ImmutableSet.<String>builder().add(DistributedServicesPropertyBean.ApplicationNames.DataImportManager.toString()).build(), 
					Props.create(AnalyticsTriggerSupervisorActor.class));
				
		assertTrue("Created singleton actor", _test_actor.isPresent());
		Thread.sleep(1000L); // (wait a second or so for it to start up)
		System.out.println("Sending messages");
		
		// send it a couple of pings
		
		_test_actor.get().tell("Ping!", test_deleter);
		_test_actor.get().tell("Ping!", test_deleter);
		
		for (int ii = 0; (ii < 20) && (_num_received.get() <= 2); ++ii) Thread.sleep(1000L);
		assertTrue("Got some messages: " + _num_received.get(), _num_received.get() > 2); // (should have gotten mine + at least one from the scheduled)
	}	
	
	@After
	public void cleanupTest() {
		System.out.println("Shutting down actor: " + _test_actor.toString());
		_test_actor.ifPresent(actor -> actor.tell(akka.actor.PoisonPill.getInstance(), actor));
		try { Thread.sleep(1000L); } catch (Exception e) {}
	}	
}
