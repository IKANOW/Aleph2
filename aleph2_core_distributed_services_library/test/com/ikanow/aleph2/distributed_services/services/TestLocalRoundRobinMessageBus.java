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
package com.ikanow.aleph2.distributed_services.services;

import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.cluster.pubsub.DistributedPubSubMediator;
import akka.event.japi.LookupEventBus;

import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.distributed_services.data_model.IRoundRobinEventBusWrapper;

public class TestLocalRoundRobinMessageBus {
	public static final Logger _logger = LogManager.getLogger();
		
	public TestLocalRoundRobinMessageBus() {}
	
	///////////////////////////////////
	///////////////////////////////////
	
	// ALEPH-2 STATE
	
	protected ICoreDistributedServices _core_distributed_services;
	protected String _connect_string;
	
	///////////////////////////////////
	///////////////////////////////////
	
	// MESSAGES	
	
	public static class TestBeanWrapper implements IRoundRobinEventBusWrapper<TestBean> {
		TestBeanWrapper(TestBean message, ActorRef sender) {
			this.message = message; this.sender = sender;
		}
		TestBean message;
		ActorRef sender;
		
		@Override
		public ActorRef sender() {
			return sender;
		}

		@Override
		public TestBean message() {
			return message;
		}
		
	}
	public static class EmbeddedTestBeanWrapper implements IRoundRobinEventBusWrapper<EmbeddedTestBean> {
		EmbeddedTestBeanWrapper(EmbeddedTestBean message, ActorRef sender) {
			this.message = message; this.sender = sender;
		}
		EmbeddedTestBean message;
		ActorRef sender;
		
		@Override
		public ActorRef sender() {
			return sender;
		}

		@Override
		public EmbeddedTestBean message() {
			return message;
		}
		
	}
	public static class TestBean implements Serializable {
		private static final long serialVersionUID = -4821515068964028313L;
		protected TestBean() {}
		public String test1() { return test1; }
		public EmbeddedTestBean embedded() { return embedded; };
		private String test1;
		private EmbeddedTestBean embedded;
	};
	
	public static class EmbeddedTestBean implements Serializable {
		private static final long serialVersionUID = 6415472365874571848L;
		protected EmbeddedTestBean() {}
		public String test2() { return test2; }
		private String test2;
	};
	
	///////////////////////////////////
	///////////////////////////////////
	
	// ACTORS	
	TestLocalRoundRobinMessageBus(
		LookupEventBus<TestBeanWrapper, ActorRef, String> test_bus1, 
		LookupEventBus<EmbeddedTestBeanWrapper, ActorRef, String> test_bus2,
		LookupEventBus<EmbeddedTestBeanWrapper, ActorRef, String> test_bus3
		)
	{
		_test_bus1 = test_bus1;
		_test_bus2 = test_bus2;
		_test_bus3 = test_bus3;
	}
	LookupEventBus<TestBeanWrapper, ActorRef, String> _test_bus1;
	LookupEventBus<EmbeddedTestBeanWrapper, ActorRef, String> _test_bus2;
	LookupEventBus<EmbeddedTestBeanWrapper, ActorRef, String> _test_bus3;
	// Test actors:
	public static class TestActor_Unwrapper extends UntypedActor { // (will sit on test bus 2)
		TestLocalRoundRobinMessageBus _odd;
		boolean _remote;
		public TestActor_Unwrapper(TestLocalRoundRobinMessageBus odd, boolean remote) {
			_odd = odd;
			_remote = remote;
		}
		@Override
		public void onReceive(Object arg0) throws Exception {
			_logger.info(this.self() + ": Unwrap from: " + this.sender() + ": " + arg0.getClass());
			if (arg0 instanceof TestBean) {
				if (!_remote) _odd._received_bus1.incrementAndGet();
				TestBean msg = (TestBean) arg0;
				_logger.info("TestBean: " + msg.test1);
				_odd._test_bus2.publish(new EmbeddedTestBeanWrapper(msg.embedded(), this.self()));
			}
			else if (arg0 instanceof EmbeddedTestBean) {
				EmbeddedTestBean msg = (EmbeddedTestBean) arg0;
				_logger.info("EmbeddedTestBean: " + msg.test2);
				_odd._received_post_bus2.incrementAndGet();
			}
			else if (arg0 instanceof DistributedPubSubMediator.SubscribeAck) {
				_logger.info("Subscribed");
			}
			else {
				_odd._unexpected.incrementAndGet();
			}
		}		
	}
	
	public static class TestActor_Echo extends UntypedActor { // (will sit on test bus1)
		TestLocalRoundRobinMessageBus _odd;
		public TestActor_Echo(TestLocalRoundRobinMessageBus odd) {
			_odd = odd;
		}
		@Override
		public void onReceive(Object arg0) throws Exception {
			_logger.info("echo from: " + this.sender() + ": " + arg0.getClass()); 
			if (arg0 instanceof DistributedPubSubMediator.SubscribeAck) {
				_logger.info("Subscribed");
			}
			else if ((arg0 instanceof String) || (arg0 instanceof TestBean)) {
				// (just do nothing, this is for testing)
			}
			else if (arg0 instanceof EmbeddedTestBean) {
				EmbeddedTestBean msg = (EmbeddedTestBean) arg0;
				_odd._test_bus3.publish(new EmbeddedTestBeanWrapper(msg, this.self()));
			}					
			else {
				this.sender().tell(arg0, this.self());
			}
		}		
	}
	
	public static class TestActor_Publisher extends UntypedActor {
		final LookupEventBus<TestBeanWrapper, ActorRef, String> _test_bus1;
		
		public TestActor_Publisher(ICoreDistributedServices core_distributed_services) {
			_test_bus1 = core_distributed_services.getRoundRobinMessageBus(TestBeanWrapper.class, TestBean.class, "test_bean");
		}
		@Override
		public void onReceive(Object arg0) throws Exception {
			_logger.info("Publishing: " + arg0.getClass());
			if (arg0 instanceof TestBeanWrapper) {
				_test_bus1.publish((TestBeanWrapper) arg0);
			}
			else if (arg0 instanceof TestBean) {
				_test_bus1.publish(new TestBeanWrapper((TestBean) arg0, this.self()));
			}
		}
	}
	
	///////////////////////////////////
	///////////////////////////////////
	
	// SETUP	
		
	@Before
	public void setup() throws Exception {
		_core_distributed_services = new MockCoreDistributedServices();
		
		_test_bus1 = _core_distributed_services.getRoundRobinMessageBus(TestBeanWrapper.class, TestBean.class, "test_bean");
		_test_bus2 = _core_distributed_services.getRoundRobinMessageBus(EmbeddedTestBeanWrapper.class, EmbeddedTestBean.class, "embedded_test_bean");
		_test_bus3 = _core_distributed_services.getRoundRobinMessageBus(EmbeddedTestBeanWrapper.class, EmbeddedTestBean.class, "counting_bus");
		
		ActorRef handler = _core_distributed_services.getAkkaSystem().actorOf(Props.create(TestActor_Unwrapper.class, this, false));
		_test_bus1.subscribe(handler, "test_bean");
		_test_bus3.subscribe(handler, "counting_bus");
	}
	
	///////////////////////////////////
	///////////////////////////////////
	
	// TEST	
		
	static final int MESSAGES_TO_SEND = 10;
	AtomicInteger _unexpected = new AtomicInteger(0);
	AtomicInteger _received_bus1 = new AtomicInteger(0);
	AtomicInteger _received_post_bus2 = new AtomicInteger(0);
	
	@Test
	public void testLocalRoundRobin() throws Exception {
				
		// Launch a thread to send me messages
		
		final Future<?> f = Executors.newSingleThreadExecutor().submit(Lambdas.wrap_runnable_u(() -> {
			threadMain();
		}));
		
		// Wait for the process to send its messages
		
		int waiting = 0;
		final int MAX_WAIT = 20;
		while ((waiting++ < MAX_WAIT) && !f.isDone()) {
			if ((_received_post_bus2.get() >= MESSAGES_TO_SEND)) {
				break;
			}
			try { Thread.sleep(1000); } catch (Exception e) {}
		}
		f.get();
		
		// Check that my actor received all its messages
		
		assertEquals(MESSAGES_TO_SEND/2, _received_bus1.get());
		assertEquals(MESSAGES_TO_SEND, _received_post_bus2.get());
		assertEquals(0, _unexpected.get());
	}
	
	///////////////////////////////////
	///////////////////////////////////
	
	// REMOTE SENDER			
	
	// A "remote" service that will shoot messages over the broadcast bus
	
	public void threadMain() throws Exception {
		ActorRef publisher = _core_distributed_services.getAkkaSystem().actorOf(Props.create(TestActor_Publisher.class, _core_distributed_services));
		
		ActorRef handler1 = _core_distributed_services.getAkkaSystem().actorOf(Props.create(TestActor_Unwrapper.class, this, true));
		_test_bus1.subscribe(handler1, "test_bean");
		
		ActorRef handler2 = _core_distributed_services.getAkkaSystem().actorOf(Props.create(TestActor_Echo.class, this));
		_test_bus2.subscribe(handler2, "embedded_test_bean");
		
		try { Thread.sleep(2000); } catch (Exception e) {}
					
		for (int i = 0; i < MESSAGES_TO_SEND; ++i) {
			if (0 == (i%2)) {
				_test_bus1.publish(new TestBeanWrapper(createMessage(), handler2));
			}
			else {
				publisher.tell(createMessage(), null);
			}
		}
		try { Thread.sleep(5000L); } catch (Exception e) {}
	}

	////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////

	// UTIL

	public static int _count = 0; 
	private static TestBean createMessage() {
	
        TestBean test = BeanTemplateUtils.build(TestBean.class).with("test1", "val1_" + _count)
				.with("embedded", 
						BeanTemplateUtils.build(EmbeddedTestBean.class).with("test2", "val2_" + _count++).done().get()).done().get();
		return test;
	}
	
}
