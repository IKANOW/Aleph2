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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Scanner;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.cluster.Cluster;
import akka.cluster.pubsub.DistributedPubSubMediator;
import akka.event.japi.LookupEventBus;

import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.distributed_services.data_model.DistributedServicesPropertyBean;
import com.ikanow.aleph2.distributed_services.data_model.IRoundRobinEventBusWrapper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class TestRemoteRoundRobinMessageBus {
	public static final Logger _logger = LogManager.getLogger();
		
	public TestRemoteRoundRobinMessageBus() {}
	
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
	TestRemoteRoundRobinMessageBus(
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
		TestRemoteRoundRobinMessageBus _odd;
		public TestActor_Unwrapper(TestRemoteRoundRobinMessageBus odd) {
			_odd = odd;
		}
		@Override
		public void onReceive(Object arg0) throws Exception {
			_logger.info(this.self() + ": Unwrap from: " + this.sender() + ": " + arg0.getClass());
			if (arg0 instanceof TestBean) {
				_odd._received_bus1++;
				TestBean msg = (TestBean) arg0;
				_logger.info("TestBean: " + msg.test1);
				_odd._test_bus2.publish(new EmbeddedTestBeanWrapper(msg.embedded(), this.self()));
			}
			else if (arg0 instanceof EmbeddedTestBean) {
				EmbeddedTestBean msg = (EmbeddedTestBean) arg0;
				_logger.info("EmbeddedTestBean: " + msg.test2);
				_odd._received_post_bus2++;
			}
			else if (arg0 instanceof DistributedPubSubMediator.SubscribeAck) {
				_logger.info("Subscribed");
			}
			else {
				_odd._unexpected++;
			}
		}		
	}
	
	public static class TestActor_Echo extends UntypedActor { // (will sit on test bus1)
		TestRemoteRoundRobinMessageBus _odd;
		public TestActor_Echo(TestRemoteRoundRobinMessageBus odd) {
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
		MockCoreDistributedServices temp = new MockCoreDistributedServices();		
		_connect_string = temp._test_server.getConnectString();
				
		HashMap<String, Object> config_map = new HashMap<String, Object>();
		config_map.put(DistributedServicesPropertyBean.ZOOKEEPER_CONNECTION, _connect_string);
		
		Config config = ConfigFactory.parseMap(config_map);				
		DistributedServicesPropertyBean bean =
				BeanTemplateUtils.from(config.getConfig(DistributedServicesPropertyBean.PROPERTIES_ROOT), DistributedServicesPropertyBean.class);
		
		assertEquals(_connect_string, bean.zookeeper_connection());
		
		_core_distributed_services = new CoreDistributedServices(bean);
		
		// Create remote bus and subscribe:
		
		Cluster.get(_core_distributed_services.getAkkaSystem()).registerOnMemberUp(() -> {
		
			_test_bus1 = _core_distributed_services.getRoundRobinMessageBus(TestBeanWrapper.class, TestBean.class, "test_bean");
			_test_bus2 = _core_distributed_services.getRoundRobinMessageBus(EmbeddedTestBeanWrapper.class, EmbeddedTestBean.class, "embedded_test_bean");
			_test_bus3 = _core_distributed_services.getRoundRobinMessageBus(EmbeddedTestBeanWrapper.class, EmbeddedTestBean.class, "counting_bus");
			
			ActorRef handler = _core_distributed_services.getAkkaSystem().actorOf(Props.create(TestActor_Unwrapper.class, this));
			_test_bus1.subscribe(handler, "test_bean");
			_test_bus3.subscribe(handler, "counting_bus");
		});
	}
	
	///////////////////////////////////
	///////////////////////////////////
	
	// TEST	
		
	static final int MESSAGES_TO_SEND = 10;
	int _unexpected = 0;
	int _received_bus1 = 0;
	int _received_post_bus2 = 0;
	
	@Test
	public void testRemoteRoundRobin() throws IOException {
		
		// Launch a thread to send me messages
		
		final Process px = Runtime.getRuntime().exec(Arrays.<String>asList(
				System.getenv("JAVA_HOME") + File.separator + "bin" + File.separator + "java",
				"-classpath",
				System.getProperty("java.class.path"),
				"com.ikanow.aleph2.distributed_services.services.TestRemoteRoundRobinMessageBus",
				_connect_string
				).toArray(new String[0]));
		
		inheritIO(px.getInputStream(), System.out);
		inheritIO(px.getErrorStream(), System.err);
		
		// Wait for the process to send its messages
		
		int waiting = 0;
		final int MAX_WAIT = 20;
		while (px.isAlive() && (waiting++ < MAX_WAIT)) {
			if (_received_post_bus2 >= MESSAGES_TO_SEND) {
				break;
			}
			try { Thread.sleep(1000); } catch (Exception e) {}
		}
		if (px.isAlive()) {
			px.destroyForcibly();			
		}
		while (px.isAlive() && (waiting++ < MAX_WAIT)) {
			try { Thread.sleep(1000); } catch (Exception e) {}			
		}
		if (waiting >= MAX_WAIT) {
			fail("Waited for 20s for the child process to finish");
		}		
		assertTrue(px.exitValue() > 0);
		
		// Check that my actor received all its messages
		
		assertTrue("Doesn't seem to be exact", _received_bus1 >= MESSAGES_TO_SEND/3); // (since i only see half of these)
		assertEquals(MESSAGES_TO_SEND, _received_post_bus2); // (i see all of these, half generated from me, half from the remote process)
		assertEquals(0, _unexpected);
		
	}
	
	///////////////////////////////////
	///////////////////////////////////
	
	// REMOTE SENDER			
	
	// A "remote" service that will shoot messages over the broadcast bus
	
	public static void main(String args[]) throws Exception {
		if (1 != args.length) {
			System.exit(-3);			
		}
		HashMap<String, Object> config_map = new HashMap<String, Object>();
		config_map.put(DistributedServicesPropertyBean.ZOOKEEPER_CONNECTION, args[0]);
		
		Config config = ConfigFactory.parseMap(config_map);				
		DistributedServicesPropertyBean bean =
				BeanTemplateUtils.from(config.getConfig(DistributedServicesPropertyBean.PROPERTIES_ROOT), DistributedServicesPropertyBean.class);
		
		assertEquals(args[0], bean.zookeeper_connection());
		
		ICoreDistributedServices core_distributed_services = new CoreDistributedServices(bean);

		Cluster.get(core_distributed_services.getAkkaSystem()).registerOnMemberUp(() -> {
			
			LookupEventBus<TestBeanWrapper, ActorRef, String> _test_bus1;
			LookupEventBus<EmbeddedTestBeanWrapper, ActorRef, String> _test_bus2, _test_bus3;
			_test_bus1 = core_distributed_services.getRoundRobinMessageBus(TestBeanWrapper.class, TestBean.class, "test_bean");
			_test_bus2 = core_distributed_services.getRoundRobinMessageBus(EmbeddedTestBeanWrapper.class, EmbeddedTestBean.class, "embedded_test_bean");
			_test_bus3 = core_distributed_services.getRoundRobinMessageBus(EmbeddedTestBeanWrapper.class, EmbeddedTestBean.class, "counting_bus");
			
			ActorRef publisher = core_distributed_services.getAkkaSystem().actorOf(Props.create(TestActor_Publisher.class, core_distributed_services));

			TestRemoteRoundRobinMessageBus local_this = new TestRemoteRoundRobinMessageBus(_test_bus1, _test_bus2, _test_bus3);
			ActorRef handler1 = core_distributed_services.getAkkaSystem().actorOf(Props.create(TestActor_Unwrapper.class, local_this));
			_test_bus1.subscribe(handler1, "test_bean");
			
			ActorRef handler2 = core_distributed_services.getAkkaSystem().actorOf(Props.create(TestActor_Echo.class, local_this));
			_test_bus2.subscribe(handler2, "embedded_test_bean");

			try { Thread.sleep(5000); } catch (Exception e) {}
						
			for (int i = 0; i < MESSAGES_TO_SEND; ++i) {
				if (0 == (i%2)) {
					_test_bus1.publish(new TestBeanWrapper(createMessage(), handler2));
				}
				else {
					publisher.tell(createMessage(), null);
				}
			}
		});
		
		try { Thread.sleep(10000); } catch (Exception e) {}
		
		System.exit(0);			
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
	
	private static void inheritIO(final InputStream src, final PrintStream dest) {
	    new Thread(new Runnable() {
	        public void run() {
	            Scanner sc = new Scanner(src);
	            while (sc.hasNextLine()) {
	                dest.println(sc.nextLine());
	            }
	            sc.close();
	        }
	    }).start();
	}	
}
