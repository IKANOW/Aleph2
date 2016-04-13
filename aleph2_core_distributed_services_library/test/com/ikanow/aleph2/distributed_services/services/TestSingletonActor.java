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
package com.ikanow.aleph2.distributed_services.services;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Optional;
import java.util.Scanner;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.SetOnce;
import com.ikanow.aleph2.data_model.utils.UuidUtils;
import com.ikanow.aleph2.distributed_services.data_model.DistributedServicesPropertyBean;
import com.ikanow.aleph2.distributed_services.data_model.IBroadcastEventBusWrapper;
import com.ikanow.aleph2.distributed_services.utils.ZookeeperUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.cluster.Cluster;
import akka.event.japi.LookupEventBus;

public class TestSingletonActor {
	public static final Logger _logger = LogManager.getLogger();
	public static String UUID = UuidUtils.get().getRandomUuid(); // (ie one per process)

	// clean_shutdown==false is slower so normally leave this as true
	//static boolean clean_shutdown = false;
	static boolean clean_shutdown = true;
	
	///////////////////////////////////
	///////////////////////////////////
	
	// ALEPH-2 STATE
	
	protected ICoreDistributedServices _core_distributed_services;
	protected String _connect_string;
	LookupEventBus<TestBeanWrapper, ActorRef, String> _test_bus1;
	
	///////////////////////////////////
	///////////////////////////////////
	
	// MESSAGES	

	public static class TestBeanWrapper implements IBroadcastEventBusWrapper<TestBean> {
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
	
	public static class TestBean implements Serializable {
		private static final long serialVersionUID = -4930243416467655902L;
		protected TestBean() {}
		public String test1() { return test1; }
		private String test1;
	};
	
	///////////////////////////////////
	///////////////////////////////////
	
	// ACTORS	

	// (note: has different local/remote meanings)
	protected static boolean _remote_singleton_is_alive = false;
	protected static boolean _local_singleton_is_alive = false;
	
	public static class TestActor extends UntypedActor {

		LookupEventBus<TestBeanWrapper, ActorRef, String> _test_bus1;
		
		public TestActor(ICoreDistributedServices core_distributed_services) {
			_local_singleton_is_alive = true;
			_logger.info("Launched TestActor: " + this + " /" + this.self() + " (uuid=" + UUID + ")");
			_test_bus1 = core_distributed_services.getBroadcastMessageBus(TestBeanWrapper.class, TestBean.class, "test_bean");
			_test_bus1.subscribe(this.self(), "test_bean");
		}		
		
		@Override
		public void onReceive(Object arg0) throws Exception {
			if (arg0 instanceof TestBean) {
				_logger.info("Received test bean: " + this.self() + " (uuid=" + UUID + ")");
				_remote_singleton_is_alive = true;
				this.sender().tell(createMessage(), this.self());
			}
			else _logger.info("Received: " + arg0.getClass() + " (uuid=" + UUID + ")");
		}
	}

	public static class StatusActor extends UntypedActor {

		@Override
		public void onReceive(Object arg0) throws Exception {
			if (arg0 instanceof TestBean) {
				_logger.info("Remote TestActor is alive" + " (uuid=" + UUID + ")");
				_remote_singleton_is_alive = true;
			}
		} // (will sit on test bus 2)
	}
	
	
	///////////////////////////////////
	///////////////////////////////////
	
	// SETUP	
		
	@Before
	public void setup() throws Exception {
		MockCoreDistributedServices temp = new MockCoreDistributedServices();		
		_connect_string = temp.getConnectString();
				
	}
	
	///////////////////////////////////
	///////////////////////////////////
	
	// TEST	
	
	@Test
	public void testSingletonActor() throws Exception {
		
		// Launch a remote process #1, will create with wrong role
		
		@SuppressWarnings("unused")
		final Process px_wrong_role = Runtime.getRuntime().exec(Arrays.<String>asList(
				System.getenv("JAVA_HOME") + File.separator + "bin" + File.separator + "java",
				"-classpath",
				System.getProperty("java.class.path"),
				"com.ikanow.aleph2.distributed_services.services.TestSingletonActor",
				_connect_string,
				"CREATE_ME_WITH_WRONG_ROLE"
				).toArray(new String[0]));
		
		inheritIO(px_wrong_role.getInputStream(), System.out);
		inheritIO(px_wrong_role.getErrorStream(), System.err);
		
		for (int i = 0;i < 30; ++i) { // (wait for remote process to start up and set up the cluster, so it's definitely the oldest...) 
			try { Thread.sleep(1000); } catch (Exception e) {}	
		}		
		_logger.info("Launching second remote process");
		
		// Launch a remote process #2
		
		final Process px = Runtime.getRuntime().exec(Arrays.<String>asList(
				System.getenv("JAVA_HOME") + File.separator + "bin" + File.separator + "java",
				"-classpath",
				System.getProperty("java.class.path"),
				"com.ikanow.aleph2.distributed_services.services.TestSingletonActor",
				_connect_string
				).toArray(new String[0]));
		
		inheritIO(px.getInputStream(), System.out);
		inheritIO(px.getErrorStream(), System.err);

		for (int i = 0;i < 30; ++i) { // (wait for remote process to start up and set up the cluster, so it's definitely the oldest...) 
			try { Thread.sleep(1000); } catch (Exception e) {}	
		}		
		_logger.info("Starting main process: " + UUID);
		
		// Now launch Akka:
		
		HashMap<String, Object> config_map = new HashMap<String, Object>();
		config_map.put(DistributedServicesPropertyBean.ZOOKEEPER_CONNECTION, _connect_string);
		config_map.put(DistributedServicesPropertyBean.APPLICATION_NAME, "test_role");
		
		Config config = ConfigFactory.parseMap(config_map);				
		DistributedServicesPropertyBean bean =
				BeanTemplateUtils.from(config.getConfig(DistributedServicesPropertyBean.PROPERTIES_ROOT), DistributedServicesPropertyBean.class);
		
		assertEquals(_connect_string, bean.zookeeper_connection());
		
		_core_distributed_services = new CoreDistributedServices(bean);
		
		assertEquals("test_role", _core_distributed_services.getApplicationName().get());
		
		final SetOnce<Boolean> started = new SetOnce<>();
		Cluster.get(_core_distributed_services.getAkkaSystem()).registerOnMemberUp(() -> {
			started.set(true);
		});
		for (int j = 0;j < 10; ++j) {
			try { Thread.sleep(1000); } catch (Exception e) {}
			if (started.isSet()) {
				_logger.info("Main thread was up after secs=" + j + " (uuid=" + UUID + ")");
				break;
			}
		}
		if (!started.isSet()) {
			fail("Cluster never woke up");
		}
		
		assertEquals("Shouldn't create actor with wrong role", Optional.empty(), _core_distributed_services.createSingletonActor("should_fail", 
				ImmutableSet.<String>builder().add("missing_role").build(), Props.create(StatusActor.class)));		
		
		_test_bus1 = _core_distributed_services.getBroadcastMessageBus(TestBeanWrapper.class, TestBean.class, "test_bean");			
		
		ActorRef status_handler = _core_distributed_services.getAkkaSystem().actorOf(Props.create(StatusActor.class));

		_logger.info("Send start up message to remote singleton handler");
		_test_bus1.publish(new TestBeanWrapper(createMessage(), status_handler));				
		
		
		for (int j = 0;j < 10; ++j) {
			try { Thread.sleep(1000); } catch (Exception e) {}	
			if (_remote_singleton_is_alive) {
				_logger.info("Remote singleton awoke");
				break;
			}
		}
		if (!_remote_singleton_is_alive) {
			fail("Remote singleton never woke up");
		}
		// OK now I can start my singleton and check it remains down (because it's a singleton!) 
		
		@SuppressWarnings("unused")
		ActorRef handler = _core_distributed_services.createSingletonActor("test_actor", 
				ImmutableSet.<String>builder().add("test_role").build(), Props.create(TestActor.class, _core_distributed_services)).get();
		
		_logger.info("Starting local singleton, shouldn't start up");
		for (int k = 0;k < 20; ++k) {
			try { Thread.sleep(1000); } catch (Exception e) {}		
			if (_local_singleton_is_alive) {
				fail("Local singleton started up");
			}
		}
		
		// Now kill the remote process and check that it starts up:

		if (!clean_shutdown) px.destroyForcibly();
		
		for (int k = 0;k < 180; ++k) {
			try { Thread.sleep(1000); } catch (Exception e) {}		
			if (_local_singleton_is_alive) {
				_logger.info("Local singleton started up");
				break;
			}
		}
		if (!_local_singleton_is_alive) {
			fail("Local singleton never woke up");
		}		
	}	
	
	///////////////////////////////////
	///////////////////////////////////
	
	// REMOTE SENDER			
	
	// A "remote" service that will shoot messages over the broadcast bus
	
	public static void main(String args[]) throws Exception {
		final boolean wrong_role;
		if (2 == args.length) { // I'm support to have the wrong role
			wrong_role = true;
			ZookeeperUtils.overrideHostname("wrong_role");
		}
		else if (1 != args.length) {
			wrong_role = true;
			System.exit(-3);	
		}
		else {
			ZookeeperUtils.overrideHostname("right_role");
			wrong_role = false;
		}
		_logger.info("STARTING REMOTE ACTOR WITH ROLE: wrong=" + wrong_role + " (uuid=" + UUID + ")"); // (i think we'll only see wrong=false, the other one's io isn't inherited)
		
		HashMap<String, Object> config_map = new HashMap<String, Object>();
		config_map.put(DistributedServicesPropertyBean.ZOOKEEPER_CONNECTION, args[0]);
		if (wrong_role) {
			config_map.put(DistributedServicesPropertyBean.APPLICATION_NAME, "random_application");
		}
		else {
			config_map.put(DistributedServicesPropertyBean.APPLICATION_NAME, "test_role");			
		}
		
		Config config = ConfigFactory.parseMap(config_map);				
		DistributedServicesPropertyBean bean =
				BeanTemplateUtils.from(config.getConfig(DistributedServicesPropertyBean.PROPERTIES_ROOT), DistributedServicesPropertyBean.class);
		
		assertEquals(args[0], bean.zookeeper_connection());
		
		ICoreDistributedServices core_distributed_services = new CoreDistributedServices(bean);

		Cluster.get(core_distributed_services.getAkkaSystem()).registerOnMemberUp(() -> {
			if (!wrong_role) {
				ActorRef handler = core_distributed_services.createSingletonActor("test_actor", 
						ImmutableSet.<String>builder().add("test_role").build(), Props.create(TestActor.class, core_distributed_services)).get();						
				_logger.info("Singleton manager = " + handler.toString() + ": " + handler.path().name());
			}
			else _logger.info("(Wrong role remote actor has also joined, as expected)"); // (not sure if we'll see this because we're only inheriting the io of the correct one)
		});
		
		for (int k = 0;k < 180; ++k) {
			if (clean_shutdown) {
				if (_remote_singleton_is_alive) {
					_logger.info("Shutting myself down cleanly in 30s");
					try { Thread.sleep(30000); } catch (Exception e) {}
					_logger.info("Shutting myself down cleanly now!");
					Cluster.get(core_distributed_services.getAkkaSystem()).leave(Cluster.get(core_distributed_services.getAkkaSystem()).selfAddress());
					try { Thread.sleep(5000); } catch (Exception e) {}
					break;
				}
			}
			try { Thread.sleep(1000); } catch (Exception e) {}
		}		
		
		
		System.exit(0);			
	}

	////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////

	// UTIL

	private static TestBean createMessage() {
        TestBean test = BeanTemplateUtils.build(TestBean.class).with("test1", "val1").done().get();
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
