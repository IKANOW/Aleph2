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
import akka.cluster.pubsub.DistributedPubSub;
import akka.cluster.pubsub.DistributedPubSubMediator;

import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.distributed_services.data_model.DistributedServicesPropertyBean;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class TestAkkaMessageBuses {
	public static final Logger _logger = LogManager.getLogger();
	
	///////////////////////////////////
	///////////////////////////////////
	
	// ALEPH-2 STATE
	
	protected ICoreDistributedServices _core_distributed_services;
	protected String _connect_string;
	
	///////////////////////////////////
	///////////////////////////////////
	
	// MESSAGES	
	
	///////////////////////////////////
	///////////////////////////////////
	
	// ACTORS	

	public static class Subscriber extends UntypedActor {
		Logger log = LogManager.getLogger();

		public Subscriber() {
			ActorRef mediator = 
					DistributedPubSub.get(getContext().system()).mediator();
			// subscribe to the topic named "content"
			mediator.tell(new DistributedPubSubMediator.Subscribe("content", getSelf()), 
					getSelf());
		}

		public void onReceive(Object msg) {
			if (msg instanceof String)
				log.info("Got: {}", msg);
			else if (msg instanceof DistributedPubSubMediator.SubscribeAck)
				log.info("subscribing");
			else
				unhandled(msg);
		}
	}	
	
	public static class Publisher extends UntypedActor {
		Logger log = LogManager.getLogger();

		// activate the extension
		ActorRef mediator = 
				DistributedPubSub.get(getContext().system()).mediator();

		public void onReceive(Object msg) {
			if (msg instanceof String) {
				String in = (String) msg;
				String out = in.toUpperCase();
				
				log.info("sending: " + out + " to " + mediator);
				
				mediator.tell(new DistributedPubSubMediator.Publish("content", out), 
						getSelf());
			} else {
				unhandled(msg);
			}
		}
	}	
	
	///////////////////////////////////
	///////////////////////////////////
	
	// SETUP	
		
	@Before
	public void setup() throws Exception {
		MockCoreDistributedServices temp = new MockCoreDistributedServices();		
		_connect_string = temp.getConnectString();
				
		HashMap<String, Object> config_map = new HashMap<String, Object>();
		config_map.put(DistributedServicesPropertyBean.ZOOKEEPER_CONNECTION, _connect_string);
		
		Config config = ConfigFactory.parseMap(config_map);				
		DistributedServicesPropertyBean bean =
				BeanTemplateUtils.clone(
						BeanTemplateUtils.from(config.getConfig(DistributedServicesPropertyBean.PROPERTIES_ROOT), DistributedServicesPropertyBean.class))
				.with("cluster_name", "testRemoteBroadcast")
				.done();
		
		assertEquals(_connect_string, bean.zookeeper_connection());
		
		_core_distributed_services = new CoreDistributedServices(bean);
		
		// Create remote bus and subscribe:
				
		Cluster.get(_core_distributed_services.getAkkaSystem()).registerOnMemberUp(() -> {
			_core_distributed_services.getAkkaSystem().actorOf(Props.create(Subscriber.class), "subscriber1");
		});
	}
	
	///////////////////////////////////
	///////////////////////////////////
	
	// TEST	
		
	@Test
	public void testRemoteBroadcast() throws IOException {
		_logger.info("Start testRemoteBroadcast");
		
		// Launch a thread to send me messages
		
		final Process px = Runtime.getRuntime().exec(Arrays.<String>asList(
				System.getenv("JAVA_HOME") + File.separator + "bin" + File.separator + "java",
				"-classpath",
				System.getProperty("java.class.path"),
				"com.ikanow.aleph2.distributed_services.services.TestAkkaMessageBuses",
				_connect_string
				).toArray(new String[0]));
		
		inheritIO(px.getInputStream(), System.out);
		inheritIO(px.getErrorStream(), System.err);
		
		// Wait for the process to send its messages
		
		_logger.info("Started process, waiting for completion");
		int waiting = 0;
		final int MAX_WAIT = 20;
		while (px.isAlive() && (waiting++ < MAX_WAIT)) {
			try { Thread.sleep(1000); } catch (Exception e) {}
		}
		if (px.isAlive()) {
			_logger.info("Process still alive, destry and fall through to fail");
			px.destroyForcibly();			
		}
		if (waiting >= MAX_WAIT) {
			fail("Waited for 20s for the child process to finish");
		}		
		while (px.isAlive() && (waiting++ < MAX_WAIT)) {
			try { Thread.sleep(1000); } catch (Exception e) {}			
		}
		assertTrue("Process shouldn't exit with error: " + px.exitValue(), px.exitValue() >= 0);		
	}
	
	///////////////////////////////////
	///////////////////////////////////
	
	// REMOTE SENDER			
	
	// A "remote" service that will shoot messages over the broadcast bus
	
	static final int MESSAGES_TO_SEND = 1;	
	
	public static void main(String args[]) throws Exception {
		if (1 != args.length) {
			_logger.error("Process command line mismatchL " + args);
			System.exit(-3);			
		}
		_logger.info("Started remote process");
		HashMap<String, Object> config_map = new HashMap<String, Object>();
		config_map.put(DistributedServicesPropertyBean.ZOOKEEPER_CONNECTION, args[0]);
		
		Config config = ConfigFactory.parseMap(config_map);				
		DistributedServicesPropertyBean bean =
				BeanTemplateUtils.from(config.getConfig(DistributedServicesPropertyBean.PROPERTIES_ROOT), DistributedServicesPropertyBean.class);
		
		assertEquals(args[0], bean.zookeeper_connection());
		
		ICoreDistributedServices core_distributed_services = new CoreDistributedServices(bean);

		Cluster.get(core_distributed_services.getAkkaSystem()).registerOnMemberUp(() -> {
			_logger.info("Remote process joined cluster");
			core_distributed_services.getAkkaSystem().actorOf(Props.create(Subscriber.class), "subscriber2");

			// Wait for the cluster to become synchronized
			try { Thread.sleep(5000L); } catch (Exception e) {};
			_logger.info("Cluster synchronized");
			
			ActorRef publisher = core_distributed_services.getAkkaSystem().actorOf(Props.create(Publisher.class), "publisher");
			// after a while the subscriptions are replicated
			for (int i = 0; i < MESSAGES_TO_SEND; ++i) {
				publisher.tell("hello", null);
			}
			_logger.info("Finished sending messages");
		});
		
		try { Thread.sleep(10000); } catch (Exception e) {}
		_logger.info("Remote process about to exit");
		
		System.exit(0);			
	}

	////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////

	// UTIL

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
