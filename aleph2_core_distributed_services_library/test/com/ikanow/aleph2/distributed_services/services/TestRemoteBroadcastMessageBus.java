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

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.distributed_services.data_model.DistributedServicesPropertyBean;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class TestRemoteBroadcastMessageBus {

	protected ICoreDistributedServices _core_distributed_services;
	protected String _connect_string;
	
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
		
		//TODO
	}
	
	@Ignore
	@Test
	public void testRemoteBroadcast() throws IOException {
		
		// Launch a thread to send me messages
		
		final Process px = Runtime.getRuntime().exec(Arrays.<String>asList(
				System.getenv("JAVA_HOME") + File.separator + "bin" + File.separator + "java",
				"-classpath",
				System.getProperty("java.class.path"),
				"com.ikanow.aleph2.distributed_services.services.TestRemoteBroadcastMessageBus",
				_connect_string
				).toArray(new String[0]));
		
		inheritIO(px.getInputStream(), System.out);
		inheritIO(px.getErrorStream(), System.err);
		
		// Wait for the process to send its messages
		
		int waiting = 0;
		final int MAX_WAIT = 20;
		while (px.isAlive() && (waiting++ < MAX_WAIT)) {			
			try { Thread.sleep(1000); } catch (Exception e) {}
		}
		if (waiting >= MAX_WAIT) {
			px.destroyForcibly();
			fail("Waited for 20s for the child process to finish");
		}
		
		assertEquals(0, px.exitValue());
		
		// Check that my actor received all its messages
		
		//TODO
		
		//TODO: need to send some replies and check they arrived (have echo messanger?)
	}
	
	//////////////////////////
	
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
		
		@SuppressWarnings("unused")
		ICoreDistributedServices core_distributed_services = new CoreDistributedServices(bean);

		//core_distributed_services.getBroadcastMessageBus(wrapper_clazz, base_message_clazz, topic)
		
		/**/
		try { Thread.sleep(10000); } catch (Exception e) {}
		
		System.exit(0);			
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
