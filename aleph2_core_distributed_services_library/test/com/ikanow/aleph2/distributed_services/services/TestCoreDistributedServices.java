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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import akka.serialization.Serialization;
import akka.serialization.SerializationExtension;
import akka.serialization.Serializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.distributed_services.data_model.DistributedServicesPropertyBean;
import com.ikanow.aleph2.distributed_services.utils.KafkaUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

@RunWith(value = Parameterized.class)
public class TestCoreDistributedServices {
	
	@Parameters(name = "{index}: auto config = {0}")
	public static Iterable<Boolean> data1() {
		return Arrays.asList(true, false);
	}
	
	final boolean auto_configure;
	public TestCoreDistributedServices(Boolean auto_configure) {
		this.auto_configure = auto_configure;
	}
	
	protected ICoreDistributedServices _core_distributed_services;
	@Before
	public void setupCoreDistributedServices() throws Exception {
		System.out.println("TEST WITH AUTO_CONFIG = " + auto_configure);
		
		MockCoreDistributedServices temp = new MockCoreDistributedServices();	
		String connect_string = temp._test_server.getConnectString();
		String broker_list_string = "localhost:" + temp.getKafkaBroker().getBrokerPort();
				
		HashMap<String, Object> config_map = new HashMap<String, Object>();
		config_map.put(DistributedServicesPropertyBean.ZOOKEEPER_CONNECTION, connect_string);
		if (!auto_configure)
			config_map.put(DistributedServicesPropertyBean.BROKER_LIST, broker_list_string);
		
		Config config = ConfigFactory.parseMap(config_map);				
		DistributedServicesPropertyBean bean =
				BeanTemplateUtils.from(config.getConfig(DistributedServicesPropertyBean.PROPERTIES_ROOT), DistributedServicesPropertyBean.class);
		
		assertEquals(connect_string, bean.zookeeper_connection());
		
		_core_distributed_services = new CoreDistributedServices(bean);
	}
	
	public static class TestBean implements Serializable {
		private static final long serialVersionUID = -1131596822679170769L;
		protected TestBean() {}
		public String test1() { return test1; }
		public EmbeddedTestBean embedded() { return embedded; };
		private String test1;
		private EmbeddedTestBean embedded;
	};
	
	public static class EmbeddedTestBean implements Serializable {
		private static final long serialVersionUID = 6856113449690828609L;
		protected EmbeddedTestBean() {}
		public String test2() { return test2; }
		private String test2;
	};
	
	@Test
	public void testCoreDistributedServices() throws KeeperException, InterruptedException, Exception {
		final CuratorFramework curator = _core_distributed_services.getCuratorFramework();
        String path = curator.getZookeeperClient().getZooKeeper().create("/test", new byte[]{1,2,3}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        assertEquals(path, "/test");
        
        // Test serialization is hooked up:
        Serialization tester = SerializationExtension.get(_core_distributed_services.getAkkaSystem());
        
        TestBean test = BeanTemplateUtils.build(TestBean.class).with("test1", "val1")
        				.with("embedded", 
        						BeanTemplateUtils.build(EmbeddedTestBean.class).with("test2", "val2").done().get()).done().get();
        
        byte[] test_bytes = tester.serialize(test).get();
        
        assertEquals("{\"test1\":\"val1\",\"embedded\":{\"test2\":\"val2\"}}", new String(test_bytes, "UTF-8"));

        Serializer serializer = tester.findSerializerFor(test);

        assertEquals(true, serializer.includeManifest());
        
        byte[] test_bytes2 = serializer.toBinary(test);        
        
        assertEquals("{\"test1\":\"val1\",\"embedded\":{\"test2\":\"val2\"}}", new String(test_bytes2, "UTF-8"));
        
        TestBean test2 = (TestBean) serializer.fromBinary(test_bytes2, TestBean.class);
        
        assertEquals(test.test1(), test2.test1());
        assertEquals(test.embedded().test2(), test2.embedded().test2());
        
        //(This doesn't really test what I wanted, which was that a manifest was passed along with the serialization info)
        
	}
	
	
	/**
	 * Tests kafka by creating a new topic, throwing 50 items on the queue
	 * and making sure we get those 50 items off the queue.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testKafka() throws Exception {
		if (!auto_configure) { // Only test this once per true/false cycle
			return; 
		}
		
		//create a random topic so its new (has to call create function)
		final String TOPIC_NAME = "TEST_CDS_" + System.currentTimeMillis();  
		final int num_to_test = 10;
		//throw items on the queue
		
		JsonNode jsonNode = new ObjectMapper().readTree("{\"keyA\":\"val1\",\"keyB\":\"val2\",\"keyC\":\"val3\"}");
		String original_message = jsonNode.toString();	
		for ( int i = 0; i < num_to_test; i++ ) 
			_core_distributed_services.produce(TOPIC_NAME, original_message);	
		
		//grab the consumer
		Iterator<String> consumer = _core_distributed_services.consume(TOPIC_NAME);
		String consumed_message = null;
		int message_count = 0;
		//read the item off the queue
		while ( consumer.hasNext() ) {
			consumed_message = consumer.next();
        	message_count++;
            System.out.println(consumed_message);
		}
		
		KafkaUtils.deleteTopic(TOPIC_NAME);
		
        assertEquals(message_count, num_to_test);
        assertTrue(original_message.equals(consumed_message));
	}
		
	@Test
	public void testKafkaForStormSpout() throws Exception {
		if (!auto_configure) { // Only test this once per true/false cycle
			return; 
		}
		
		//create a topic for a kafka spout, put some things in the spout		
		final String TOPIC_NAME = "TEST_KAFKA_SPOUT_" + System.currentTimeMillis();  
		final int num_to_test = 100; //set this high enough to hit the concurrent connection limit just incase we messed something up
		//throw items on the queue
		
		JsonNode jsonNode = new ObjectMapper().readTree("{\"keyA\":\"val1\",\"keyB\":\"val2\",\"keyC\":\"val3\"}");
		String original_message = jsonNode.toString();	
		for ( int i = 0; i < num_to_test; i++ ) 
			_core_distributed_services.produce(TOPIC_NAME, original_message);	
		
		//grab the consumer
		Iterator<String> consumer = _core_distributed_services.consume(TOPIC_NAME);
		String consumed_message = null;
		int message_count = 0;
		//read the item off the queue
		while ( consumer.hasNext() ) {
			consumed_message = consumer.next();
        	message_count++;
            System.out.println(consumed_message);
		}
		
		KafkaUtils.deleteTopic(TOPIC_NAME);
		
		assertEquals(num_to_test, message_count); 
	}
}
