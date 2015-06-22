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

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.message.MessageAndMetadata;
import kafka.producer.KeyedMessage;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.junit.Before;
import org.junit.Test;

import akka.serialization.Serialization;
import akka.serialization.SerializationExtension;
import akka.serialization.Serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.distributed_services.data_model.DistributedServicesPropertyBean;
import com.ikanow.aleph2.distributed_services.data_model.IJsonSerializable;
import com.ikanow.aleph2.distributed_services.utils.WrappedConsumerIterator;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class TestCoreDistributedServices {

	private static final String TOPIC_NAME = "TEST_CDS";
	private static final int NUM_THREADS = 1;
	protected ICoreDistributedServices _core_distributed_services;
	
	@Before
	public void setupCoreDistributedServices() throws Exception {
		MockCoreDistributedServices temp = new MockCoreDistributedServices();		
		String connect_string = temp._test_server.getConnectString();
				
		HashMap<String, Object> config_map = new HashMap<String, Object>();
		config_map.put(DistributedServicesPropertyBean.ZOOKEEPER_CONNECTION, connect_string);
		
		Config config = ConfigFactory.parseMap(config_map);				
		DistributedServicesPropertyBean bean =
				BeanTemplateUtils.from(config.getConfig(DistributedServicesPropertyBean.PROPERTIES_ROOT), DistributedServicesPropertyBean.class);
		
		assertEquals(connect_string, bean.zookeeper_connection());
		
		_core_distributed_services = new CoreDistributedServices(bean);
	}
	
	public static class TestBean implements IJsonSerializable {
		protected TestBean() {}
		public String test1() { return test1; }
		public EmbeddedTestBean embedded() { return embedded; };
		private String test1;
		private EmbeddedTestBean embedded;
	};
	
	public static class EmbeddedTestBean {
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
	
	@Test
	public void testKafka() throws Exception {
//		ICoreDistributedServices cds = new MockCoreDistributedServices();
//		//throw an item on the queue
//		JsonNode jsonNode = new ObjectMapper().readTree("{\"key\":\"val\"}");
//		String original_message = jsonNode.toString();
//		cds.produce(TOPIC_NAME, original_message);
//		
//		//grab the consumer
//		Iterator<String> consumer = cds.consume(TOPIC_NAME);
//		String consumed_message = null;
//		int message_count = 0;
//		//read the item off the queue
//		while ( consumer.hasNext() ) {
//			consumed_message = consumer.next();
//        	message_count++;
//            System.out.println(consumed_message);
//		}
//		
//        assertEquals(message_count, 1);
//        assertTrue(original_message.equals(consumed_message));
	}
}
