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
package com.ikanow.aleph2.distributed_services.utils;

import static org.junit.Assert.*;

import java.util.Optional;

import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import org.I0Itec.zkclient.ZkClient;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.distributed_services.services.MockCoreDistributedServices;

public class TestKafkaUtils {

	
	MockCoreDistributedServices _cds;
	
	@Before
	public void setupCoreDistributedServices() throws Exception {
		_cds = new MockCoreDistributedServices();	
	}
	
	@Test
	public void test_kafkaConnectionFromZookeeper() throws Exception {
		final ObjectMapper mapper = BeanTemplateUtils.configureMapper(Optional.empty());
		assertEquals(Integer.toString(_cds.getKafkaBroker().getBrokerPort()), 
				KafkaUtils.getBrokerListFromZookeeper(_cds.getCuratorFramework(), Optional.empty(), mapper).split(":")[1]);
		assertEquals(Integer.toString(_cds.getKafkaBroker().getBrokerPort()), 
				KafkaUtils.getBrokerListFromZookeeper(_cds.getCuratorFramework(), Optional.of("/brokers/ids"), mapper).split(":")[1]);
	}
	
	@Test
	public void test_kafkaCaching() {
		
		final ZkClient zk_client = KafkaUtils.getNewZkClient();
		
		assertTrue("Topic should _not_ exist", !KafkaUtils.doesTopicExist("random_topic", zk_client));		

		// Check cached version, cache type 1:
		KafkaUtils.known_topics.put("random_topic", true);
		assertTrue("Topic does exist", KafkaUtils.doesTopicExist("random_topic", zk_client));		
		KafkaUtils.known_topics.put("random_topic", false);
		assertTrue("Topic does not exist", !KafkaUtils.doesTopicExist("random_topic", zk_client));		
		KafkaUtils.known_topics.asMap().remove("random_topic");
		
		// Check cached version, cache type 2:
		KafkaUtils.my_topics.put("random_topic", true);
		assertTrue("Topic does exist", KafkaUtils.doesTopicExist("random_topic", zk_client));		
		KafkaUtils.my_topics.put("random_topic", false);
		assertTrue("Topic does not exist", !KafkaUtils.doesTopicExist("random_topic", zk_client));		
		KafkaUtils.my_topics.remove("random_topic");
		
		//(adds to my_cache)
		assertTrue("Topic should _not_ exist", !KafkaUtils.doesTopicExist("random_topic", zk_client));
		
		// Create a topic
		KafkaUtils.createTopic("random_topic", Optional.empty(), zk_client);
		
		/**/
		System.out.println("-------------------");
		
		// Will initially return true because createTopic adds to my_topics
		assertTrue("Topic does exist", KafkaUtils.doesTopicExist("random_topic", zk_client));
		// Clear my_topics cache
		// Will return false because cached:
		KafkaUtils.my_topics.remove("random_topic");		
		assertTrue("Topic should _not_ exist", !KafkaUtils.doesTopicExist("random_topic", zk_client));
		
		// Clear cache:
		KafkaUtils.known_topics.asMap().remove("random_topic");
		// (finally will actually go check the topics)
		assertTrue("Topic does exist", KafkaUtils.doesTopicExist("random_topic", zk_client));		
	}
	
	/**
	 * Tests topic creation, asserts it exists after creation
	 * 
	 * @throws InterruptedException
	 */
	@Test
	public void testCreateTopic() throws InterruptedException {
		final String topic = "test_create";
		final ZkClient zk_client = KafkaUtils.getNewZkClient();
		KafkaUtils.createTopic(topic, Optional.empty(), zk_client);		
		Thread.sleep(5000);
		assertTrue(KafkaUtils.doesTopicExist(topic, zk_client));
	}
	
	/**
	 * Tests produce and consume into a kafka queue
	 * 
	 * @throws InterruptedException
	 */
	@Test
	public void testProduceConsume() throws InterruptedException {
		final String topic = "test_produce_consume";
		final ZkClient zk_client = KafkaUtils.getNewZkClient();
		KafkaUtils.createTopic(topic, Optional.empty(), zk_client);		
		Thread.sleep(5000);
		assertTrue(KafkaUtils.doesTopicExist(topic, zk_client));
		
		//write something into the topic
		Producer<String, String> producer = KafkaUtils.getKafkaProducer();
		long num_messages_to_produce = 5;
		for (long i = 0; i < num_messages_to_produce; i++)
			producer.send(new KeyedMessage<String, String>(topic, "test"));		
		
		Thread.sleep(5000);
		
		//see if we can read that items
		ConsumerConnector consumer2 = KafkaUtils.getKafkaConsumer(topic, Optional.empty());
		WrappedConsumerIterator wrapped_consumer2 = new WrappedConsumerIterator(consumer2, topic);		
		long count = 0;
		while ( wrapped_consumer2.hasNext() ) {
			wrapped_consumer2.next();
			count++;
		}
		wrapped_consumer2.close();
		assertEquals(count, num_messages_to_produce);
	}
	
	/**
	 * Tests deleting an existing topic.
	 * 1. Creates a new topic
	 * 2. Puts some data in topic
	 * 3. Deletes topic
	 * 4. Tries to get data, should not be able to (because its been deleted)
	 * 
	 * @throws InterruptedException
	 */
	@Ignore
	@Test
	public void testDeleteTopic() throws InterruptedException {
		final String topic = "test_delete_topic11";
		final ZkClient zk_client = KafkaUtils.getNewZkClient();
				
		//Create a topic to delete later
		KafkaUtils.createTopic(topic, Optional.empty(), zk_client);
		
		Thread.sleep(5000);
		
		//write something into the topic
		Producer<String, String> producer = KafkaUtils.getKafkaProducer();
		long num_messages_to_produce = 3;
		for (long i = 0; i < num_messages_to_produce; i++)
			producer.send(new KeyedMessage<String, String>(topic, "test"));		
		
		Thread.sleep(5000);
		
		//delete the topic
		assertTrue(KafkaUtils.doesTopicExist(topic, zk_client));
		KafkaUtils.deleteTopic(topic, zk_client);
		//need to wait a second for DeleteTopicsListener to pick up our delete request
		Thread.sleep(5000);	
		//NOTE: looks like the local kafka might not clean up topics
		assertFalse(KafkaUtils.doesTopicExist(topic, zk_client));
		
		//verify it doesn't exist AT ALL
		//TODO we shouldn't be able to grab the data in a consumer
		//TODO see what actually happens in here
		System.out.println("STARTING TO GET CONSUMER");
		//see if we can read that iem
		ConsumerConnector consumer1 = KafkaUtils.getKafkaConsumer(topic, Optional.empty());
		WrappedConsumerIterator wrapped_consumer1 = new WrappedConsumerIterator(consumer1, topic);
		System.out.println("LOOPING OVER MESSAGES");
		while ( wrapped_consumer1.hasNext() ) {
			System.out.println("NEXT: " + wrapped_consumer1.next());
		}
		wrapped_consumer1.close();
		fail();
	}
	
	/**
	 * Tests sending a topic that doesn't exist for deletion, should
	 * just ignore the request and carry on.
	 * 
	 */
	@Test
	public void testDeleteNonExistantTopic() {		
		final String topic = "test_delete_topic_dne";
		final ZkClient zk_client = KafkaUtils.getNewZkClient();				
		assertFalse(KafkaUtils.doesTopicExist(topic, zk_client));
		//delete topic just quietly ignores the request
		KafkaUtils.deleteTopic(topic, zk_client);
		assertFalse(KafkaUtils.doesTopicExist(topic, zk_client));
		//assert that its been kicked from the caches
		Boolean does_topic_exist = KafkaUtils.known_topics.getIfPresent(topic);
		if ( does_topic_exist != null ) {
			assertFalse(does_topic_exist.booleanValue());
		}
		assertFalse(KafkaUtils.my_topics.containsKey(topic));
	}
}
