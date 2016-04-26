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
package com.ikanow.aleph2.distributed_services.utils;

import static org.junit.Assert.*;

import java.util.Optional;

import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.utils.ZkUtils;

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
		_cds.getKafkaBroker(); // (Setup kafka)
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
		
		final ZkUtils zk_client = KafkaUtils.getNewZkClient();
		
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
		final ZkUtils zk_client = KafkaUtils.getNewZkClient();
		KafkaUtils.createTopic(topic, Optional.empty(), zk_client);		
		//Thread.sleep(5000);
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
		final ZkUtils zk_client = KafkaUtils.getNewZkClient();
		KafkaUtils.createTopic(topic, Optional.empty(), zk_client);		
		//Thread.sleep(5000);
		assertTrue(KafkaUtils.doesTopicExist(topic, zk_client));
		
		//have to create consumers before producing
		ConsumerConnector consumer2 = KafkaUtils.getKafkaConsumer(topic, Optional.empty());
		WrappedConsumerIterator wrapped_consumer2 = new WrappedConsumerIterator(consumer2, topic, 2000);		
		
		//write something into the topic
		Producer<String, String> producer = KafkaUtils.getKafkaProducer();
		long num_messages_to_produce = 5;
		for (long i = 0; i < num_messages_to_produce; i++)
			producer.send(new KeyedMessage<String, String>(topic, "test"));		
		
		Thread.sleep(5000); //wait a few seconds for producers to dump batch
		
		//see if we can read that items
		
		long count = 0;
		while ( wrapped_consumer2.hasNext() ) {
			wrapped_consumer2.next();
			count++;
		}
		wrapped_consumer2.close();
		assertEquals(count, num_messages_to_produce);
	}
	
	@Test
	public void testProduceConsumeSecondTopic() throws InterruptedException {
		final String topic1 = "test_produce_consume1";
		final String topic2 = "test_produce_consume2";
		final ZkUtils zk_client = KafkaUtils.getNewZkClient();
		KafkaUtils.createTopic(topic1, Optional.empty(), zk_client);		
		KafkaUtils.createTopic(topic2, Optional.empty(), zk_client);
		//Thread.sleep(5000);
		assertTrue(KafkaUtils.doesTopicExist(topic1, zk_client));
		assertTrue(KafkaUtils.doesTopicExist(topic2, zk_client));
		
		//have to create consumers before producing
		ConsumerConnector consumer1 = KafkaUtils.getKafkaConsumer(topic1, Optional.empty());
		WrappedConsumerIterator wrapped_consumer1 = new WrappedConsumerIterator(consumer1, topic1, 2000);		
		ConsumerConnector consumer2 = KafkaUtils.getKafkaConsumer(topic2, Optional.empty());
		WrappedConsumerIterator wrapped_consumer2 = new WrappedConsumerIterator(consumer2, topic2, 2000);
		
		
		//write something into the topic
		Producer<String, String> producer = KafkaUtils.getKafkaProducer();
		long num_messages_to_produce = 5;
		for (long i = 0; i < num_messages_to_produce; i++) {
			producer.send(new KeyedMessage<String, String>(topic1, "test1"));
			producer.send(new KeyedMessage<String, String>(topic2, "test2"));
		}
		
		Thread.sleep(5000); //wait a few seconds for producers to dump batch
		
		//see if we can read that items from topic1				
		long count = 0;
		while ( wrapped_consumer1.hasNext() ) {
			wrapped_consumer1.next();
			count++;
		}
		wrapped_consumer1.close();
		assertEquals(count, num_messages_to_produce);
		
		//see if we can read that items from topic2
				
		count = 0;
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
		final String topic = "test_delete_topic";
		final ZkUtils zk_client = KafkaUtils.getNewZkClient();
				
		//Create a topic to delete later
		KafkaUtils.createTopic(topic, Optional.empty(), zk_client);
		
//		Thread.sleep(5000);
		
		//write something into the topic
		Producer<String, String> producer = KafkaUtils.getKafkaProducer();
		long num_messages_to_produce = 3;
		for (long i = 0; i < num_messages_to_produce; i++)
			producer.send(new KeyedMessage<String, String>(topic, "test"));		
		
		Thread.sleep(5000); //wait a few seconds for producers to dump batch
		
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
		WrappedConsumerIterator wrapped_consumer1 = new WrappedConsumerIterator(consumer1, topic, 2000);
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
		final ZkUtils zk_client = KafkaUtils.getNewZkClient();				
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
	
	/**
	 * Tests creating a named consumer, then closing it and cleaning it up.
	 * 1. Create topic
	 * 2. Create consumer
	 * 3. Produce some data
	 * 4. Consume said data with previous consumer
	 * 5. Close consumer, assert it doesnt exist
	 * 6. Open consumer with same name
	 * 7. Produce some data
	 * 8. Consume said data
	 * 9. Close consumer
	 * @throws InterruptedException 
	 */
	@Ignore //This test currently fails when run with the group for some reason?
	//it also isn't really doing anything currently because kafka doesn't fully cleanup consumers
	//in ZK currently.
	@Test
	public void testConsumerCleanup() throws InterruptedException {
		final String topic = "test_consumer_cleanup";
		final String group_id = "test_consumer";
		final ZkUtils zk_client = KafkaUtils.getNewZkClient();
		
		System.out.println("CREATING TOPIC");
		KafkaUtils.createTopic(topic, Optional.empty(), zk_client);		
		//Thread.sleep(5000);
		assertTrue(KafkaUtils.doesTopicExist(topic, zk_client));
		
		System.out.println("CREATING CONSUMER");
		//create a named consumer before we start producing
		ConsumerConnector consumer = KafkaUtils.getKafkaConsumer(topic, Optional.of(group_id));
		@SuppressWarnings("resource")
		WrappedConsumerIterator wrapped_consumer = new WrappedConsumerIterator(consumer, topic, 2000);
		
		System.out.println("PRODUCE SOME DATA");
		//write something into the topic
		Producer<String, String> producer = KafkaUtils.getKafkaProducer();
		long num_messages_to_produce = 5;
		for (long i = 0; i < num_messages_to_produce; i++) {
			System.out.println("produce message: " + i);
			producer.send(new KeyedMessage<String, String>(topic, "test_pt1_" + i));
		}
		Thread.sleep(5000); //sleep to wait for records getting moved
		
		System.out.println("CONSUMING DATA");
		//see if we can read that items
		long count = 0;
		while ( wrapped_consumer.hasNext() ) {
			wrapped_consumer.next();
			count++;
		}
		assertEquals(count, num_messages_to_produce);
		
		System.out.println("DELETING CONSUMER");
		//assert consumer exists
		assertTrue(zk_client.pathExists(ZkUtils.ConsumersPath() + "/" + group_id));
		//close consumer
		wrapped_consumer.close();
		//assert consumer no longer exists
		//NOTE: current consumer does not delete this entry out, you have to manually handle it
		//we could delete it via ZKUtils.deletePathRecursively but waiting until 0.8.2 to see how that handles
		//TODO when we want to fully kill consumers we can put this line back in
		//assertFalse(ZkUtils.pathExists(zk_client, ZkUtils.ConsumersPath() + "/" + group_id));
		
		System.out.println("CREATING CONSUMER AGAIN, REUSING NAME");
		consumer = KafkaUtils.getKafkaConsumer(topic, Optional.of(group_id));
		wrapped_consumer = new WrappedConsumerIterator(consumer, topic, 2000);	
		
		System.out.println("PRODUCE SOME DATA");
		//assert we can reuse the same consumer
		//write something into the topic, again
		for (long i = 0; i < num_messages_to_produce; i++)
			producer.send(new KeyedMessage<String, String>(topic, "test_pt2"));				
		Thread.sleep(5000); //sleep to wait for records getting moved
		
		System.out.println("CONSUME DATA");
		//see if we can read that items			
		count = 0;
		while ( wrapped_consumer.hasNext() ) {
			wrapped_consumer.next();
			count++;
		}
		assertEquals(count, num_messages_to_produce);
	}
	
	@Test
	public void testSpeed() throws InterruptedException {
		final String topic = "test_speed";
		final ZkUtils zk_client = KafkaUtils.getNewZkClient();
		KafkaUtils.createTopic(topic, Optional.empty(), zk_client);		
		//Thread.sleep(5000);
		assertTrue(KafkaUtils.doesTopicExist(topic, zk_client));
		
		//have to create consumers before producing
		ConsumerConnector consumer2 = KafkaUtils.getKafkaConsumer(topic, Optional.empty());
		WrappedConsumerIterator wrapped_consumer2 = new WrappedConsumerIterator(consumer2, topic, 2000);		
		
		long start_time = System.currentTimeMillis();
		
		//write something into the topic
		Producer<String, String> producer = KafkaUtils.getKafkaProducer();
		long num_messages_to_produce = 500000;
		for (long i = 0; i < num_messages_to_produce; i++)
			producer.send(new KeyedMessage<String, String>(topic, "test"));		
		
		long time_to_produce = System.currentTimeMillis()-start_time;
		
		Thread.sleep(5000); //wait a few seconds for producers to dump batch
		
		//see if we can read that items
		
		long count = 0;
		while ( wrapped_consumer2.hasNext() ) {
			wrapped_consumer2.next();
			count++;
		}
		wrapped_consumer2.close();
		assertEquals(count, num_messages_to_produce);
		
		System.out.println("TEST RESULTS: num_produced: " + num_messages_to_produce + " time: " + time_to_produce + "ms");
	}
}
