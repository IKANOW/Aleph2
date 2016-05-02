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

import kafka.utils.ZkUtils;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
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
		//Thread.sleep(10000);
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
		//Thread.sleep(10000);
		assertTrue(KafkaUtils.doesTopicExist(topic, zk_client));
		
		//have to create consumers before producing
		KafkaConsumer<String, String> consumer2 = KafkaUtils.getKafkaConsumer(topic, Optional.empty());
		WrappedConsumerIterator wrapped_consumer2 = new WrappedConsumerIterator(consumer2, topic, 2000);		
		
		//write something into the topic
		Producer<String, String> producer = KafkaUtils.getKafkaProducer();
		long num_messages_to_produce = 5;
		for (long i = 0; i < num_messages_to_produce; i++)
			producer.send(new ProducerRecord<String, String>(topic, "test"));
//			producer.send(new ProducerRecord<String, String>(topic, "test"));		
		
		Thread.sleep(10000); //wait a few seconds for producers to dump batch
		
		//see if we can read that items
		
		long count = 0;
		while ( wrapped_consumer2.hasNext() ) {
			wrapped_consumer2.next();
			count++;
		}
		wrapped_consumer2.close();
		assertEquals(count, num_messages_to_produce);
	}
	
	/**
	 * Some systems were holding on to one consumer and checking hasNext perioidically.  This test
	 * ensures that if you check hasNext and it returns false (because there are no items in the queue)
	 * that the connection stays active and you can check it again later.
	 * 
	 * @throws InterruptedException
	 */
	@Test
	public void testConsumerEmptyHasNext() throws InterruptedException {
		final String topic = "testConsumerEmptyHasNext";
		final ZkUtils zk_client = KafkaUtils.getNewZkClient();
		KafkaUtils.createTopic(topic, Optional.empty(), zk_client);
		
		//create a consumer
		KafkaConsumer<String, String> consumer1 = KafkaUtils.getKafkaConsumer(topic, Optional.empty());
		WrappedConsumerIterator wrapped_consumer1 = new WrappedConsumerIterator(consumer1, topic);
		
		wrapped_consumer1.hasNext();
		
		//produce something
		Producer<String, String> producer = KafkaUtils.getKafkaProducer();
		producer.send(new ProducerRecord<String, String>(topic, "asdfsa"));
		//check if it exists
		Thread.sleep(5000); //wait a few seconds for producers to dump batch
		
		long count = 0;
		while ( wrapped_consumer1.hasNext() ) {
			wrapped_consumer1.next();
			count++;
		}
		wrapped_consumer1.close();
		assertEquals(count, 1);
		
		
	}
	
	/**
	 * Creates 2 topics, 2 consumers w/ diff groupnames, then produce/consume on both topics.
	 * Should be able to receive all messages because they have diff names and are pointed at different topics.
	 * @throws InterruptedException
	 */
	@Test
	public void testProduceConsumeTwoTopicDiffGroups() throws InterruptedException {
		final String topic1 = "test_produce_consume2_1";
		final String topic2 = "test_produce_consume2_2";
		final ZkUtils zk_client = KafkaUtils.getNewZkClient();
		KafkaUtils.createTopic(topic1, Optional.empty(), zk_client);		
		KafkaUtils.createTopic(topic2, Optional.empty(), zk_client);
		assertTrue(KafkaUtils.doesTopicExist(topic1, zk_client));
		assertTrue(KafkaUtils.doesTopicExist(topic2, zk_client));
		
		//have to create consumers before producing
		KafkaConsumer<String, String> consumer1 = KafkaUtils.getKafkaConsumer(topic1, Optional.of("g1"));
		WrappedConsumerIterator wrapped_consumer1 = new WrappedConsumerIterator(consumer1, topic1, 2000);		
		KafkaConsumer<String, String> consumer2 = KafkaUtils.getKafkaConsumer(topic2, Optional.of("g2"));
		WrappedConsumerIterator wrapped_consumer2 = new WrappedConsumerIterator(consumer2, topic2, 2000);
		
		
		//write something into the topic		
		Producer<String, String> producer = KafkaUtils.getKafkaProducer();
		long num_messages_to_produce = 5;
		for (long i = 0; i < num_messages_to_produce; i++) {
			producer.send(new ProducerRecord<String, String>(topic1, "test1"));
			producer.send(new ProducerRecord<String, String>(topic2, "test2"));
		}
		
		Thread.sleep(10000); //wait a few seconds for producers to dump batch
		
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
	 * Create 2 topics, 2 consumers w/ the same group name, then produce/consume on both topics.
	 * Should be able to receive all messages because they are pointed at different topics.
	 * @throws InterruptedException
	 */
	@Test
	public void testProduceConsumeTwoTopicSameGroups() throws InterruptedException {
		final String topic1 = "test_produce_consume1";
		final String topic2 = "test_produce_consume2";
		final ZkUtils zk_client = KafkaUtils.getNewZkClient();
		KafkaUtils.createTopic(topic1, Optional.empty(), zk_client);		
		KafkaUtils.createTopic(topic2, Optional.empty(), zk_client);
		//Thread.sleep(10000);
		assertTrue(KafkaUtils.doesTopicExist(topic1, zk_client));
		assertTrue(KafkaUtils.doesTopicExist(topic2, zk_client));
		
		//have to create consumers before producing
		KafkaConsumer<String, String> consumer1 = KafkaUtils.getKafkaConsumer(topic1, Optional.empty()); //defaults to random_uuid
		WrappedConsumerIterator wrapped_consumer1 = new WrappedConsumerIterator(consumer1, topic1, 2000);		
		KafkaConsumer<String, String> consumer2 = KafkaUtils.getKafkaConsumer(topic2, Optional.empty()); //defaults to random_uuid
		WrappedConsumerIterator wrapped_consumer2 = new WrappedConsumerIterator(consumer2, topic2, 2000);
		
		
		//write something into the topic		
		Producer<String, String> producer = KafkaUtils.getKafkaProducer();
		long num_messages_to_produce = 5;
		for (long i = 0; i < num_messages_to_produce; i++) {
			producer.send(new ProducerRecord<String, String>(topic1, "test1"));
			producer.send(new ProducerRecord<String, String>(topic2, "test2"));
		}
		
		Thread.sleep(10000); //wait a few seconds for producers to dump batch
		
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
	 * Set 2 topics up and 2 consumers pointed to that topic (w/ same groupname).
	 * They should each get part of the produced messages (have to go above the batch limit to get
	 * data to both).
	 * 
	 * @throws InterruptedException
	 */
	@Ignore //this test is ignored currently because we don't allow partitioning, so the 2nd consumer will never get anything
	@Test
	public void testProduceConsumeOneTopicSameGroups() throws InterruptedException {
		final String topic1 = "test_produce_consume1";
		final ZkUtils zk_client = KafkaUtils.getNewZkClient();
		KafkaUtils.createTopic(topic1, Optional.empty(), zk_client);	
		//Thread.sleep(10000);
		assertTrue(KafkaUtils.doesTopicExist(topic1, zk_client));
		
		//have to create consumers before producing
		KafkaConsumer<String, String> consumer1 = KafkaUtils.getKafkaConsumer(topic1, Optional.empty()); //defaults to aleph2_unknown
		WrappedConsumerIterator wrapped_consumer1 = new WrappedConsumerIterator(consumer1, topic1, 2000);		
		KafkaConsumer<String, String> consumer2 = KafkaUtils.getKafkaConsumer(topic1, Optional.empty()); //defaults to aleph2_unknown
		WrappedConsumerIterator wrapped_consumer2 = new WrappedConsumerIterator(consumer2, topic1, 2000);
		
		
		//write something into the topic		
		Producer<String, String> producer = KafkaUtils.getKafkaProducer();
		long num_messages_to_produce = 5000;
		for (long i = 0; i < num_messages_to_produce; i++) {			
			producer .send(new ProducerRecord<String, String>(topic1, "test1"));
		}
		
		Thread.sleep(10000); //wait a few seconds for producers to dump batch
		
		//see if we can read that items from topic1			
		long count = 0;
		while ( wrapped_consumer1.hasNext() ) {
			wrapped_consumer1.next();
			count++;
		}
		
		assertTrue(count > 0); //should have received some number of messages		
		
		//see if we can read that items from topic2
		count = 0;
		while ( wrapped_consumer2.hasNext() ) {
			wrapped_consumer2.next();
			count++;
		}		
		
		assertTrue(count > 0); //should have received some number of messages
		
		wrapped_consumer1.close();
		wrapped_consumer2.close();
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
		
//		Thread.sleep(10000);
		
		//write something into the topic
		Producer<String, String> producer = KafkaUtils.getKafkaProducer();
		long num_messages_to_produce = 3;
		for (long i = 0; i < num_messages_to_produce; i++)
			producer.send(new ProducerRecord<String, String>(topic, "test"));		
		
		Thread.sleep(10000); //wait a few seconds for producers to dump batch
		
		//delete the topic
		assertTrue(KafkaUtils.doesTopicExist(topic, zk_client));
		KafkaUtils.deleteTopic(topic, zk_client);
		//need to wait a second for DeleteTopicsListener to pick up our delete request
		Thread.sleep(10000);	
		//NOTE: looks like the local kafka might not clean up topics
		assertFalse(KafkaUtils.doesTopicExist(topic, zk_client));
		
		//verify it doesn't exist AT ALL
		//TODO we shouldn't be able to grab the data in a consumer
		//TODO see what actually happens in here
		//see if we can read that iem
		KafkaConsumer<String, String> consumer1 = KafkaUtils.getKafkaConsumer(topic, Optional.empty());
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
		
		KafkaUtils.createTopic(topic, Optional.empty(), zk_client);		
		//Thread.sleep(10000);
		assertTrue(KafkaUtils.doesTopicExist(topic, zk_client));
		
		//create a named consumer before we start producing
		KafkaConsumer<String, String> consumer = KafkaUtils.getKafkaConsumer(topic, Optional.of(group_id));
		@SuppressWarnings("resource")
		WrappedConsumerIterator wrapped_consumer = new WrappedConsumerIterator(consumer, topic, 2000);
		
		//write something into the topic
		Producer<String, String> producer = KafkaUtils.getKafkaProducer();
		long num_messages_to_produce = 5;
		for (long i = 0; i < num_messages_to_produce; i++) {
			producer.send(new ProducerRecord<String, String>(topic, "test_pt1_" + i));
		}
		Thread.sleep(10000); //sleep to wait for records getting moved
		
		//see if we can read that items
		long count = 0;
		while ( wrapped_consumer.hasNext() ) {
			wrapped_consumer.next();
			count++;
		}
		assertEquals(count, num_messages_to_produce);
		
		//assert consumer exists
		assertTrue(zk_client.pathExists(ZkUtils.ConsumersPath() + "/" + group_id));
		//close consumer
		wrapped_consumer.close();
		//assert consumer no longer exists
		//NOTE: current consumer does not delete this entry out, you have to manually handle it
		//we could delete it via ZKUtils.deletePathRecursively but waiting until 0.8.2 to see how that handles
		//TODO when we want to fully kill consumers we can put this line back in
		//assertFalse(ZkUtils.pathExists(zk_client, ZkUtils.ConsumersPath() + "/" + group_id));
		
		consumer = KafkaUtils.getKafkaConsumer(topic, Optional.of(group_id));
		wrapped_consumer = new WrappedConsumerIterator(consumer, topic, 2000);	
		
		//assert we can reuse the same consumer
		//write something into the topic, again
		for (long i = 0; i < num_messages_to_produce; i++)
			producer.send(new ProducerRecord<String, String>(topic, "test_pt2"));				
		Thread.sleep(10000); //sleep to wait for records getting moved
		
		//see if we can read that items			
		count = 0;
		while ( wrapped_consumer.hasNext() ) {
			wrapped_consumer.next();
			count++;
		}
		assertEquals(count, num_messages_to_produce);
	}
	
	/**
	 * Tests creating a second consumer pointed at a topic (w/o closing the first).  Will only
	 * work if both have different names.
	 * @throws InterruptedException 
	 * 
	 */
	@SuppressWarnings("resource")
	@Test
	public void testTwoConsumerSameTopic() throws InterruptedException {
		final String topic = "testTwoConsumerSameTopic";
		final ZkUtils zk_client = KafkaUtils.getNewZkClient();
		KafkaUtils.createTopic(topic, Optional.empty(), zk_client);		
		//Thread.sleep(10000);
		assertTrue(KafkaUtils.doesTopicExist(topic, zk_client));
		
		//CONSUMER 1 - PRODUCE A MESSAGE AND MAKE SURE IT GETS IT
		
		//have to create consumers before producing
		KafkaConsumer<String, String> consumer1 = KafkaUtils.getKafkaConsumer(topic, Optional.of("c1"));
		WrappedConsumerIterator wrapped_consumer1 = new WrappedConsumerIterator(consumer1, topic, 2000);
		Thread.sleep(5000);
		//write something into the topic
		Producer<String, String> producer = KafkaUtils.getKafkaProducer();
		long num_messages_to_produce = 5;
		for (long i = 0; i < num_messages_to_produce; i++) {
			producer.send(new ProducerRecord<String, String>(topic, "test"));
		}
		
		Thread.sleep(5000); //wait a few seconds for producers to dump batch
		
		//see if we can read that items
		
		long count = 0;
		while ( wrapped_consumer1.hasNext() ) {
			wrapped_consumer1.next();
			count++;
		}
		assertEquals(count, num_messages_to_produce);
		
		//CONSUMER 2 - Now create a consumer pointed at same topic, produce a message, make sure consumer 2 gets it
		KafkaConsumer<String, String> consumer2 = KafkaUtils.getKafkaConsumer(topic, Optional.of("c2"));
		WrappedConsumerIterator wrapped_consumer2 = new WrappedConsumerIterator(consumer2, topic, 2000);	
		Thread.sleep(5000);
		
		//write something into the topic
		for (long i = 0; i < num_messages_to_produce; i++) {
			producer.send(new ProducerRecord<String, String>(topic, "test"));
		}
		
		Thread.sleep(5000); //wait a few seconds for producers to dump batch
		
		//see if we can read that items	

		long count1 = 0;
		while ( wrapped_consumer1.hasNext() ) {
			wrapped_consumer1.next();
			count1++;
		}
		long count2 = 0;
		while ( wrapped_consumer2.hasNext() ) {
			wrapped_consumer2.next();
			count2++;
		}
		//because the consumers have different names they should both be able to read
		assertEquals(count1, num_messages_to_produce); //old consumer should get the new messages
		assertEquals(count2, num_messages_to_produce); //new consumer gets all the results
	}
	
	@SuppressWarnings("resource")
	@Test
	public void testTwoConsumerSameTopic2() throws InterruptedException {
		final String topic = "testTwoConsumerSameTopic";
		final ZkUtils zk_client = KafkaUtils.getNewZkClient();
		KafkaUtils.createTopic(topic, Optional.empty(), zk_client);		
		//Thread.sleep(10000);
		assertTrue(KafkaUtils.doesTopicExist(topic, zk_client));
		Producer<String, String> producer = KafkaUtils.getKafkaProducer();
		long num_messages_to_produce = 5;
		//CONSUMER 1 - PRODUCE A MESSAGE AND MAKE SURE IT GETS IT
		
		//have to create consumers before producing
		KafkaConsumer<String, String> consumer1 = KafkaUtils.getKafkaConsumer(topic, Optional.of("c1"));
		WrappedConsumerIterator wrapped_consumer1 = new WrappedConsumerIterator(consumer1, topic, 2000);
		//write something into the topic
		
		for (long i = 0; i < num_messages_to_produce; i++) {
			producer.send(new ProducerRecord<String, String>(topic, "test"));
		}
		
		Thread.sleep(5000); //wait a few seconds for producers to dump batch
		
		//see if we can read that items
		
		long count = 0;
		while ( wrapped_consumer1.hasNext() ) {
			wrapped_consumer1.next();
			count++;
		}
		assertEquals(count, num_messages_to_produce);
		
		//CONSUMER 2 - Now create a consumer pointed at same topic, produce a message, make sure consumer 2 gets it
		KafkaConsumer<String, String> consumer2 = KafkaUtils.getKafkaConsumer(topic, Optional.of("c2"));
		WrappedConsumerIterator wrapped_consumer2 = new WrappedConsumerIterator(consumer2, topic, 2000);	
		
		//write something into the topic
		for (long i = 0; i < num_messages_to_produce; i++) {
			producer.send(new ProducerRecord<String, String>(topic, "test"));
		}
		
		Thread.sleep(5000); //wait a few seconds for producers to dump batch
		
		//see if we can read that items	

		long count1 = 0;
		while ( wrapped_consumer1.hasNext() ) {
			wrapped_consumer1.next();
			count1++;
		}
		long count2 = 0;
		while ( wrapped_consumer2.hasNext() ) {
			wrapped_consumer2.next();
			count2++;
		}
		//because the consumers have different names they should both be able to read
		assertEquals(count1, num_messages_to_produce); //old consumer should get the new messages
		assertEquals(count2, num_messages_to_produce); //new consumer gets all the results
	}
	
	@Test
	public void testProduceConsumeLongGroupid() throws InterruptedException {
		final String topic = "test_produce_consume";
		final ZkUtils zk_client = KafkaUtils.getNewZkClient();
		KafkaUtils.createTopic(topic, Optional.empty(), zk_client);		
		//Thread.sleep(10000);
		assertTrue(KafkaUtils.doesTopicExist(topic, zk_client));
		
		//have to create consumers before producing
		final String from = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
		final String groupid = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
		KafkaConsumer<String, String> consumer2 = KafkaUtils.getKafkaConsumer(topic, Optional.of(from), Optional.of(groupid));
		WrappedConsumerIterator wrapped_consumer2 = new WrappedConsumerIterator(consumer2, topic, 2000);		
		
		//write something into the topic
		Producer<String, String> producer = KafkaUtils.getKafkaProducer();
		long num_messages_to_produce = 5;
		for (long i = 0; i < num_messages_to_produce; i++)
			producer.send(new ProducerRecord<String, String>(topic, "test"));
//			producer.send(new ProducerRecord<String, String>(topic, "test"));		
		
		Thread.sleep(10000); //wait a few seconds for producers to dump batch
		
		//see if we can read that items
		
		long count = 0;
		while ( wrapped_consumer2.hasNext() ) {
			wrapped_consumer2.next();
			count++;
		}
		wrapped_consumer2.close();
		assertEquals(count, num_messages_to_produce);
	}
}
