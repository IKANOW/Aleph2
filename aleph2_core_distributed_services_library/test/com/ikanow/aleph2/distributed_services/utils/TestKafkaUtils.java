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

import org.I0Itec.zkclient.ZkClient;
import org.junit.Before;
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
	
}
