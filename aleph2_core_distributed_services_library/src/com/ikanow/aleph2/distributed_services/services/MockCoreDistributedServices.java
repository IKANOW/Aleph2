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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.message.MessageAndMetadata;
import kafka.producer.ProducerConfig;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.event.japi.LookupEventBus;

import com.google.inject.Inject;
import com.ikanow.aleph2.distributed_services.data_model.IBroadcastEventBusWrapper;
import com.ikanow.aleph2.distributed_services.data_model.IJsonSerializable;
import com.ikanow.aleph2.distributed_services.utils.WrappedConsumerIterator;

/** Implementation class for standalone Curator instance
 * @author acp
 *
 */
public class MockCoreDistributedServices implements ICoreDistributedServices {

	protected final TestingServer _test_server;
	protected final CuratorFramework _curator_framework;
	protected final ActorSystem _akka_system;
	
	/** Guice-invoked constructor
	 * @throws Exception 
	 */
	@Inject
	public MockCoreDistributedServices() throws Exception {
		_test_server = new TestingServer();
		_test_server.start();
		RetryPolicy retry_policy = new ExponentialBackoffRetry(1000, 3);
		_curator_framework = CuratorFrameworkFactory.newClient(_test_server.getConnectString(), retry_policy);
		_curator_framework.start();		
		
		_akka_system = ActorSystem.create("default");
	}	
	 
	/** Returns a connection to the Curator server
	 * @return
	 */
	public CuratorFramework getCuratorFramework() {
		return _curator_framework;
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices#getAkkaSystem()
	 */
	@Override
	public ActorSystem getAkkaSystem() {
		return _akka_system;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices#getBroadcastMessageBus(java.lang.Class, java.lang.String)
	 */
	@Override
	public <U extends IJsonSerializable, M extends IBroadcastEventBusWrapper<U>> 
		LookupEventBus<M, ActorRef, String> getBroadcastMessageBus(final Class<M> wrapper_clazz, final Class<U> base_message_clazz, final String topic)
	{		
		return new LocalBroadcastMessageBus<M>(topic);
	}

	@Override
	public void produce(String topic, String message) {
		//this pretends to send a message, does nothing
		MockProducer producer = new MockProducer(true);
		producer.send(new ProducerRecord<byte[], byte[]>(topic, message.getBytes()));
		producer.close();
	}
	
	@Override
	public Iterator<String> consume(String topic) {
		MockConsumer consumer = new MockConsumer();
		Map<TopicPartition, Long> topicCountMap = new HashMap<TopicPartition, Long>();
		topicCountMap.put(new TopicPartition(topic, 0), 0L);
		consumer.commit(topicCountMap, true);
		List<String> results = new ArrayList<String>();
		Map<String, ConsumerRecords<byte[], byte[]>> map = consumer.poll(1000);
		ConsumerRecords<byte[], byte[]> records = map.get(topic);
		if ( records != null ) {
			List<ConsumerRecord<byte[], byte[]>> record_list = records.records(0);
			if ( record_list != null ) {
				for( ConsumerRecord<byte[], byte[]> record : record_list) {
					try {
						results.add(new String( record.value()));
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}
				
		consumer.close();
		return results.iterator();		
	}
}
