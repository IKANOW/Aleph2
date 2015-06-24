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
 ******************************************************************************/
package com.ikanow.aleph2.distributed_services.services;

//import kafka.javaapi.consumer.ConsumerConnector;
//import kafka.javaapi.producer.Producer;

import java.util.Iterator;

import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.message.MessageAndMetadata;

import org.apache.curator.framework.CuratorFramework;

import com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService;
import com.ikanow.aleph2.distributed_services.data_model.IBroadcastEventBusWrapper;
import com.ikanow.aleph2.distributed_services.data_model.IJsonSerializable;
import com.ikanow.aleph2.distributed_services.utils.WrappedConsumerIterator;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.event.japi.LookupEventBus;

/** Provides general access to distributed services in the cluster - eg distributed mutexes, control messaging, data queue access
 * @author acp
 */
public interface ICoreDistributedServices extends IUnderlyingService {

	/** Returns a connection to the Curator server
	 * @return
	 */
	CuratorFramework getCuratorFramework();
		
	/** Returns a connector to the Akka infrastructure
	 * @return
	 */
	ActorSystem getAkkaSystem();

	/** Returns a message bus for a specific topic
	 * @param wrapper_clazz - the class of the _wrapper_ (not the underlying message)
	 * @param topic
	 * @return
	 */
	<U extends IJsonSerializable, M extends IBroadcastEventBusWrapper<U>> LookupEventBus<M, ActorRef, String> getBroadcastMessageBus(final Class<M> wrapper_clazz, final Class<U> base_message_clazz, final String topic);
	
	//TODO (ALEPH-19): need to decide on Kafka API
	//TODO hide these interfaces so we aren't exposing kafka things, just take what we need to produce and do it internally
	/** Returns a Kafka producer, you can then call producer.send()
	 * @return
	 */
//	<T1, T2> Producer<T1, T2> getKafkaProducer();
//	
//	/** Returns a Kafka consumer of messages
//	 * @return
//	 */
//	ConsumerConnector getKafkaConsumer();

	void produce(String topic, String message);
	Iterator<String> consume(String topic);
	
}
