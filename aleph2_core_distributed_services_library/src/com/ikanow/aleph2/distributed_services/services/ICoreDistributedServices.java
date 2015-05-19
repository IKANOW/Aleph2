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

import org.apache.curator.framework.CuratorFramework;
import org.checkerframework.checker.nullness.qual.NonNull;

import akka.actor.ActorSystem;

/** Provides general access to distributed services in the cluster - eg distributed mutexes, control messaging, data queue access
 * @author acp
 */
public interface ICoreDistributedServices {

	/** Returns a connection to the Curator server
	 * @return
	 */
	@NonNull
	CuratorFramework getCuratorFramework();
		
	/** Returns a connector to the Akka infrastructure
	 * @return
	 */
	@NonNull
	ActorSystem getAkkaSystem();

	//TODO (ALEPH-19): need to decide on Kafka API
	
	/** Returns a Kafka producer
	 * @return
	 */
//	@NonNull
//	<T1, T2> Producer<T1, T2> getKafkaProducer();
	
	/** Returns a Kafka consumer of messages
	 * @return
	 */
//	@NonNull
//	ConsumerConnector getKafkaConsumer();
	
}
