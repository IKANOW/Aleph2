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

import java.io.Serializable;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.apache.curator.framework.CuratorFramework;

import scala.concurrent.duration.FiniteDuration;

import com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService;
import com.ikanow.aleph2.distributed_services.data_model.IBroadcastEventBusWrapper;
import com.ikanow.aleph2.distributed_services.data_model.IRoundRobinEventBusWrapper;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.event.japi.LookupEventBus;

/** Provides general access to distributed services in the cluster - eg distributed mutexes, control messaging, data queue access
 * @author acp
 */
public interface ICoreDistributedServices extends IUnderlyingService {

	/** Returns the application role associated with this process, or optional if none (eg transient processes)
	 * @return
	 */
	Optional<String> getApplicationName();
	
	/** Returns a connection to the Curator server
	 * @return
	 */
	CuratorFramework getCuratorFramework();
		
	/** Waits for the node to become part of the Akka cluster - until then, no management db operations should be performed
	 * @param timeout - optional timeout, if left blank, will generate exception after 60s 
	 * @return true if joined, false otherwise
	 */
	boolean waitForAkkaJoin(final Optional<FiniteDuration> timeout);
	
	/** Wait for Akka node to become part of the Akka cluster and then run the designated code
	 * @return a future that completes when the task is complete, and contains any errors (via the ExecutionException)
	 */
	CompletableFuture<Void> runOnAkkaJoin(Runnable task);
	
	/** Returns a connector to the Akka infrastructure
	 * @return
	 */
	ActorSystem getAkkaSystem();

	/** An empty message used to tell singleton actor's that they have been removed from the leadership role and are about to be closed
	 */
	public static class SingletonEndMessage {		
	};
	
	/** Returns an actor that will be a singleton across the cluster - ie start it on all nodes, but it will only run on one node at a time
	 * @param actor_config - the standard actor-defining configuration
	 * @param for_role - singletons can only be created for a specific role (eg DataImportManager, DataAnalyticsManager)
	 * @return Optionally (if the role matches this process) the cluster manager - note the actual actor cannot be interacted with directly from this ActorRef, only via actorSelection ("using the names you have specified when creating the ClusterSingletonManager"). Best solution is to have the master register itself with a message bus.
	 */
	Optional<ActorRef> createSingletonActor(final String actor_name, final Set<String> for_roles, final Props actor_config);
	
	/** Returns a broadcast message bus for a specific topic
	 * @param wrapper_clazz - the class of the _wrapper_ (not the underlying message)
	 * @param topic
	 * @return
	 */
	<U extends Serializable, M extends IBroadcastEventBusWrapper<U>> LookupEventBus<M, ActorRef, String> getBroadcastMessageBus(final Class<M> wrapper_clazz, final Class<U> base_message_clazz, final String topic);
	
	/** Returns a random  message bus for a specific topic
	 * @param wrapper_clazz - the class of the _wrapper_ (not the underlying message)
	 * @param topic
	 * @return
	 */
	<U extends Serializable, M extends IRoundRobinEventBusWrapper<U>> LookupEventBus<M, ActorRef, String> getRoundRobinMessageBus(final Class<M> wrapper_clazz, final Class<U> base_message_clazz, final String topic);
	
	/** Writes a JSON string to the designated message queue
	 * @param topic - the name of the message queue, eg for buckets will usually be KafkaUtils.bucketNameToKafkaTopic(bucket.full_name)
	 * @param message - a string, in most cases will represent a JSON object
	 */
	void produce(String topic, String message);
	
	/** Returns an iterator from which messages can be read
	 * @param topic - the name of the message queue, eg for buckets will usually be KafkaUtils.bucketNameToKafkaTopic(bucket.full_name)
	 * @return an iterator of Strings, typically representing JSON stringd
	 */
	Iterator<String> consume(String topic);
	
}
