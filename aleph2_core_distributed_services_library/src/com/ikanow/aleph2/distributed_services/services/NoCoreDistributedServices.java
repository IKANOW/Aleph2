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
package com.ikanow.aleph2.distributed_services.services;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.apache.curator.framework.CuratorFramework;

import scala.concurrent.duration.FiniteDuration;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.event.japi.LookupEventBus;

import com.ikanow.aleph2.distributed_services.data_model.IBroadcastEventBusWrapper;
import com.ikanow.aleph2.distributed_services.data_model.IRoundRobinEventBusWrapper;

/** Completely stubs out the ICoreDistributedServices library
 * @author Alex
 */
public class NoCoreDistributedServices implements ICoreDistributedServices {

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService#getUnderlyingArtefacts()
	 */
	@Override
	public Collection<Object> getUnderlyingArtefacts() {
		return Arrays.asList();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService#getUnderlyingPlatformDriver(java.lang.Class, java.util.Optional)
	 */
	@Override
	public <T> Optional<T> getUnderlyingPlatformDriver(Class<T> driver_class,
			Optional<String> driver_options) {
		return Optional.empty();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices#getApplicationName()
	 */
	@Override
	public Optional<String> getApplicationName() {
		return Optional.empty();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices#getCuratorFramework()
	 */
	@Override
	public CuratorFramework getCuratorFramework() {
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices#waitForAkkaJoin(java.util.Optional)
	 */
	@Override
	public boolean waitForAkkaJoin(Optional<FiniteDuration> timeout) {
		return true;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices#runOnAkkaJoin(java.lang.Runnable)
	 */
	@Override
	public CompletableFuture<Void> runOnAkkaJoin(Runnable task) {
		return CompletableFuture.completedFuture(null);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices#getAkkaSystem()
	 */
	@Override
	public ActorSystem getAkkaSystem() {
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices#createSingletonActor(java.lang.String, java.util.Set, akka.actor.Props)
	 */
	@Override
	public Optional<ActorRef> createSingletonActor(String actor_name,
			Set<String> for_roles, Props actor_config) {
		return Optional.empty();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices#getBroadcastMessageBus(java.lang.Class, java.lang.Class, java.lang.String)
	 */
	@Override
	public <U extends Serializable, M extends IBroadcastEventBusWrapper<U>> LookupEventBus<M, ActorRef, String> getBroadcastMessageBus(
			Class<M> wrapper_clazz, Class<U> base_message_clazz, String topic) {
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices#getRoundRobinMessageBus(java.lang.Class, java.lang.Class, java.lang.String)
	 */
	@Override
	public <U extends Serializable, M extends IRoundRobinEventBusWrapper<U>> LookupEventBus<M, ActorRef, String> getRoundRobinMessageBus(
			Class<M> wrapper_clazz, Class<U> base_message_clazz, String topic) {
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices#generateTopicName(java.lang.String, java.util.Optional)
	 */
	@Override
	public String generateTopicName(String path, Optional<String> subchannel) {
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices#createTopic(java.lang.String, java.util.Optional)
	 */
	@Override
	public void createTopic(String topic, Optional<Map<String, Object>> options) {
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices#doesTopicExist(java.lang.String)
	 */
	@Override
	public boolean doesTopicExist(String topic) {
		return false;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices#deleteTopic(java.lang.String)
	 */
	@Override
	public void deleteTopic(String topic) {
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices#produce(java.lang.String, java.lang.String)
	 */
	@Override
	public void produce(String topic, String message) {
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices#consumeAs(java.lang.String, java.util.Optional)
	 */
	@Override
	public Iterator<String> consumeAs(String topic,
			Optional<String> consumer_name) {
		return null;
	}

}
