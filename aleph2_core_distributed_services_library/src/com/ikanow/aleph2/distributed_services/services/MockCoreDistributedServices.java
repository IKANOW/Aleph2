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

import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import org.I0Itec.zkclient.ZkClient;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.concurrent.duration.FiniteDuration;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.event.japi.LookupEventBus;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.utils.SetOnce;
import com.ikanow.aleph2.distributed_services.data_model.IBroadcastEventBusWrapper;
import com.ikanow.aleph2.distributed_services.data_model.IRoundRobinEventBusWrapper;
import com.ikanow.aleph2.distributed_services.utils.KafkaUtils;
import com.ikanow.aleph2.distributed_services.utils.MockKafkaBroker;
import com.ikanow.aleph2.distributed_services.utils.WrappedConsumerIterator;
import com.typesafe.config.ConfigFactory;

/** Implementation class for standalone Curator instance
 * @author acp
 *
 */
public class MockCoreDistributedServices implements ICoreDistributedServices {

	protected final SetOnce<TestingServer> _test_server = new SetOnce<>();
	protected final SetOnce<CuratorFramework> _curator_framework = new SetOnce<>(); // (this is quite annoying for testing, so I'm going to make it lazy)
	protected final SetOnce<ZkClient> _kafka_zk_framework = new SetOnce<>(); //(ZkClient is a less well maintained curator-esque library)
	protected final ActorSystem _akka_system;
	private final SetOnce<MockKafkaBroker> _kafka_broker = new SetOnce<>(); // (this is quite annoying for testing, so I'm going to make it lazy)
	private final static Logger logger = LogManager.getLogger();
	
	protected String _mock_application_name = null;
	
	/** For testing, lets the developer create an application name for the mock CDS object
	 * @param application_name
	 */
	public void setApplicationName(final String application_name) {
		_mock_application_name = application_name;
	}
	
	/** Guice-invoked constructor
	 * @throws Exception 
	 */
	@Inject
	public MockCoreDistributedServices() throws Exception {
		setupKafka(); // (also sets up curator) lazy initialization didn't seem to work - maybe MockKafkaBroker takes a few seconds to become available, not worth worrying about for now 
		
		_akka_system = ActorSystem.create("default");		
	}	
	 
	/** Lazy initialization for Kafka
	 */
	private void setupKafka() {
		setupCurator();
		synchronized (this) {
			if (!_kafka_broker.isSet()) {
				try {
					_kafka_broker.set(new MockKafkaBroker(_test_server.get().getConnectString()));
					
					final Map<String, Object> config_map_kafka = ImmutableMap.<String, Object>builder()
							.put("metadata.broker.list", "127.0.0.1:" + getKafkaBroker().getBrokerPort())
							.put("zookeeper.connect", _test_server.get().getConnectString())
							.build();	
					KafkaUtils.setProperties(ConfigFactory.parseMap(config_map_kafka));
					
					_kafka_zk_framework.set(KafkaUtils.getNewZkClient());
				}
				catch (Exception e) { // (just make unchecked)
					throw new RuntimeException(e);
				}
			}
		}
	}

	/** Lazy initialization for Curator/zookeeper
	 */
	private void setupCurator() {
		synchronized (this) {
			try {
				if (!_test_server.isSet()) {
					_test_server.set(new TestingServer());
					_test_server.get().start();
					RetryPolicy retry_policy = new ExponentialBackoffRetry(1000, 3);
					_curator_framework.set(CuratorFrameworkFactory.newClient(_test_server.get().getConnectString(), retry_policy));
					_curator_framework.get().start();
				}
			}
			catch (Exception e) { // (just make unchecked)
				throw new RuntimeException(e);
			}
		}
	}
	
	/** Test function to enable tests to find out where the "mock" ZK server is running
	 * @return
	 */
	public String getConnectString() {
		setupCurator();
		return _test_server.get().getConnectString();
	}
	
	/** Returns a connection to the Curator server
	 * @return
	 */
	public CuratorFramework getCuratorFramework() {
		this.setupCurator();
		return _curator_framework.get();
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
	public <U extends Serializable, M extends IBroadcastEventBusWrapper<U>> 
		LookupEventBus<M, ActorRef, String> getBroadcastMessageBus(final Class<M> wrapper_clazz, final Class<U> base_message_clazz, final String topic)
	{		
		return new LocalBroadcastMessageBus<M>(topic);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices#getRoundRobinMessageBus(java.lang.Class, java.lang.Class, java.lang.String)
	 */
	@Override
	public <U extends Serializable, M extends IRoundRobinEventBusWrapper<U>> LookupEventBus<M, ActorRef, String> getRoundRobinMessageBus(
			Class<M> wrapper_clazz, Class<U> base_message_clazz, String topic) {
		return new LocalRoundRobinMessageBus<M>(topic);
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices#getSingletonActor(java.lang.String, akka.actor.Props)
	 */
	@Override
	public Optional<ActorRef> createSingletonActor(final String actor_name, final Set<String> for_roles, final Props actor_config) {
		//(single node - therefore just generates a single actor as per usual)
		return Optional.of(_akka_system.actorOf(actor_config, actor_name));
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices#waitForAkkaJoin(java.util.Optional)
	 */
	@Override
	public boolean waitForAkkaJoin(Optional<FiniteDuration> timeout) {
		return true;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices#runOnAkkaJoin(java.util.Optional, java.lang.Runnable)
	 */
	@Override
	public CompletableFuture<Void> runOnAkkaJoin(Runnable task) {
		return CompletableFuture.runAsync(task);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices#createTopic(java.lang.String)
	 */
	@Override
	public 	void createTopic(String topic, Optional<Map<String, Object>> options) {
		setupKafka();
		logger.debug("CREATING " + topic);
		KafkaUtils.createTopic(topic, options, _kafka_zk_framework.get());
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices#deleteTopic(java.lang.String)
	 */
	@Override
	public void deleteTopic(String topic) {
		setupKafka();
		logger.debug("DELETE " + topic);
		KafkaUtils.deleteTopic(topic, _kafka_zk_framework.get());
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices#produce(java.lang.String, java.lang.String)
	 */
	@Override
	public void produce(String topic, String message) {
		this.createTopic(topic, Optional.empty());
		
		logger.debug("PRODUCING");
		Producer<String, String> producer = KafkaUtils.getKafkaProducer();	
		logger.debug("SENDING");
		producer.send(new KeyedMessage<String, String>(topic, message));
		logger.debug("DONE SENDING");
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices#consume(java.lang.String)
	 */
	@Override
	public Iterator<String> consumeAs(String topic, Optional<String> consumer_name) {
		setupKafka();
		logger.debug("CONSUMING");
		ConsumerConnector consumer = KafkaUtils.getKafkaConsumer(topic, consumer_name);
		return new WrappedConsumerIterator(consumer, topic);
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices#generateTopicName(java.lang.String, java.util.Optional)
	 */
	@Override
	public String generateTopicName(String path, Optional<String> subchannel) {
		return KafkaUtils.bucketPathToTopicName(path, subchannel.filter(sc -> !sc.equals(QUEUE_START_ALIAS.get())));		
	}	
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices#doesTopicExist(java.lang.String)
	 */
	@Override
	public boolean doesTopicExist(String topic) {
		return KafkaUtils.doesTopicExist(topic, _kafka_zk_framework.get());
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService#getUnderlyingArtefacts()
	 */
	@Override
	public Collection<Object> getUnderlyingArtefacts() {
		return Arrays.asList(this);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService#getUnderlyingPlatformDriver(java.lang.Class, java.util.Optional)
	 */
	@Override
	public <T> Optional<T> getUnderlyingPlatformDriver(Class<T> driver_class,
			Optional<String> driver_options) {
		return Optional.empty();
	}
	
	/** Stops a kafka broker
	 */
	public void kill() {
		getKafkaBroker().stop();
	}

	/** Returns the current Kafka broker
	 * @return
	 */
	public MockKafkaBroker getKafkaBroker() {
		setupKafka();
		return _kafka_broker.get();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices#getApplicationName()
	 */
	@Override
	public Optional<String> getApplicationName() {
		return Optional.ofNullable(_mock_application_name);
	}

}
