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

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

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
import akka.event.japi.LookupEventBus;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.ikanow.aleph2.distributed_services.data_model.IBroadcastEventBusWrapper;
import com.ikanow.aleph2.distributed_services.data_model.IJsonSerializable;
import com.ikanow.aleph2.distributed_services.utils.KafkaUtils;
import com.ikanow.aleph2.distributed_services.utils.MockKafkaBroker;
import com.ikanow.aleph2.distributed_services.utils.WrappedConsumerIterator;
import com.typesafe.config.ConfigFactory;

/** Implementation class for standalone Curator instance
 * @author acp
 *
 */
public class MockCoreDistributedServices implements ICoreDistributedServices {

	protected final TestingServer _test_server;
	protected final CuratorFramework _curator_framework;
	protected final ActorSystem _akka_system;
	private final MockKafkaBroker _kafka_broker;
	private final static Logger logger = LogManager.getLogger();
	
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
		_kafka_broker = new MockKafkaBroker(_test_server.getConnectString());
		
		final Map<String, Object> config_map_kafka = ImmutableMap.<String, Object>builder()
				.put("metadata.broker.list", "127.0.0.1:" + getKafkaBroker().getBrokerPort())
				.put("serializer.class", "kafka.serializer.StringEncoder")
				.put("request.required.acks", "1")
				.put("zookeeper.connect", _test_server.getConnectString())
				.put("group.id", "somegroup")
				.put("zookeeper.session.timeout.ms", "1200")
				.put("zookeeper.sync.time.ms", "200")
		        .put("auto.commit.interval.ms", "1000")			
				.build();	
		KafkaUtils.setProperties(ConfigFactory.parseMap(config_map_kafka));
		
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
		//TODO memoize this function?
		KafkaUtils.createTopic(topic);
		
		//TODO we should probably cache producers for a bit? or return back some wrapped object?
		logger.debug("PRODUCING");
		Producer<String, String> producer = KafkaUtils.getKafkaProducer();	
		logger.debug("SENDING");
		producer.send(new KeyedMessage<String, String>(topic, message));
		logger.debug("DONE SENDING");
		//TODO close out the producer, we shouldn't be doing this after each message because its costly to start		
		//producer.close();
	}
	
	@Override
	public Iterator<String> consume(String topic) {
		logger.debug("CONSUMING");
		ConsumerConnector consumer = KafkaUtils.getKafkaConsumer(topic);
		return new WrappedConsumerIterator(consumer, topic);
	}
	
	@Override
	public Collection<Object> getUnderlyingArtefacts() {
		return Arrays.asList(this);
	}

	@Override
	public <T> Optional<T> getUnderlyingPlatformDriver(Class<T> driver_class,
			Optional<String> driver_options) {
		return Optional.empty();
	}
	
	public void kill() {
		getKafkaBroker().stop();
	}

	@Override
	public boolean waitForAkkaJoin(Optional<FiniteDuration> timeout) {
		return true;
	}

	public MockKafkaBroker getKafkaBroker() {
		return _kafka_broker;
	}
}
