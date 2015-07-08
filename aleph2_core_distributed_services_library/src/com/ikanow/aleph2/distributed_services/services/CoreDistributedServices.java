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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;

import scala.Tuple3;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.cluster.Cluster;
import akka.cluster.seed.ZookeeperClusterSeed;
import akka.event.japi.LookupEventBus;

import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IExtraDependencyLoader;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.distributed_services.data_model.DistributedServicesPropertyBean;
import com.ikanow.aleph2.distributed_services.data_model.IBroadcastEventBusWrapper;
import com.ikanow.aleph2.distributed_services.data_model.IJsonSerializable;
import com.ikanow.aleph2.distributed_services.modules.CoreDistributedServicesModule;
import com.ikanow.aleph2.distributed_services.utils.KafkaUtils;
import com.ikanow.aleph2.distributed_services.utils.WrappedConsumerIterator;
import com.ikanow.aleph2.distributed_services.utils.ZookeeperUtils;
import com.typesafe.config.ConfigFactory;

/** Implementation class for full Curator service
 * @author acp
 *
 */
public class CoreDistributedServices implements ICoreDistributedServices, IExtraDependencyLoader {

	protected CompletableFuture<Boolean> _joined_akka_cluster = new CompletableFuture<>();
	protected final CuratorFramework _curator_framework;
	protected final ActorSystem _akka_system;
	private final static Logger logger = LogManager.getLogger();
	protected final DistributedServicesPropertyBean _config_bean;
	protected final Runnable _shutdown_hook;
	
	protected final static ConcurrentHashMap<Tuple3<String, String, String>, RemoteBroadcastMessageBus<?>> _buses = 
			new ConcurrentHashMap<Tuple3<String, String, String>, RemoteBroadcastMessageBus<?>>();
	
	/** Guice-invoked constructor
	 * @throws Exception 
	 */
	@Inject
	public CoreDistributedServices(DistributedServicesPropertyBean config_bean) throws Exception {
		_config_bean = config_bean;
		
		final String connection_string = Optional.ofNullable(config_bean.zookeeper_connection())
											.orElse(DistributedServicesPropertyBean.__DEFAULT_ZOOKEEPER_CONNECTION);
		
		logger.info("Zookeeper connection_string=" + connection_string);
		
		final RetryPolicy retry_policy = new ExponentialBackoffRetry(1000, 3);
		_curator_framework = CuratorFrameworkFactory.newClient(connection_string, retry_policy);
		_curator_framework.start();		
		
		// Set up a config for Akka overrides
		final Map<String, Object> config_map = ImmutableMap.<String, Object>builder()
											//.put("akka.loglevel", "DEBUG") // (just in case it's quickly needed during unit testing)
											.put("akka.actor.provider", "akka.cluster.ClusterActorRefProvider")
											.put("akka.extensions", Arrays.asList("akka.cluster.pubsub.DistributedPubSub"))
											.put("akka.remote.netty.tcp.port", "0")
											.put("akka.cluster.seed.zookeeper.url", connection_string)
											.put("akka.cluster.auto-down-unreachable-after", "120s")
											.put("akka.actor.serializers.jackson", "com.ikanow.aleph2.distributed_services.services.JsonSerializerService")
											.put("akka.actor.serialization-bindings.\"com.ikanow.aleph2.distributed_services.data_model.IJsonSerializable\"", "jackson")
											.build();		
	
		// WORKAROUND FOR BUG IN akka-cluster/akka-zookeeper-seed: if it grabs the old ephemeral connection info of master then bad things can happen
		// so wait until a ZK node that I create for this purpose is removed (so the others also should have been)
		final String application_name = config_bean.application_name();
		final String hostname_application = DistributedServicesPropertyBean.ZOOKEEPER_APPLICATION_LOCK + "/" + ZookeeperUtils.getHostname() + ":" + application_name;
		if (null == application_name) {
			logger.info("(This is a transient application, cannot be the master)");
		}
		else {
			logger.info("Checking for old ZK artefacts from old instance of this application path=" + hostname_application);
			final int MAX_ZK_ATTEMPTS = 6;
			int i = 0;
			for (i = 0; i <= MAX_ZK_ATTEMPTS; ++i) {
				try {
					this.getCuratorFramework().create()
						.creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(hostname_application);
					
					Thread.sleep(2000L); // (Wait a little longer)
					break;
				}
				catch (Exception e) {
					logger.warn(ErrorUtils.getLongForm("Waiting for old instance to be cleared out (err={0}), retrying={1}", e, i < MAX_ZK_ATTEMPTS));
					try { Thread.sleep(10000L); } catch (Exception __) {}
				}
			}
			if (i > MAX_ZK_ATTEMPTS) {
				throw new RuntimeException("Failed to clear out lock, not clear why - try removing by hand: " + (DistributedServicesPropertyBean.ZOOKEEPER_APPLICATION_LOCK + "/" + hostname_application));
			}			
		}
		
		_akka_system = ActorSystem.create("default", ConfigFactory.parseMap(config_map));
		ZookeeperClusterSeed.get(_akka_system).join();
		
		_shutdown_hook = Lambdas.wrap_runnable_u(() -> {
			_joined_akka_cluster = new CompletableFuture<>(); //(mainly just for testing)
			Cluster.get(_akka_system).leave(ZookeeperClusterSeed.get(_akka_system).address());
			// If it's an application, not transient, then handle synchronization
			if (null != application_name) {
				logger.info("Shutting down in 5s");
				// (don't delete the ZK node - appear to still be able to run into race problems if you do, left here to remind me):
				//this.getCuratorFramework().delete().deletingChildrenIfNeeded().forPath(hostname_application);
				Thread.sleep(5000L);
			}
			else {
				logger.info("Shutting down now");					
			}
		});
		Cluster.get(_akka_system).registerOnMemberUp(() -> {
			logger.info("Joined cluster address=" + ZookeeperClusterSeed.get(_akka_system).address() +", adding shutdown hook");
			_joined_akka_cluster.complete(true);
			
			// Now register a shutdown hook
			Runtime.getRuntime().addShutdownHook(new Thread(_shutdown_hook));
		});
		
		final String broker_list_string = Optional.ofNullable(config_bean.broker_list())
				.orElse(DistributedServicesPropertyBean.__DEFAULT_BROKER_LIST);
		final Map<String, Object> config_map_kafka = ImmutableMap.<String, Object>builder()
				.put("metadata.broker.list", broker_list_string)
				.put("serializer.class", "kafka.serializer.StringEncoder")
				.put("request.required.acks", "1")
				.put("zookeeper.connect", connection_string)
				.put("group.id", "somegroup")
				.put("zookeeper.session.timeout.ms", "400")
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

	/** Pass the local bindings module to the parent
	 * @return
	 */
	public static List<AbstractModule> getExtraDependencyModules() {
		return Arrays.asList(new CoreDistributedServicesModule());
	}
	
	@Override
	public void youNeedToImplementTheStaticFunctionCalled_getExtraDependencyModules() {
		// done!
		
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
		LookupEventBus<M, ActorRef, String> getBroadcastMessageBus(final Class<M> wrapper_clazz, final Class<U> base_message_clazz, final String topic) {
		
		final Tuple3<String, String, String> key = Tuples._3T(wrapper_clazz.getName(), base_message_clazz.getName(), topic);
		
		@SuppressWarnings("unchecked")
		RemoteBroadcastMessageBus<M> ret_val = (RemoteBroadcastMessageBus<M>) _buses.get(key);
		
		if (null == ret_val) {
			_buses.put(key, (ret_val = new RemoteBroadcastMessageBus<M>(_akka_system, topic)));
		}
		return ret_val;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices#produce(java.lang.String, java.lang.String)
	 */
	@Override
	public void produce(String topic, String message) {
		KafkaUtils.createTopic(topic);
		
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
	public Iterator<String> consume(String topic) {
		logger.debug("CONSUMING");
		ConsumerConnector consumer = KafkaUtils.getKafkaConsumer(topic);
		return new WrappedConsumerIterator(consumer, topic);
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

	private FiniteDuration _default_akka_join_timeout = Duration.create(60, TimeUnit.SECONDS);
	
	/** Really just for testing
	 * @param new_timeout
	 */
	protected void setAkkaJoinTimeout(FiniteDuration new_timeout) {
		_default_akka_join_timeout = new_timeout;
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices#waitForAkkaJoin(java.util.Optional)
	 */
	@Override
	public boolean waitForAkkaJoin(Optional<FiniteDuration> timeout) {
		try {
			if (_joined_akka_cluster.isDone()) {
				return true;
			}
			else {
				logger.info("Waiting for cluster to start up");
				_joined_akka_cluster.get(timeout.orElse(_default_akka_join_timeout).toMillis(), TimeUnit.MILLISECONDS);
			}
			return true;
		} catch (Exception e) {
			logger.info("Cluster timed out: " + timeout.orElse(_default_akka_join_timeout) + ", throw_error=" + !timeout.isPresent());
			if (!timeout.isPresent()) {
				throw new RuntimeException("waitForAkkaJoin timeout");
			}
			return false;
		}
	}
}
