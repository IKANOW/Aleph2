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
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import org.I0Itec.zkclient.ZkClient;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;

import scala.Tuple2;
import scala.Tuple3;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.seed.ZookeeperClusterSeed;
import akka.cluster.singleton.ClusterSingletonManager;
import akka.cluster.singleton.ClusterSingletonManagerSettings;
import akka.event.japi.LookupEventBus;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IExtraDependencyLoader;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Functions;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.SetOnce;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.distributed_services.data_model.DistributedServicesPropertyBean;
import com.ikanow.aleph2.distributed_services.data_model.IBroadcastEventBusWrapper;
import com.ikanow.aleph2.distributed_services.data_model.IRoundRobinEventBusWrapper;
import com.ikanow.aleph2.distributed_services.modules.CoreDistributedServicesModule;
import com.ikanow.aleph2.distributed_services.utils.KafkaUtils;
import com.ikanow.aleph2.distributed_services.utils.WrappedConsumerIterator;
import com.ikanow.aleph2.distributed_services.utils.ZookeeperUtils;
import com.typesafe.config.ConfigFactory;

//TODO: ALEPH-12: have a singleton actor responsible for checking N topics/minute, and deciding if they still exist or not

/** Implementation class for full Curator service
 * @author acp
 *
 */
public class CoreDistributedServices implements ICoreDistributedServices, IExtraDependencyLoader {
	private final static Logger logger = LogManager.getLogger();
	
	protected final ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());
	protected final DistributedServicesPropertyBean _config_bean;

	// Curator, instantiated lazily (in practice immediately because we need it for Kafka)
	protected final SetOnce<CuratorFramework> _curator_framework = new SetOnce<>();
	protected final SetOnce<ZkClient> _kafka_zk_framework = new SetOnce<>(); //(ZkClient is a less well maintained curator-esque library)

	// Akka, instantiated laziy
	protected final SetOnce<ActorSystem> _akka_system = new SetOnce<>();
	protected CompletableFuture<Boolean> _joined_akka_cluster = new CompletableFuture<>();
	protected boolean _has_joined_akka_cluster = false; 
	protected final SetOnce<Runnable> _shutdown_hook = new SetOnce<>();
	protected final LinkedList<Tuple2<CompletableFuture<Void>, Runnable>> _post_join_task_list = new LinkedList<>();
	
	// Kafka, initialize in the background
	boolean _initializing_kafka = true;
	protected final CompletableFuture<Void> _initialized_kafka;
	
	protected final static ConcurrentHashMap<Tuple3<String, String, String>, RemoteBroadcastMessageBus<?>> _broadcast_buses = 
			new ConcurrentHashMap<>();
	protected final static ConcurrentHashMap<Tuple3<String, String, String>, RemoteRoundRobinMessageBus<?>> _roundrobin_buses = 
			new ConcurrentHashMap<>();
	
	/** Guice-invoked constructor
	 * @throws Exception 
	 */
	@Inject
	public CoreDistributedServices(DistributedServicesPropertyBean config_bean) throws Exception {
		
		final String connection_string = Optional.ofNullable(config_bean.zookeeper_connection())
											.orElse(DistributedServicesPropertyBean.__DEFAULT_ZOOKEEPER_CONNECTION);

		_config_bean = BeanTemplateUtils.clone(config_bean).with(DistributedServicesPropertyBean::zookeeper_connection, connection_string).done();		
		
		logger.info("Zookeeper connection_string=" + _config_bean.zookeeper_connection());
				
		// Else join akka cluster lazily, because often it's not required at all
		if (null != config_bean.application_name()) {
			joinAkkaCluster();			
		}		
		
		if (null != config_bean.broker_list()) {
			final String broker_list_string = config_bean.broker_list();
			KafkaUtils.setStandardKafkaProperties(_config_bean.zookeeper_connection(), broker_list_string, 
					Optional.ofNullable(_config_bean.cluster_name()).orElse(DistributedServicesPropertyBean.__DEFAULT_CLUSTER_NAME));			
			_initialized_kafka = new CompletableFuture<>();
			_initialized_kafka.complete(null);
			_initializing_kafka = false;
		}
		else { // Launch an async process to intialize kafka
			logger.info("Fetching Kafka broker_list from Zookeeper");
			_initialized_kafka = CompletableFuture.runAsync(() -> {
				try {
					final String broker_list = KafkaUtils.getBrokerListFromZookeeper(this.getCuratorFramework(), Optional.empty(), _mapper);
					KafkaUtils.setStandardKafkaProperties(_config_bean.zookeeper_connection(), broker_list,										
							Optional.ofNullable(_config_bean.cluster_name()).orElse(DistributedServicesPropertyBean.__DEFAULT_CLUSTER_NAME));			
					logger.info("Kafka broker_list=" + broker_list);
					
					_kafka_zk_framework.set(KafkaUtils.getNewZkClient());
				}
				catch (Exception e) { // just use the default and hope:
					KafkaUtils.setStandardKafkaProperties(_config_bean.zookeeper_connection(), DistributedServicesPropertyBean.__DEFAULT_BROKER_LIST,
							Optional.ofNullable(_config_bean.cluster_name()).orElse(DistributedServicesPropertyBean.__DEFAULT_CLUSTER_NAME));			
				}
				_initializing_kafka = false;
			});
		}
	}
	
	/** Joins the Akka cluster
	 */
	protected void joinAkkaCluster() {
		if (!_akka_system.isSet()) {
			this.getAkkaSystem(); // (this will also join the cluster)
			return;
		}
		if (!_has_joined_akka_cluster) {
			_has_joined_akka_cluster = true;
			
			// WORKAROUND FOR BUG IN akka-cluster/akka-zookeeper-seed: if it grabs the old ephemeral connection info of master then bad things can happen
			// so wait until a ZK node that I create for this purpose is removed (so the others also should have been)
			final String application_name = _config_bean.application_name();
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
						logger.warn(ErrorUtils.get("Waiting for old instance to be cleared out (err={0}), retrying={1}", e.getMessage(), i < MAX_ZK_ATTEMPTS));
						try { Thread.sleep(10000L); } catch (Exception __) {}
					}
				}
				if (i > MAX_ZK_ATTEMPTS) {
					throw new RuntimeException("Failed to clear out lock, not clear why - try removing by hand: " + (DistributedServicesPropertyBean.ZOOKEEPER_APPLICATION_LOCK + "/" + hostname_application));
				}			
			}
			
			ZookeeperClusterSeed.get(_akka_system.get()).join();
			
			_shutdown_hook.set(Lambdas.wrap_runnable_u(() -> {
				_joined_akka_cluster = new CompletableFuture<>(); //(mainly just for testing)
				Cluster.get(_akka_system.get()).leave(ZookeeperClusterSeed.get(_akka_system.get()).address());
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
			}));
			Cluster.get(_akka_system.get()).registerOnMemberUp(() -> {
				logger.info("Joined cluster address=" + ZookeeperClusterSeed.get(_akka_system.get()).address() +", adding shutdown hook");
				synchronized (_joined_akka_cluster) { // (prevents a race condition vs runOnAkkaJoin)
					_joined_akka_cluster.complete(true);
				}
				// Now register a shutdown hook
				Runtime.getRuntime().addShutdownHook(new Thread(_shutdown_hook.get()));
				
				_post_join_task_list.stream().parallel().forEach(retval_task -> {
					try {
						retval_task._2().run();
						retval_task._1().complete(null);
					}
					catch (Throwable t) {
						retval_task._1().completeExceptionally(t);
					}
				});				
			});
		}
	}
	
	/** Returns a connection to the Curator server
	 * @return
	 */
	public synchronized CuratorFramework getCuratorFramework() {
		if (!_curator_framework.isSet()) {
			final RetryPolicy retry_policy = new ExponentialBackoffRetry(1000, 3);
			_curator_framework.set(CuratorFrameworkFactory.newClient(_config_bean.zookeeper_connection(), retry_policy));
			_curator_framework.get().start();					
		}
		return _curator_framework.get();
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
	public synchronized boolean waitForAkkaJoin(Optional<FiniteDuration> timeout) {
		joinAkkaCluster(); // (does nothing if already joined)
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
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices#runOnAkkaJoin(java.util.Optional, java.lang.Runnable)
	 */
	@Override
	public CompletableFuture<Void> runOnAkkaJoin(Runnable task) {
		synchronized (_joined_akka_cluster) {
			if (_joined_akka_cluster.isDone()) {
				return CompletableFuture.runAsync(task);
			}
			else {
				final CompletableFuture<Void> on_complete = new CompletableFuture<>();		
				_post_join_task_list.add(Tuples._2T(on_complete, task));				
				return on_complete;
			}
		}
	}
	
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices#getAkkaSystem()
	 */
	@Override
	public synchronized ActorSystem getAkkaSystem() {
		if (!_akka_system.isSet()) {
			
			// Get port or use 0 as alternative
			Integer port = Optional.ofNullable(_config_bean.application_port()).orElse(Collections.emptyMap())
										.getOrDefault(_config_bean.application_name(), 0);
			
			logger.info(ErrorUtils.get("Using port {0} for application {1}",
					0 != port ? port.toString() : "(transient)",
					Optional.ofNullable(_config_bean.application_name()).orElse("(transient)")
					));
			
			// Set up a config for Akka overrides
			final ImmutableMap.Builder<String, Object> config_map_builder = 
			ImmutableMap.<String, Object>builder()
												//.put("akka.loglevel", "DEBUG") // (just in case it's quickly needed during unit testing)
												.put("akka.actor.provider", "akka.cluster.ClusterActorRefProvider")
												.put("akka.extensions", Arrays.asList("akka.cluster.pubsub.DistributedPubSub"))
												.put("akka.remote.netty.tcp.port", port.toString())
												.put("akka.cluster.seed.zookeeper.url", _config_bean.zookeeper_connection())
												.put("akka.cluster.auto-down-unreachable-after", "120s")
												.put("akka.cluster.pub-sub.routing-logic", "round-robin")
												;

			if (null != _config_bean.application_name()) {
				config_map_builder.put("akka.cluster.roles", Arrays.asList(_config_bean.application_name()));
			}			
			final Map<String, Object> config_map = config_map_builder.build();;
			_akka_system.set(ActorSystem.create(Optional.ofNullable(_config_bean.cluster_name()).orElse(DistributedServicesPropertyBean.__DEFAULT_CLUSTER_NAME), 
								ConfigFactory.parseMap(config_map)));
			this.joinAkkaCluster();
		}
		return _akka_system.get();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices#getBroadcastMessageBus(java.lang.Class, java.lang.String)
	 */
	@Override
	public <U extends Serializable, M extends IBroadcastEventBusWrapper<U>> 
		LookupEventBus<M, ActorRef, String> getBroadcastMessageBus(final Class<M> wrapper_clazz, final Class<U> base_message_clazz, final String topic) {
		this.waitForAkkaJoin(Optional.empty());
		
		final Tuple3<String, String, String> key = Tuples._3T(wrapper_clazz.getName(), base_message_clazz.getName(), topic);
		
		@SuppressWarnings("unchecked")
		RemoteBroadcastMessageBus<M> ret_val = (RemoteBroadcastMessageBus<M>) _broadcast_buses.get(key);
		
		if (null == ret_val) {
			_broadcast_buses.put(key, (ret_val = new RemoteBroadcastMessageBus<M>(this.getAkkaSystem(), topic)));
		}
		return ret_val;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices#getRoundRobinMessageBus(java.lang.Class, java.lang.Class, java.lang.String)
	 */
	@Override
	public <U extends Serializable, M extends IRoundRobinEventBusWrapper<U>> LookupEventBus<M, ActorRef, String> getRoundRobinMessageBus(
			Class<M> wrapper_clazz, Class<U> base_message_clazz, String topic) {
		this.waitForAkkaJoin(Optional.empty());
		
		final Tuple3<String, String, String> key = Tuples._3T(wrapper_clazz.getName(), base_message_clazz.getName(), topic);
		
		@SuppressWarnings("unchecked")
		RemoteRoundRobinMessageBus<M> ret_val = (RemoteRoundRobinMessageBus<M>) _roundrobin_buses.get(key);
		
		if (null == ret_val) {
			_roundrobin_buses.put(key, (ret_val = new RemoteRoundRobinMessageBus<M>(this.getAkkaSystem(), topic)));
		}
		return ret_val;
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices#getSingletonActor(akka.actor.Props)
	 */
	@Override
	public Optional<ActorRef> createSingletonActor(final String actor_name, final Set<String> for_roles, final Props actor_config) {
		if (for_roles.isEmpty()) {
			logger.warn(ErrorUtils.get("Called createSingletonActor for {0} with no roles so never start", actor_name));
		}
		return for_roles.contains(_config_bean.application_name())
				?
				Optional.of(getAkkaSystem().actorOf(ClusterSingletonManager.props(actor_config, new SingletonEndMessage(), 
								ClusterSingletonManagerSettings.create(getAkkaSystem()).withRole(_config_bean.application_name()).withSingletonName(actor_name)
								)))
				:
				Optional.empty();
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices#createTopic(java.lang.String)
	 */
	@Override
	public 	void createTopic(String topic, Optional<Map<String, Object>> options) {
		if (_initializing_kafka) { //(wait for async to complete)
			_initialized_kafka.join();
		}		
		logger.debug("CREATING " + topic);
		KafkaUtils.createTopic(topic, options, _kafka_zk_framework.get());				
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices#deleteTopic(java.lang.String)
	 */
	@Override
	public void deleteTopic(String topic) {
		if (_initializing_kafka) { //(wait for async to complete)
			_initialized_kafka.join();
		}		
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
		final Producer<String, String> producer = KafkaUtils.getKafkaProducer();
		
		logger.debug("SENDING");
		producer.send(new KeyedMessage<String, String>(topic, message));
		
		logger.debug("DONE SENDING");
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices#consume(java.lang.String)
	 */
	@Override
	public Iterator<String> consumeAs(String topic, Optional<String> consumer_name) {
		if (_initializing_kafka) { //(wait for async to complete)
			_initialized_kafka.join();
		}		
		logger.debug("CONSUMING");
		final ConsumerConnector consumer = KafkaUtils.getKafkaConsumer(topic, consumer_name);
		return new WrappedConsumerIterator(consumer, topic);
	}

	/** Memoized version of generateTopicName
	 */
	private Function<Tuple2<String, Optional<String>>, String> generateTopicName_internal = Functions.memoize(path_subchannel -> {
		return KafkaUtils.bucketPathToTopicName(path_subchannel._1(), path_subchannel._2().filter(sc -> !sc.equals(QUEUE_START_ALIAS.get())));		
	});
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices#generateTopicName(java.lang.String, java.util.Optional)
	 */
	@Override
	public String generateTopicName(String path, Optional<String> subchannel) {
		return generateTopicName_internal.apply(Tuples._2T(path, subchannel));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices#doesTopicExist(java.lang.String)
	 */
	@Override
	public boolean doesTopicExist(String topic) {
		if (_initializing_kafka) { //(wait for async to complete)
			_initialized_kafka.join();
		}				
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

	/** Pass the local bindings module to the parent
	 * @return
	 */
	public static List<AbstractModule> getExtraDependencyModules() {
		return Arrays.asList(new CoreDistributedServicesModule());
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IExtraDependencyLoader#youNeedToImplementTheStaticFunctionCalled_getExtraDependencyModules()
	 */
	@Override
	public void youNeedToImplementTheStaticFunctionCalled_getExtraDependencyModules() {
		// done!
		
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices#getApplicationName()
	 */
	@Override
	public Optional<String> getApplicationName() {
		return Optional.ofNullable(_config_bean.application_name());
	}
}
