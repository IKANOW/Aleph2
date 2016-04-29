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
package com.ikanow.aleph2.distributed_services.utils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.curator.framework.CuratorFramework;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Option;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.ikanow.aleph2.data_model.utils.BucketUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.UuidUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import kafka.admin.AdminUtils;
import kafka.api.TopicMetadata;
import kafka.utils.ZkUtils;
import kafka.utils.ZKStringSerializer$;

/**
 * Houses some static util functions for getting a kafka producer/consumer and
 * converting bucket names to kafka topic names.
 * 
 * @author Burch
 *
 */
public class KafkaUtils {
//	private static final Integer NUM_THREADS = 1;
	private static Properties kafka_properties = new Properties();
	private final static Logger logger = LogManager.getLogger();
	protected final static Map<String, Boolean> my_topics = new ConcurrentHashMap<String, Boolean>(); // (Things to which I am publishing)
	protected final static Cache<String, Boolean> known_topics = CacheBuilder.newBuilder().expireAfterWrite(5, TimeUnit.MINUTES).build();
	protected static Producer<String, String> producer = null;
	//TODO (ALEPH-12): make my_topics a cached map also
	
	/** Creates a new ZK client from the properties
	 * @return
	 */
	public synchronized static ZkUtils getNewZkClient() {
		return new ZkUtils(new ZkClient(kafka_properties.getProperty("zookeeper.connect"), 10000, 10000, ZKStringSerializer$.MODULE$),
				new ZkConnection(kafka_properties.getProperty("zookeeper.connect")), false)
		;
	}
	
	/**
	 * Returns a producer pointed at the currently configured Kafka instance.
	 * 
	 * Producers are reused for all topics so it's okay to not close this between
	 * uses.  Not sure what will happen if its left open for a long time.
	 * 
	 * @return
	 */
	public synchronized static Producer<String, String> getKafkaProducer() {			
		if ( producer == null )
			producer = new KafkaProducer<>(kafka_properties);
			//producer = new Producer<String, String>(new ProducerConfig(kafka_properties));
		
		return producer;
	}
	
	/**
	 * Returns a consumer pointed at the given topic.  The consumer group.id will be auto set to:
	 * <topic>__a2__<random_uuid>
	 * 
	 * WARNING: When a consumer is created, it starts its reading at now, so if you
	 * previously produced on a topic, this consumer won't be able to see it.
	 * 
	 * This consumer should be closed once you are done reading.
	 * 
	 * @param topic
	 * @return
	 */
	public static KafkaConsumer<String, String> getKafkaConsumer(final String topic) {
		return getKafkaConsumer(topic, Optional.empty());
	}
	
	/**
	 * Returns a consumer pointed at the given topic.  The consumer group.id will be auto set to:
	 * <topic>__<from|a2>__<random_uuid>
	 * 
	 * WARNING: When a consumer is created, it starts its reading at now, so if you
	 * previously produced on a topic, this consumer won't be able to see it.
	 * 
	 * This consumer should be closed once you are done reading.
	 * 
	 * @param topic
	 * @param from
	 * @return
	 */
	public static KafkaConsumer<String, String> getKafkaConsumer(final String topic, Optional<String> from) {
		return getKafkaConsumer(topic, from, Optional.empty());
	}
	
	/**
	 * Creates a consumer for a single topic with the currently configured Kafka instance.
	 * The consumer group.id will be set to: <topic>__<from|a2>__<consumer_name|random_uuid>
	 * 
	 * WARNING: When a consumer is created, it starts its reading at now, so if you
	 * previously produced on a topic, this consumer won't be able to see it.
	 * 
	 * This consumer should be closed once you are done reading.
	 * 
	 * Note: if you set the consumer_name you must be careful about creating consumers:
	 * 1. multiple consumers with different names pointed to same topic == everyone gets all data from the same topic
	 * 2. multiple consumers with different names pointed to different topics == everyone reads their own topics (like normal)
	 * 3. multiple consumers with same names pointed to same topic == should round-robin (partitioning is not currently setup in aleph2 (TODO))
	 * 4. multiple consumers with same names pointed to different topic == BROKEN (kafka issue)
	 *
	 * @param topic
	 * @param consumer_name - if set then uses a specific consumer group instead of the central "system" consumer - this has the effect of copying the data instead of round-robining it
	 * @return
	 */
	public static KafkaConsumer<String, String> getKafkaConsumer(final String topic, final Optional<String> from, final Optional<String> consumer_name) {
		final String groupid = topic + "__" +
			from.map(f->f).orElse("a2") + "__" +
			consumer_name.map(name -> name).orElse(UuidUtils.get().getRandomUuid());
		final Properties new_properties = addGroupIdToProps(groupid);
		final KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(new_properties);
		consumer.subscribe(Arrays.asList(topic));
		return consumer;
	}
	
	private static Properties addGroupIdToProps(final String groupid) {
		final Properties np = new Properties();
		kafka_properties.forEach((key, val) -> np.put(key, val));
		np.put("group.id", groupid);
		return np;
	}
	
	/** Returns the configured Zookeeper connection string, needed by some Kafka modules
	 * @return the ZK connection string
	 */
	public static String getZookeperConnectionString() {
		return (String) kafka_properties.get("zookeeper.connect");
	}
	
	/** Returns the configured Kafka broker list
	 * @return
	 */
	public static String getBrokers() {
		return (String) kafka_properties.get("metadata.broker.list");
	}
	
	/** Converts from a bucket path to a Kafka topic
	 * @param bucket_path
	 * @return
	 */
	public static String bucketPathToTopicName(final String bucket_path, final Optional<String> sub_channel) {
		return BucketUtils.getUniqueSignature(bucket_path, sub_channel);
	}
	
	/** Checks if a topic exists, caches the result
	 * @param topic
	 * @return
	 */
	public static boolean doesTopicExist(final String topic, final ZkUtils zk_client) {
		Boolean does_topic_exist = my_topics.get(topic);
		if (null != does_topic_exist) {
			return does_topic_exist.booleanValue();
		}
		does_topic_exist = known_topics.getIfPresent(topic);
		if (null != does_topic_exist) {
			return does_topic_exist.booleanValue();
		}		
		final boolean topic_exists = AdminUtils.topicExists(zk_client, topic);
		
		known_topics.put(topic, topic_exists);		
		return topic_exists;
	}
	
	/**
	 * Changes out this classes configured properties pointing at
	 * a kafka instance.  Also resets the current producer so a
	 * new one must be grabbed to point at the correct place.
	 * 
	 * Consumers should also grab a new instance to point at the
	 * correct place but we don't have a handle on created consumers.
	 * 
	 * @param parseMap
	 */
	public static void setProperties(Config parseMap) {
		kafka_properties = new Properties();
		final Map<String, Object> config_map_kafka = ImmutableMap.<String, Object>builder()
				.put("group.id", "aleph2_unknown")
				
				//producer specific config
				.put("serializer.class", "kafka.serializer.StringEncoder") 
				.put("request.required.acks", "1")
				.put("producer.type", "async") 
				.put("compression.codec", "2")
				.put("batch.size", "800") 		//latest version config I think		
				.put("batch.num.messages", "800") //old version config 0.8.0?
				
				//producer 0.9.0 config
				.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
				.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
				
				//consumer 0.9.0 config
				.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
				.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
				
				.put("consumer.timeout.ms", "3000")
		        .put("auto.commit.interval.ms", "1000")
		        // Not sure which of these 2 sets is correct, so will list them both!
		        // these are listed here: https://kafka.apache.org/08/configuration.html
				.put("zookeeper.session.timeout.ms", "6000")
				.put("zookeeper.connection.timeout.ms", "6000")
				.put("zookeeper.sync.time.ms", "2000")
				// these are listed here: http://kafka.apache.org/07/configuration.html
				.put("zk.connectiontimeout.ms", "6000")
				.put("zk.sessiontimeout.ms", "6000")
				.put("zk.synctime.ms", "2000")
				.put("delete.topic.enable", "true")
				.build();	
		
		final Config fullConfig = parseMap.withFallback(ConfigFactory.parseMap(config_map_kafka));
		fullConfig.entrySet().stream().forEach(e -> kafka_properties.put(e.getKey(), e.getValue().unwrapped()));

		//PRODUCER PROPERTIES		
		String broker = fullConfig.getString("metadata.broker.list");
		kafka_properties.put("bootstrap.servers", broker); //DEBUG TESTING 0.9.0 consumer
		logger.debug("BROKER: " + broker);
		
        //CONSUMER PROPERTIES		
		String zk = fullConfig.getString("zookeeper.connect");
		logger.debug("ZOOKEEPER: " + zk);
        
        //reset producer so a new one will be created
		if ( producer != null ) {
			producer.close();
			producer = null;
		}    
	}

	/** Generates a connection string by reading ZooKeeper
	 * @param curator
	 * @param path_override
	 * @return
	 * @throws Exception 
	 */
	public static String getBrokerListFromZookeeper(final CuratorFramework curator, Optional<String> path_override, final ObjectMapper mapper) throws Exception {
		final String path = path_override.orElse("/brokers/ids");
		final List<String> brokers = curator.getChildren().forPath(path);
		return brokers.stream()
			.map(Lambdas.wrap_u(broker_node -> new String(curator.getData().forPath(path + "/" + broker_node))))
			.flatMap(Lambdas.flatWrap_i(broker_str -> mapper.readTree(broker_str))) // (just discard any badly formatted nodes)
			.flatMap(Lambdas.flatWrap_i(json -> json.get("host").asText() + ":" + json.get("port").asText()))
			.collect(Collectors.joining(","));
	}
	
	/** A simpler set of Kafka properties, just requiring the ZK/broker list (note you can get the broker list from ZK using getBrokerListFromZookeeper)
	 * @param zk_connection
	 * @param broker_list
	 */
	public static void setStandardKafkaProperties(final String zk_connection, final String broker_list, final String cluster_name, Optional<Map<String,String>> optional_kafka_properties) {
		final Map<String, Object> config_map_kafka = ImmutableMap.<String, Object>builder()
				.putAll(optional_kafka_properties.orElse(Collections.emptyMap()))
				.put("metadata.broker.list", broker_list)
				.put("zookeeper.connect", zk_connection)
				.put("group.id", cluster_name)				
				.build();	
		KafkaUtils.setProperties(ConfigFactory.parseMap(config_map_kafka));		
	}
	
	/**
	 * Checks if a topic exists, if not creates a new kafka queue for it.
	 * 
	 * After creating a new topic, waits to see if a leader gets elected (it always seems to fail),
	 * then creates a consumer as a hack to get a leader elected and the offset set correctly.
	 * 
	 * I haven't found a way to create a topic and then immediately be able to produce on it without
	 * crashing a consumer first.  This is horrible.
	 * 
	 * @param topic
	 */
	public synchronized static void createTopic(String topic, Optional<Map<String, Object>> options, final ZkUtils zk_client1) {
		
		//TODO (ALEPH-10): need to handle topics getting deleted but not being removed from this map
		//TODO (ALEPH-10): override options if they change? not sure if that's possible 
		
		if ( !my_topics.containsKey(topic) ) {
			logger.debug("CREATE TOPIC");
			//http://stackoverflow.com/questions/27036923/how-to-create-a-topic-in-kafka-through-java
			
			// For some reason need to create a new zk_client in here for the createTopic call, otherwise the partitions aren't set correctly?!
			final ZkUtils zk_client = getNewZkClient();
			
			try { if ( !AdminUtils.topicExists(zk_client, topic) ) {		
				final Properties props = options.map(o -> { 
						final Properties p = new Properties();
						p.putAll(o);
						return p;
				})
				.orElse(new Properties());				
				AdminUtils.createTopic(zk_client, topic, 1, 1, props);			
				boolean leader_elected = waitUntilLeaderElected(zk_client, topic, 1000);
				logger.debug("LEADER WAS ELECTED: " + leader_elected);
				
				//create a consumer to fix offsets (this is a hack, idk why it doesn't work until we create a consumer)
				WrappedConsumerIterator iter = new WrappedConsumerIterator(getKafkaConsumer(topic, Optional.empty()), topic, 1);
				iter.hasNext();
				
				//debug info
				if (logger.isDebugEnabled()) {
					logger.error("DONE CREATING TOPIC");
					//(this was removed in 0.9):
					//logger.debug(AdminUtils.fetchTopicConfig(zk_client, topic).toString());
					TopicMetadata meta = AdminUtils.fetchTopicMetadataFromZk(topic, zk_client);
					logger.error("META: " + meta);
				}
				
				// (close resources)
				iter.close();
			}}
			finally {
				zk_client.close();				
			}
			my_topics.put(topic, true); //topic either already existed or was created
		}
	}

	/**
	 * Continually checks zookeeper for a leader on the given topic partition.  If
	 * a leader is found, returns true, otherwise spins until timeout_ms and returns
	 * false.
	 * 
	 * @param zk_client
	 * @param topic
	 * @param timeout_ms
	 * @return
	 */
	private static boolean waitUntilLeaderElected(ZkUtils zk_client, String topic, long timeout_ms) {
		long timeout_time_ms = System.currentTimeMillis() + timeout_ms;
		Option<Object> leader = zk_client.getLeaderForPartition(topic, 0);
		while ( System.currentTimeMillis() < timeout_time_ms && leader.isEmpty() ) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {				
				e.printStackTrace();
				return false;
			}
		}
		if ( System.currentTimeMillis() > timeout_time_ms ) {
			logger.debug("TIMED OUT BEFORE LEADER ELECTION");
			return false;
		}
		return true;
	}

	/**
	 * Deletes a topic, removes the topics entry from zookeeper
	 * good cleanup for test cases.
	 * 
	 * @param topic
	 */
	public static void deleteTopic(String topic, final ZkUtils zk_client) {
		// Update local cache - remote caches will need to wait to clear of course
		known_topics.invalidate(topic);
		my_topics.remove(topic);
		
		logger.debug("DELETE TOPIC: " + topic);
		AdminUtils.deleteTopic(zk_client, topic);
	}
}
