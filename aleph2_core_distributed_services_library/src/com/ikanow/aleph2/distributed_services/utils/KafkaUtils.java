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
package com.ikanow.aleph2.distributed_services.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.I0Itec.zkclient.ZkClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Option;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import kafka.admin.AdminUtils;
import kafka.api.TopicMetadata;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
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
	private static final Integer NUM_THREADS = 1;
	private static Producer<String, String> producer;	
	private static Properties kafka_properties = new Properties();
	private final static Logger logger = LogManager.getLogger();
	private final static Map<String, Boolean> known_topics = new ConcurrentHashMap<String, Boolean>();
	
	/**
	 * Returns a producer pointed at the currently configured Kafka instance.
	 * 
	 * Producers are reused for all topics so it's okay to not close this between
	 * uses.  Not sure what will happen if its left open for a long time.
	 * 
	 * @return
	 */
	public static Producer<String, String> getKafkaProducer() {
		producer = null;
		if ( producer == null ) {
			ProducerConfig config = new ProducerConfig(kafka_properties);
			producer = new Producer<String, String>(config);
		}
        return producer;
	}
	
	/**
	 * Creates a consumer for a single topic with the currently configured Kafka instance.
	 * 
	 * This consumer should be closed once you are done reading.
	 * 
	 * @param topic
	 * @return
	 */
	public static ConsumerConnector getKafkaConsumer(String topic) {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, NUM_THREADS);
		ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(kafka_properties));
		consumer.commitOffsets();
		return consumer;
	}
	
	/** Returns the configured Zookeeper connection string, needed by some Kafka modules
	 * @return the ZK connection string
	 */
	public static String getZookeperConnectionString() {
		return (String) kafka_properties.get("zookeeper.connect");
	}
	
	/** Converts from a bucket path to a Kafka topic
	 * @param bucket_path
	 * @return
	 */
	public static String bucketPathToTopicName(final String bucket_path) {
		return bucket_path.replaceFirst("^/", "").replace("/", "-").replaceAll("[^a-zA-Z\\d._-]", "_");
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
		
		//PRODUCER PROPERTIES		
		String broker = parseMap.getString("metadata.broker.list");
		logger.debug("BROKER: " + broker);		
		kafka_properties.put("metadata.broker.list", broker);
		//kafka_properties.put("metadata.broker.list", "api001.dev.ikanow.com:6667");		
		kafka_properties.put("serializer.class", "kafka.serializer.StringEncoder");
		kafka_properties.put("request.required.acks", "1");
        
        //CONSUMER PROPERTIES		
		String zk = parseMap.getString("zookeeper.connect");
		logger.debug("ZOOKEEPER: " + zk);
		kafka_properties.put("zookeeper.connect", zk);
		//kafka_properties.put("zookeeper.connect", "api001.dev.ikanow.com:2181");		
		kafka_properties.put("group.id", "somegroup_1");
        kafka_properties.put("zookeeper.session.timeout.ms", "400");
        kafka_properties.put("zookeeper.sync.time.ms", "200");
        kafka_properties.put("auto.commit.interval.ms", "1000");
        kafka_properties.put("consumer.timeout.ms", "3000"); //throw an exception if no item found for 3s
        
        //reset producer so a new one will be created
        if ( producer != null )
        	producer.close();
        producer = null;
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
	public static void setStandardKafkaProperties(final String zk_connection, final String broker_list) {
		final Map<String, Object> config_map_kafka = ImmutableMap.<String, Object>builder()
				.put("metadata.broker.list", broker_list)
				.put("serializer.class", "kafka.serializer.StringEncoder")
				.put("request.required.acks", "1")
				.put("zookeeper.connect", zk_connection)
				.put("group.id", "somegroup")
				.put("zookeeper.session.timeout.ms", "400")
				.put("zookeeper.sync.time.ms", "200")
		        .put("auto.commit.interval.ms", "1000")			
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
	public static void createTopic(String topic) {		
		if ( !known_topics.containsKey(topic) ) {
			logger.debug("CREATE TOPIC");
			ZkClient zk_client = new ZkClient(kafka_properties.getProperty("zookeeper.connect"), 10000, 10000, ZKStringSerializer$.MODULE$);				
			logger.debug("DOES TOPIC EXIST: " + AdminUtils.topicExists(zk_client, topic));
			if ( !AdminUtils.topicExists(zk_client, topic) ) {		
				AdminUtils.createTopic(zk_client, topic, 1, 1, new Properties());			
				boolean leader_elected = waitUntilLeaderElected(zk_client, topic, 1000);
				logger.debug("LEADER WAS ELECTED: " + leader_elected);
				
				//create a consumer to fix offsets (this is a hack, idk why it doesn't work until we create a consumer)
				WrappedConsumerIterator iter = new WrappedConsumerIterator(getKafkaConsumer(topic), topic);
				iter.hasNext();
				
				//debug info
				logger.debug("DONE CREATING TOPIC");	
				logger.debug(AdminUtils.fetchTopicConfig(zk_client, topic).toString());
				TopicMetadata meta = AdminUtils.fetchTopicMetadataFromZk(topic, zk_client);
				logger.debug("META: " + meta);
				iter.close();				
			}	
			known_topics.put(topic, true); //topic either already existed or was created
			zk_client.close();
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
	private static boolean waitUntilLeaderElected(ZkClient zk_client, String topic, long timeout_ms) {
		long timeout_time_ms = System.currentTimeMillis() + timeout_ms;
		Option<Object> leader = ZkUtils.getLeaderForPartition(zk_client, topic, 0);
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
	public static void deleteTopic(String topic) {
		logger.debug("DELETE TOPIC: " + topic);
		ZkClient zk_client = new ZkClient(kafka_properties.getProperty("zookeeper.connect"), 10000, 10000, ZKStringSerializer$.MODULE$);
		AdminUtils.deleteTopic(zk_client, topic);
	}
}
