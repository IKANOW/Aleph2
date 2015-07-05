package com.ikanow.aleph2.distributed_services.utils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Option;

import com.typesafe.config.Config;

import kafka.admin.AdminUtils;
import kafka.api.TopicMetadata;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.utils.ZkUtils;
import kafka.utils.ZKStringSerializer$;

public class KafkaUtils {
	private static final Integer NUM_THREADS = 1;
	private static Producer<String, String> producer;	
	private static Properties kafka_properties = new Properties();
	private final static Logger logger = LogManager.getLogger();
	
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
        
        //reset producer so a new one will be created
        if ( producer != null )
        	producer.close();
        producer = null;
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
		logger.debug("CREATE TOPIC");
		//if you don't use the ZKStringSerializer creating topics will fail
		//http://stackoverflow.com/questions/27036923/how-to-create-a-topic-in-kafka-through-java
		ZkClient zk_client = new ZkClient(kafka_properties.getProperty("zookeeper.connect"), 10000, 10000, ZKStringSerializer$.MODULE$);				
		logger.debug("DOES TOPIC EXIST: " + AdminUtils.topicExists(zk_client, topic));
		if ( !AdminUtils.topicExists(zk_client, topic) ) {		
			AdminUtils.createTopic(zk_client, topic, 1, 1, new Properties());			
			boolean leader_elected = waitUntilLeaderElected(zk_client, topic, 1000);
			logger.debug("LEADER WAS ELECTED: " + leader_elected);
			
			//create a consumer to fix offsets (this is a hack, idk why it doesn't work until we create a consumer)
			@SuppressWarnings("resource")
			Iterator<String> iter = new WrappedConsumerIterator(getKafkaConsumer(topic), topic);
			iter.hasNext();
			
			//debug info
			logger.debug("DONE CREATING TOPIC");	
			logger.debug(AdminUtils.fetchTopicConfig(zk_client, topic).toString());
			TopicMetadata meta = AdminUtils.fetchTopicMetadataFromZk(topic, zk_client);
			logger.debug("META: " + meta);
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
