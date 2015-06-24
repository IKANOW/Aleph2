package com.ikanow.aleph2.distributed_services.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.typesafe.config.Config;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

public class KafkaUtils {
	private static final Integer NUM_THREADS = 1;
	private static Producer<String, String> producer;	
	private static Properties kafka_properties = new Properties();
	
	public static Producer<String, String> getKafkaProducer() {
		if ( producer == null ) {
			ProducerConfig config = new ProducerConfig(kafka_properties);
			producer = new Producer<String, String>(config);
		}
        return producer;
	}
	
	public static ConsumerConnector getKafkaConsumer(String topic) {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, NUM_THREADS);
		ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(kafka_properties));
		return consumer;
	}
	
	public static void setProperties(Config parseMap) {
		kafka_properties = new Properties();
		kafka_properties.put("host.name", "localhost");
		//PRODUCER PROPERTIES
		//kafka_properties.put("metadata.broker.list", "api001.dev.ikanow.com:6667");
		kafka_properties.put("brokerid", "1");
		kafka_properties.put("broker.list", "localhost:6661");
		kafka_properties.put("metadata.broker.list", "localhost:6661");
        //props.put("zk.connect", "127.0.0.1:2181");
		kafka_properties.put("serializer.class", "kafka.serializer.StringEncoder");
        //props.put("partitioner.class", "kafka_test.SimplePartitioner");
		kafka_properties.put("request.required.acks", "1");
        
        //CONSUMER PROPERTIES
		//kafka_properties.put("zookeeper.connect", "api001.dev.ikanow.com:2181");
		String zk = parseMap.getString("zookeeper.connect");
		zk = zk.replaceAll("127\\.0\\.0\\.1", "localhost");
		System.out.println("ZOOKEEPER: " + zk);
		kafka_properties.put("zookeeper.connect", zk);
		kafka_properties.put("group.id", "somegroup");
        kafka_properties.put("zookeeper.session.timeout.ms", "400");
        kafka_properties.put("zookeeper.sync.time.ms", "200");
        kafka_properties.put("auto.commit.interval.ms", "1000");
	}
}
