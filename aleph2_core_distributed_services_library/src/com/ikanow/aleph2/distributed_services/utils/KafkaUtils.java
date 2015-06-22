package com.ikanow.aleph2.distributed_services.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

public class KafkaUtils {
	private static final Integer NUM_THREADS = 1;
	//TODO was switching producers to grab from a global cache, need to use a lock
	private static final Map<String, Producer> producer_map = new HashMap<String, Producer>();
	
	public static <T1, T2> Producer<T1, T2> getKafkaProducer() {
		ProducerConfig config = new ProducerConfig(getKafkaProperties());
        Producer<T1, T2> producer = new Producer<T1, T2>(config);
        return producer;
	}
	
	public static ConsumerConnector getKafkaConsumer(String topic) {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, NUM_THREADS);
		ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(getKafkaProperties()));
		return consumer;
	}
	
	private static Properties getKafkaProperties() {
		Properties props = new Properties();
		//PRODUCER PROPERTIES
        props.put("metadata.broker.list", "api001.dev.ikanow.com:6667");
        //props.put("zk.connect", "127.0.0.1:2181");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //props.put("partitioner.class", "kafka_test.SimplePartitioner");
        props.put("request.required.acks", "1");
        
        //CONSUMER PROPERTIES
        props.put("zookeeper.connect", "api001.dev.ikanow.com:2181");
        props.put("group.id", "somegroup");
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        return props;
	}
}
