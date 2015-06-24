package com.ikanow.aleph2.distributed_services.utils;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class MockProducer<K, V> extends Producer<K, V> {
	
	public MockProducer(ProducerConfig config) {
		super(config);
	}

	public void send(KeyedMessage<K,V> message) {
		MockKafka.putOnQueue(message.topic(), (String)message.message());
	}

}
