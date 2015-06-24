package com.ikanow.aleph2.distributed_services.utils;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

public class MockKafka {
	private static Map<String,Queue<String>> kafka_topic_queues = new HashMap<String,Queue<String>>();
	
	public static void putOnQueue(String topic, String message) {
		if ( !kafka_topic_queues.containsKey(topic) ) {
			kafka_topic_queues.put(topic, new LinkedList<String>());
		}
		kafka_topic_queues.get(topic).add(message);
	}
	
	public static String removeFromQueue(String topic) {
		if ( kafka_topic_queues.containsKey(topic) ) {
			if ( kafka_topic_queues.get(topic).size() > 0 )
				return kafka_topic_queues.get(topic).remove();
		}
		return null;
	}	
}
