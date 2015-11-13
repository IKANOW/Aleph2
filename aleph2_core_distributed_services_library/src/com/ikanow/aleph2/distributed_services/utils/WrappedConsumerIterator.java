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

import java.io.Closeable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

/**
 * Wrapper around a kafka consumer iterator for ease of use, returns Strings
 * on a next() call.  This can be converted to return JsonNode if we need to instead.
 * 
 * @author Burch
 *
 */
public class WrappedConsumerIterator implements Closeable, Iterator<String> {

	final protected ConsumerConnector consumer;
	final protected String topic;	
	final protected Iterator<MessageAndMetadata<byte[], byte[]>> iterator;
	final private static Logger logger = LogManager.getLogger();
	
	/**
	 * Takes a consumer and the topic name, retrieves the stream of results and
	 * creates an iterator for it, can use calling hasNext() and next() then.
	 * 
	 * @param consumer
	 * @param topic
	 */
	public WrappedConsumerIterator(ConsumerConnector consumer, String topic) {
		this.consumer = consumer;
		this.topic = topic;
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1);
        final Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        final List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
        final KafkaStream<byte[], byte[]> stream = streams.get(0);
        this.iterator = stream.iterator();       
	}
	
	/**
	 * Returns the next string in the queue, should call hasNext() before this everytime.
	 * 
	 */
	@Override
	public String next() {
		try {
			MessageAndMetadata<byte[], byte[]> next_message = iterator.next();
			if ( next_message != null )
				return new String(next_message.message());
			return null;
		} catch (Exception e) {
			close();
			return null;
		}
	}
	
	/**
	 * Checks if there is an item available in the queue, if consumer.timeout.ms has
	 * been set (we do it in KafkaUtils currently) then this will throw an exception after
	 * that timeout and return false, otherwise it will block forever until a new item is found,
	 * it never returns false from the internal iterator, we do on an exception (timeout)
	 * 
	 */
	@Override
	public boolean hasNext() {
		try {
			return iterator.hasNext();
		} catch (Exception e) {
			logger.debug("Topic iterator exceptioned (typically because no item was found in timeout period), this is set in KafkaUtils via consumer.timeout.ms");
			close();
			return false;
		}		
	}
	
	/**
	 * Shuts down the consumer as a cleanup step.
	 * 
	 */
	@Override
	public void close() {
		if ( consumer != null )
			consumer.shutdown();
	}

}
