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
			logger.info("Topic iterator exceptioned (typically because no item was found in timeout period), this is set in KafkaUtils via consumer.timeout.ms");
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
