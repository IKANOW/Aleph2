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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
	final protected long force_timeout_ms;
	
	/**
	 * Takes a consumer and the topic name, retrieves the stream of results and
	 * creates an iterator for it, can use calling hasNext() and next() then.
	 * 
	 * @param consumer
	 * @param topic
	 */
	public WrappedConsumerIterator(ConsumerConnector consumer, String topic) {
        this(consumer, topic, 0);
	}
	
	public WrappedConsumerIterator(ConsumerConnector consumer, String topic, long force_timeout_ms) {
		this.consumer = consumer;
		this.topic = topic;
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1);
        final Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        final List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
        final KafkaStream<byte[], byte[]> stream = streams.get(0);
        this.iterator = stream.iterator();    
        this.force_timeout_ms = force_timeout_ms;
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
	 * If force_timeout_ms is set, will only wait a max of it for hasNext to return, if set to 0 or less, will
	 * just leave it up to kafka config for when to kick out of hasNext (see consumer.timeout.ms)
	 * 
	 */
	@Override
	public boolean hasNext() {
		final ExecutorService executor = Executors.newSingleThreadExecutor();
		Future<Boolean> future = executor.submit(new Callable<Boolean>() {
			@Override
			public Boolean call() throws Exception {
				try {
					return iterator.hasNext();
				} catch (Exception e) {
					logger.debug("Topic iterator exceptioned (typically because no item was found in timeout period), this is set in KafkaUtils via consumer.timeout.ms", e);
					close();
					return false;
				}	
			}
		});
		executor.shutdown();
		if ( force_timeout_ms > 0 ) {
			try {
				executor.awaitTermination(force_timeout_ms, TimeUnit.MILLISECONDS);				
			} catch (Exception ex) {
				logger.debug("Topic iterator exceptioned (typically because no item was found in timeout period), this is set in KafkaUtils via consumer.timeout.ms", ex);
				close();
				return false;
			} finally {
				executor.shutdownNow();
			}
		}
		try {
			return future.get();
		} catch (InterruptedException | ExecutionException e) {
			logger.debug("Topic iterator exceptioned (typically because no item was found in timeout period), this is set in KafkaUtils via consumer.timeout.ms", e);
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
