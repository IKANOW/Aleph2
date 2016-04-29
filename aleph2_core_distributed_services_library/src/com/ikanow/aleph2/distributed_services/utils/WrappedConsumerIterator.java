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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Wrapper around a kafka consumer iterator for ease of use, returns Strings
 * on a next() call.  This can be converted to return JsonNode if we need to instead.
 * 
 * @author Burch
 *
 */
public class WrappedConsumerIterator implements Closeable, Iterator<String> {

	final protected KafkaConsumer<String,String> consumer;
	final protected String topic;	
	final protected long force_timeout_ms;
//	final private static Logger logger = LogManager.getLogger();
	private static final long DEFAULT_TIMEOUT_MS = 2000;
	protected List<ConsumerRecord<String, String>> curr_record_set = new ArrayList<ConsumerRecord<String,String>>();
	
	/**
	 * Takes a consumer and the topic name, retrieves the stream of results and
	 * creates an iterator for it, can use calling hasNext() and next() then.
	 * 
	 * @param consumer
	 * @param topic
	 */
	public WrappedConsumerIterator(final KafkaConsumer<String,String> consumer, final String topic ) {
		this(consumer, topic, DEFAULT_TIMEOUT_MS);
	}
	
	public WrappedConsumerIterator(final KafkaConsumer<String,String> consumer, final String topic, final long force_timeout_ms) {
		this.consumer = consumer;
		this.topic = topic;
		this.force_timeout_ms = force_timeout_ms;
		getNextRecordSet(0); //initialize by forcing a poll call to occur (ugh)        
	}
	
	/**
	 * Returns the next string in the queue, should call hasNext() before this everytime.
	 * 
	 */
	@Override
	public String next() {
		if ( curr_record_set.isEmpty())
			return null;
		return curr_record_set.remove(0).value();
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
		//if we have records left in buffer, just bail out quickly
		if ( !curr_record_set.isEmpty() )
			return true;
		
		//otherwise try to get more records
		getNextRecordSet(force_timeout_ms);
		return curr_record_set.size() > 0;
	}
	
	private void getNextRecordSet(long timeout_ms) {
		ConsumerRecords<String, String> records = consumer.poll(timeout_ms);
		for ( ConsumerRecord<String, String> record : records) {
			curr_record_set.add(record);
		}
		consumer.commitSync();
	}
	
	/**
	 * Shuts down the consumer as a cleanup step.
	 * 
	 */
	@Override
	public void close() {
		System.out.println("Consumer for topic: " + topic + " was told to close");
		if ( consumer != null )
			consumer.close();
	}

}
