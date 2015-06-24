package com.ikanow.aleph2.distributed_services.utils;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

public class WrappedConsumerIterator implements Closeable, Iterator<String> {

	final protected ConsumerConnector consumer;
	final protected String topic;	
	final protected Iterator<MessageAndMetadata<byte[], byte[]>> iterator;
	final protected ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
	
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
	
	@Override
	public String next() {
		Future<String> future = executor.submit(new Callable<String>() {
			@Override
			public String call() throws Exception {
				MessageAndMetadata<byte[], byte[]> next_message = iterator.next();
				if ( next_message != null )
					return new String(next_message.message());
				return null;
			}			
		});
		try {
			return future.get(10, TimeUnit.SECONDS);
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			System.out.println("next timed out, closing");
			close();
			return null;
		}
	}
	
	@Override
	public boolean hasNext() {
		Future<Boolean> future = executor.submit(new Callable<Boolean>() {
			@Override
			public Boolean call() throws Exception {
				System.out.println("trying to get hasNext");
				return iterator.hasNext();
			}			
		});
		try {
			return future.get(10, TimeUnit.SECONDS);
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			System.out.println("hasNext timed out, closing");
			close();
			return false;
		}
	}
	
	@Override
	public void close() {
		executor.shutdown();
		if ( consumer != null )
			consumer.shutdown();
	}

}
