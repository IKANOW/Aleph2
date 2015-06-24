package com.ikanow.aleph2.distributed_services.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.consumer.KafkaStream;
import kafka.consumer.TopicFilter;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.Decoder;


public class MockConsumer implements ConsumerConnector {

	@Override
	public <K, V> Map<String, List<KafkaStream<K, V>>> createMessageStreams(
			Map<String, Integer> topicCountMap, Decoder<K> keyDecoder,
			Decoder<V> valueDecoder) {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public Map<String, List<KafkaStream<byte[], byte[]>>> createMessageStreams(
			Map<String, Integer> topicCountMap) {
		Map<String, List<KafkaStream<byte[], byte[]>>> streams = new HashMap<String, List<KafkaStream<byte[],byte[]>>>();
		topicCountMap.keySet().stream().forEach(key -> {
			List<KafkaStream<byte[], byte[]>> stream = new ArrayList<KafkaStream<byte[],byte[]>>();
			//stream.add(new KafkaStream<byte[], byte[]>(queue, consumerTimeoutMs, keyDecoder, valueDecoder, clientId))
			streams.put(key, stream);
		});
		return streams;
	}

	@Override
	public <K, V> List<KafkaStream<K, V>> createMessageStreamsByFilter(
			TopicFilter topicFilter, int numStreams, Decoder<K> keyDecoder,
			Decoder<V> valueDecoder) {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public List<KafkaStream<byte[], byte[]>> createMessageStreamsByFilter(
			TopicFilter topicFilter, int numStreams) {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public List<KafkaStream<byte[], byte[]>> createMessageStreamsByFilter(
			TopicFilter topicFilter) {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public void commitOffsets() {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public void commitOffsets(boolean retryOnFailure) {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public void shutdown() {
		//nothing to do
	}

}
