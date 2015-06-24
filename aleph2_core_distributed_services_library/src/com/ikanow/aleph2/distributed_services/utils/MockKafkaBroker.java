package com.ikanow.aleph2.distributed_services.utils;

import java.io.File;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.Time;

public class MockKafkaBroker {
	public KafkaServer kafka_server;
	private static final Logger logger = LogManager.getLogger();
	
	public MockKafkaBroker(String zookeeper_connection) {
		Properties props = new Properties();
		props.put("port", "6661");
		props.put("broker.id", "0");
		props.put("log.dir", System.getProperty("java.io.tmpdir") + File.pathSeparator + "kafka_local_temp");
		System.out.println("log dir is: " + props.getProperty("log.dir"));
		String zk = zookeeper_connection;
		zk = zk.replaceAll("127\\.0\\.0\\.1", "localhost");
		System.out.println("ZOOKEEPER: " + zk);
		props.put("zookeeper.connect", zk);
		props.put("host.name", "localhost");
		KafkaConfig config = new KafkaConfig(props);
		//NOTE: scala version won't work here for some reason, copied same implementation as {@link kafka.utils.SystemTime}
		kafka_server = new KafkaServer(config, new Time() {
			
			@Override
			public void sleep(long arg0) {
				try {
					Thread.sleep(arg0);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
			@Override
			public long nanoseconds() {
				return System.nanoTime();
			}
			
			@Override
			public long milliseconds() {
				return System.currentTimeMillis();
			}
		});
		kafka_server.startup();
		logger.debug("local kafka is a go");
	}
	
	public void stop() {
		kafka_server.shutdown();
		logger.debug("local kafka is a stop");
	}
}
