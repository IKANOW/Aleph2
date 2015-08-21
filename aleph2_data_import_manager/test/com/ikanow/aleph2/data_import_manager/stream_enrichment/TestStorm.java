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
******************************************************************************/
package com.ikanow.aleph2.data_import_manager.stream_enrichment;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.ikanow.aleph2.data_import_manager.stream_enrichment.services.IStormController;
import com.ikanow.aleph2.data_import_manager.stream_enrichment.services.LocalStormController;
import com.ikanow.aleph2.data_import_manager.stream_enrichment.storm_samples.SampleWebReaderSpout;
import com.ikanow.aleph2.data_import_manager.stream_enrichment.storm_samples.SampleWordParserBolt;
import com.ikanow.aleph2.data_import_manager.stream_enrichment.utils.StormControllerUtil;
import com.ikanow.aleph2.core.shared.utils.JarBuilderUtil;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
//import com.ikanow.aleph2.storm.samples.bolts.SampleKafkaBolt;
//import com.ikanow.aleph2.storm.samples.bolts.SampleKafkaOutputFileBolt;
//import com.ikanow.aleph2.storm.samples.bolts.SampleWordParserBolt;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.PropertiesUtils;
import com.ikanow.aleph2.distributed_services.data_model.DistributedServicesPropertyBean;
import com.ikanow.aleph2.distributed_services.services.CoreDistributedServices;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.distributed_services.services.MockCoreDistributedServices;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

@SuppressWarnings("unused")
public class TestStorm {
	private static IStormController storm_cluster;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		storm_cluster = StormControllerUtil.getLocalStormController();
	}
	
	@Before
	public void setupCoreDistributedServices() throws Exception {
		
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

//	@Test
//	public void test() throws Exception {
//		TopologyBuilder builder = new TopologyBuilder();
//		builder.setSpout("spout1", new SampleWebReaderSpout("http://lifehacker.com/the-best-board-games-for-developing-valuable-real-life-1714642211"));
//		builder.setBolt("bolt1", new SampleWordParserBolt()).shuffleGrouping("spout1");
//		
//		StormControllerUtil.submitJob(storm_cluster, "test_job", null, builder.createTopology());
//		//storm_cluster.submitJob("test_job", null, builder.createTopology());
//		
//		Thread.sleep(10000);
//		
//		assertTrue(true);
//	}
//	
//	@Ignore
//	@Test
//	public void testKafkaSpout() throws Exception {
//		final String TOPIC_NAME = "TEST_KAFKA_SPOUT";
//		final String sample_jar_file = "C:/Users/Burch/Desktop/aleph2_storm_samples-0.0.1-SNAPSHOT-jar-with-dependencies.jar";
//		//STEP 1: Run the CDS kafka test to create data in the kafka queue: TEST_KAFKA_SPOUT
//		//TODO port that code over here (need to create a CDS)
//		
//		//STEP 2: create a topology and submit it to that cluster
//		TopologyBuilder builder = new TopologyBuilder();
//		BrokerHosts hosts = new ZkHosts("api001.dev.ikanow.com:2181");
//		SpoutConfig spout_config = new SpoutConfig(hosts, TOPIC_NAME, "/" + TOPIC_NAME, "1");
//		spout_config.scheme = new SchemeAsMultiScheme(new StringScheme());
//		KafkaSpout kafka_spout = new KafkaSpout(spout_config);
//		builder.setSpout("spout1", kafka_spout );
//		builder.setBolt("bolt1", new SampleKafkaBolt()).shuffleGrouping("spout1");
//		builder.setBolt("bolt2", new SampleKafkaOutputFileBolt()).shuffleGrouping("bolt1");
//		
//		String nimbus_host = "api001.dev.ikanow.com";
//		int nimbus_thrift_port = 6627;
//		String storm_thrift_transport_plugin = "backtype.storm.security.auth.SimpleTransportPlugin";
//		IStormController storm = StormControllerUtil.getRemoteStormController(nimbus_host, nimbus_thrift_port, storm_thrift_transport_plugin);
//		List<String> jars_to_merge = new ArrayList<String>();
//		jars_to_merge.add(sample_jar_file);
//		//TODO should merge in data_model
//		StormControllerUtil.submitJob(storm, "test_kafka_spout", StormControllerUtil.buildStormTopologyJar(jars_to_merge), builder.createTopology());
//	}
	

	
//	@Test
//	public void testKafkaSpout() throws JsonParseException, JsonMappingException, IOException {
//		Map<String, Object> map = new HashMap<String, Object>();
//		map.put("globals.local_yarn_config_dir", "C:/Users/Burch/Desktop/yarn_config/");
//		Config config = ConfigFactory.parseMap(map);
//		final Config subconfig = PropertiesUtils.getSubConfig(config, GlobalPropertiesBean.PROPERTIES_ROOT).orElse(null);
//		final GlobalPropertiesBean globals = BeanTemplateUtils.from(subconfig, GlobalPropertiesBean.class);
//		System.out.println(globals.local_yarn_config_dir());
//		
//		final String TOPIC_NAME = "TEST_KAFKA_SPOUT";
//		
//		//1. create local kafka
//		//This is handled when MockCDS starts, should be fine
//		
//		//2. create local storm
//		IStormController storm_controller = StormControllerUtil.getStormControllerFromYarnConfig(globals.local_yarn_config_dir());// new LocalStormController();
//		
//		//3. create kafka queue
//		
//		//4. create storm topology using kafka spout
//		
//		//5. produce to kafka queue
//		
//		//6. observe storm output receiving produced stuff
//	}
	
//	@Test
//	public void testCaching() throws Exception {
//		TopologyBuilder builder = new TopologyBuilder();
//		builder.setSpout("spout1", new SampleWebReaderSpout("http://lifehacker.com/the-best-board-games-for-developing-valuable-real-life-1714642211"));
//		builder.setBolt("bolt1", new SampleWordParserBolt()).shuffleGrouping("spout1");
//		
//		//StormControllerUtil.submitJob(storm_cluster, "test_job", null, builder.createTopology());
//		//storm_cluster.submitJob("test_job", null, builder.createTopology());
//		
//		String nimbus_host = "api001.dev.ikanow.com";
//		int nimbus_thrift_port = 6627;
//		String storm_thrift_transport_plugin = "backtype.storm.security.auth.SimpleTransportPlugin";
//		IStormController storm_controller = StormControllerUtil.getRemoteStormController(nimbus_host, nimbus_thrift_port, storm_thrift_transport_plugin);
//		//TODO
//		//StormControllerUtil.startJob(storm_controller, bucket, context, user_lib_paths, enrichment_toplogy);
//		
//		Thread.sleep(10000);
//		
//		assertTrue(true);
//	}		
	
	@Test
	public void testCache() throws IOException, InterruptedException, ExecutionException {
		final String jar_location = System.getProperty("java.io.tmpdir");
		File file1 = createFakeZipFile(null);//File.createTempFile("recent_date_test_", null);
		Thread.sleep(1500);
		File file2 = createFakeZipFile(null);//File.createTempFile("recent_date_test_", null);
		Thread.sleep(1500);
		File file3 = createFakeZipFile(null);//File.createTempFile("recent_date_test_", null);		
		List<String> files1 = Arrays.asList(file1.getCanonicalPath(),file2.getCanonicalPath(),file3.getCanonicalPath());
		String input_jar_location = JarBuilderUtil.getHashedJarName(files1, jar_location);
		File input_jar = new File(input_jar_location);
		input_jar.delete();
		assertFalse(input_jar.exists());
		
		//first time it should create
		final CompletableFuture<String> jar_future1 = StormControllerUtil.buildOrReturnCachedStormTopologyJar(files1, jar_location);
		jar_future1.get();
		assertTrue(input_jar.exists());
		
		//second time it should cache
		long file_mod_time = getFileModifiedTime(input_jar);
		final CompletableFuture<String> jar_future2 = StormControllerUtil.buildOrReturnCachedStormTopologyJar(files1, jar_location);
		jar_future2.get();
		assertEquals(file_mod_time, getFileModifiedTime(input_jar));
		
		//third time modify a file, it should no longer cache
		Thread.sleep(1500); //sleep a ms so the modified time updates
		file1.delete();
		file1 = createFakeZipFile(file2.getCanonicalPath());
		final CompletableFuture<String> jar_future3 = StormControllerUtil.buildOrReturnCachedStormTopologyJar(files1, jar_location);
		jar_future3.get();
		assertNotEquals(file_mod_time, getFileModifiedTime(input_jar)); //original jar creation time should not match its current modified time (it should have been remade)
		
		//cleanup
		file1.delete();
		file2.delete();
		file3.delete();
		new File(input_jar_location).delete();
	}
	
	private long getFileModifiedTime(File input_jar) throws IOException {
		ZipInputStream inputZip = new ZipInputStream(new FileInputStream(input_jar));		
		ZipEntry e = inputZip.getNextEntry();	
		long time = e.getLastModifiedTime().toMillis();
		inputZip.close();
		return time;
	}

	private static File createFakeZipFile(String file_name) throws IOException {
		File file;
		if ( file_name == null )
			file = File.createTempFile("recent_date_test_", ".zip");
		else
			file = new File(file_name);
		Random r = new Random();
		ZipOutputStream outputZip = new ZipOutputStream(new FileOutputStream(file));
		ZipEntry e = new ZipEntry("some_file.tmp");		
		outputZip.putNextEntry(e);
		outputZip.write(r.nextInt());
		outputZip.close();
		return file;
	}
}
