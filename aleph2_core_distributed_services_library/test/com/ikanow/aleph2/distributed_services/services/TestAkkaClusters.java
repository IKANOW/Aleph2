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
package com.ikanow.aleph2.distributed_services.services;

import static org.junit.Assert.*;

import java.util.Date;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.junit.Before;
import org.junit.Test;

import scala.concurrent.duration.Duration;
import akka.cluster.Cluster;
import akka.cluster.seed.ZookeeperClusterSeed;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.distributed_services.data_model.DistributedServicesPropertyBean;
import com.ikanow.aleph2.distributed_services.utils.ZookeeperUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class TestAkkaClusters {
	public static final Logger _logger = LogManager.getLogger();
	
	///////////////////////////////////
	///////////////////////////////////
	
	// ALEPH-2 STATE
	
	protected MockCoreDistributedServices _mock_core_distributed_services;	
	protected CoreDistributedServices _core_distributed_services;
	protected String _connect_string;
	
	///////////////////////////////////
	///////////////////////////////////
	
	// SETUP	

	@Before
	public void baseSetup() throws Exception {
		MockCoreDistributedServices temp = new MockCoreDistributedServices();	
		_mock_core_distributed_services = temp;
	}
	
	public void setup(final String application_name, final String test_name) throws Exception {
		_connect_string = _mock_core_distributed_services._test_server.getConnectString();
				
		HashMap<String, Object> config_map = new HashMap<String, Object>();
		config_map.put(DistributedServicesPropertyBean.ZOOKEEPER_CONNECTION, _connect_string);
		if (null != application_name) config_map.put(DistributedServicesPropertyBean.APPLICATION_NAME, application_name);
		
		Config config = ConfigFactory.parseMap(config_map);				
		DistributedServicesPropertyBean bean =
				BeanTemplateUtils.clone(
						BeanTemplateUtils.from(config.getConfig(DistributedServicesPropertyBean.PROPERTIES_ROOT), DistributedServicesPropertyBean.class))
				.with("cluster_name", test_name)
				.with("application_port", 
						ImmutableMap.builder()
							.put("test_application_4011", 4011)
							.put("test_application_4129", 4129)
							.put("test_application_4273", 4273)
						.build())
				.done();
		
		assertEquals(_connect_string, bean.zookeeper_connection());
		
		_core_distributed_services = new CoreDistributedServices(bean);		
	}
	
	///////////////////////////////////
	///////////////////////////////////
	
	// TEST
	
	protected boolean doesLockNodeExist() throws Exception {
		final String application_name = _core_distributed_services._config_bean.application_name();
		final String hostname_application = DistributedServicesPropertyBean.ZOOKEEPER_APPLICATION_LOCK + "/" + ZookeeperUtils.getHostname() + ":" + application_name;
		return Optional.ofNullable(_core_distributed_services.getCuratorFramework().checkExists().forPath(hostname_application)).map(__  -> true).orElse(false);
	}		
	
	@Test
	public void test_setupComplete() throws Exception {
		// (try a few in case something's camped on one of the ports)
		try {
			setup("test_application_4011", "doesLockNodeExist1");
		}
		catch (Exception ee) {
			try {
				setup("test_application_4129", "doesLockNodeExist2");
			}
			catch (Exception eee) {
				setup("test_application_4273", "doesLockNodeExist3");
			}
			
		}
		
		assertTrue("lock node built", doesLockNodeExist());				

		long now = new Date().getTime();
		assertTrue("Akka joins - test coverage", _core_distributed_services.waitForAkkaJoin(Optional.empty()));
		
		long now2 = new Date().getTime();
		assertTrue("Didn't take too long to join: " + (now2 - now), now2 - now < 30000L);

		assertTrue("Was one of the allowed ports: " + ZookeeperClusterSeed.get(_core_distributed_services.getAkkaSystem()).address().port().get().toString(),
				ImmutableSet.builder().add("4011", "4129", "4273").build().contains(
						ZookeeperClusterSeed.get(_core_distributed_services.getAkkaSystem()).address().port().get().toString()));		
		
		_core_distributed_services._shutdown_hook.get().run();
		
		for (int i = 0; i < 10; ++i) {			
			Thread.sleep(1000L);
			if (Cluster.get(_core_distributed_services.getAkkaSystem()).isTerminated()) { 
				break;
			}
		}
		assertTrue("Left cluster", Cluster.get(_core_distributed_services.getAkkaSystem()).isTerminated());
		assertFalse("_joined_akka_cluster reset", _core_distributed_services._joined_akka_cluster.isDone());
		
		// (Currently we're leaving the lock in... so reflect that in the test)
		//assertFalse("No lock node", doesLockNodeExist());				
		assertTrue("Had to leave lock node alone", doesLockNodeExist());				
	}
	
	@Test
	public void test_waitForLock() throws Exception {
		final String app_name = "test_application";
		
		String connection_string = _mock_core_distributed_services._test_server.getConnectString();
		final RetryPolicy retry_policy = new ExponentialBackoffRetry(1000, 3);
		CuratorFramework _curator_framework = CuratorFrameworkFactory.newClient(connection_string, retry_policy);
		_curator_framework.start();		
		final String hostname_application = DistributedServicesPropertyBean.ZOOKEEPER_APPLICATION_LOCK + "/" + ZookeeperUtils.getHostname() + ":" + app_name;
		_curator_framework.create()
				.creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(hostname_application);		
		
		try {
			setup(app_name, "test_application");
			fail("Should have thrown");
		}
		catch (Exception e) {}		
	}
	
	@Test
	public void test_waitForAkkaJoin() throws Exception {
		setup(null, "test_waitForAkkaJoin");
		
		// While I'm here, quickly check that no ZK node has been generated
		assertFalse("No lock node", doesLockNodeExist());				
		
		// OK back to the main testing
		
		long now = new Date().getTime();
		assertTrue("Akka joins - test coverage", _core_distributed_services.waitForAkkaJoin(Optional.empty()));
		
		long now2 = new Date().getTime();
		assertTrue("Didn't take too long to join: " + (now2 - now), now2 - now < 30000L);

		_core_distributed_services._shutdown_hook.get().run();
		
		for (int i = 0; i < 10; ++i) {			
			Thread.sleep(1000L);
			if (Cluster.get(_core_distributed_services.getAkkaSystem()).isTerminated()) { 
				break;
			}
		}
		assertTrue("Left cluster", Cluster.get(_core_distributed_services.getAkkaSystem()).isTerminated());
		assertFalse("_joined_akka_cluster reset", _core_distributed_services._joined_akka_cluster.isDone());
		
		// OK now join again so I can do some tests
		_core_distributed_services.setAkkaJoinTimeout(Duration.create(1, TimeUnit.MICROSECONDS));
		try {
			_core_distributed_services.waitForAkkaJoin(Optional.empty());
			fail("Should have thrown exception");
		}
		catch (Exception e) {}
		assertEquals("Should timeout again, returning false this time", false, _core_distributed_services.waitForAkkaJoin(Optional.of(Duration.create(1, TimeUnit.MICROSECONDS))));
	}
}
