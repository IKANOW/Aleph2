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
package com.ikanow.aleph2.distributed_services.services;

import static org.junit.Assert.*;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.junit.Before;
import org.junit.Test;

import com.ikanow.aleph2.data_model.utils.SetOnce;
import com.ikanow.aleph2.distributed_services.utils.KafkaUtils;

public class TestMockCoreDistributedServices {

	protected ICoreDistributedServices _core_distributed_services;
	
	protected SetOnce<Boolean> _test1 = new SetOnce<>();
	protected SetOnce<Boolean> _test3 = new SetOnce<>();
	protected CompletableFuture<Void> _completed1;
	protected CompletableFuture<Void> _completed2;
	protected CompletableFuture<Void> _completed3;
	
	@Before
	public void setupMockCoreDistributedServices() throws Exception {
		MockCoreDistributedServices test = new MockCoreDistributedServices();
		test.setApplicationName("test_app_name");
		_core_distributed_services = test;
		_completed1 = _core_distributed_services.runOnAkkaJoin(() -> {
			_test1.set(true);
		});
		_completed2 = _core_distributed_services.runOnAkkaJoin(() -> {
			throw new RuntimeException("test2");
		});
	}
	
	@Test
	public void testMockCoreDistributedServices() throws KeeperException, InterruptedException, Exception {		
		final CuratorFramework curator = _core_distributed_services.getCuratorFramework();		
        String path = curator.getZookeeperClient().getZooKeeper().create("/test", new byte[]{1,2,3}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        assertEquals(path, "/test");
        
        assertTrue(_core_distributed_services.waitForAkkaJoin(Optional.empty()));
        
        assertEquals("test_app_name", _core_distributed_services.getApplicationName().get());
        
        _completed1.get(20, TimeUnit.SECONDS);
        assertEquals(true, _test1.get());
        
        try {
        	_completed2.get(20, TimeUnit.SECONDS);
        }
        catch (Exception e) {
        	assertEquals(e.getCause().getMessage(), "test2");
        }        
		_completed3 = _core_distributed_services.runOnAkkaJoin(() -> {
			_test3.set(false);
		});
        _completed3.get(20, TimeUnit.SECONDS);
        assertEquals(false, _test3.get());
	}
	
	@Test
	public void test_topicNameGeneration() {		
        assertEquals(KafkaUtils.bucketPathToTopicName("/test", Optional.empty()), _core_distributed_services.generateTopicName("/test", Optional.empty()));
        assertEquals(KafkaUtils.bucketPathToTopicName("/test", Optional.empty()), _core_distributed_services.generateTopicName("/test", Optional.of("$start")));
        assertEquals(KafkaUtils.bucketPathToTopicName("/test", Optional.of("$end")), _core_distributed_services.generateTopicName("/test", Optional.of("$end")));
        assertEquals(KafkaUtils.bucketPathToTopicName("/test", Optional.of("other")), _core_distributed_services.generateTopicName("/test", Optional.of("other")));
	}
}
