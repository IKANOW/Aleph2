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

import java.util.HashMap;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.junit.Before;
import org.junit.Test;

import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.distributed_services.data_model.DistributedServicesPropertyBean;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class TestCoreDistributedServices {

	protected ICoreDistributedServices _core_distributed_services;
	
	@Before
	public void setupMockCoreDistributedServices() throws Exception {
		MockCoreDistributedServices temp = new MockCoreDistributedServices();		
		String connect_string = temp._test_server.getConnectString();
				
		HashMap<String, Object> config_map = new HashMap<String, Object>();
		config_map.put(DistributedServicesPropertyBean.ZOOKEEPER_CONNECTION, connect_string);
		
		Config config = ConfigFactory.parseMap(config_map);				
		DistributedServicesPropertyBean bean =
				BeanTemplateUtils.from(config.getConfig(DistributedServicesPropertyBean.PROPERTIES_ROOT), DistributedServicesPropertyBean.class);
		
		assertEquals(connect_string, bean.zookeeper_connection());
		
		_core_distributed_services = new CoreDistributedServices(bean);
	}
	
	@Test
	public void testMockCoreDistributedServices() throws KeeperException, InterruptedException, Exception {
		final CuratorFramework curator = _core_distributed_services.getCuratorFramework();
        String path = curator.getZookeeperClient().getZooKeeper().create("/test", new byte[]{1,2,3}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        assertEquals(path, "/test");
	}
}
