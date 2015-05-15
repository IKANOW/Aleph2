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
package com.ikanow.aleph2.management_db.services;

import static org.junit.Assert.*;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.junit.Before;
import org.junit.Test;

import com.ikanow.aleph2.data_model.interfaces.shared_services.ICoreDistributedServices;

public class TestCoreDistributedServices {

	protected ICoreDistributedServices _core_distributed_services;
	
	@SuppressWarnings("unused")
	@Before
	public void setupMockCoreDistributedServices() throws Exception {
		//TODO: set up config from here:
		MockCoreDistributedServices temp = new MockCoreDistributedServices();		
		String connect_string = temp._test_server.getConnectString();		
		_core_distributed_services = new CoreDistributedServices();
	}
	
	@Test
	public void testMockCoreDistributedServices() throws KeeperException, InterruptedException, Exception {
    	//TODO: once done		
		final CuratorFramework curator = _core_distributed_services.getCuratorFramework();
        String path = curator.getZookeeperClient().getZooKeeper().create("/test", new byte[]{1,2,3}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        assertEquals(path, "/test");
	}
}
