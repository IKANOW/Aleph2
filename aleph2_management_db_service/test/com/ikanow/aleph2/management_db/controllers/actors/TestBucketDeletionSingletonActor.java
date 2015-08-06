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
package com.ikanow.aleph2.management_db.controllers.actors;

import java.util.Optional;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.ikanow.aleph2.data_model.interfaces.shared_services.MockServiceContext;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.distributed_services.services.MockCoreDistributedServices;
import com.ikanow.aleph2.management_db.services.ManagementDbActorContext;

public class TestBucketDeletionSingletonActor {

	@Before
	public void testSetup() throws Exception {
		MockServiceContext mock_service_context = new MockServiceContext();
		MockCoreDistributedServices mock_core_distributed_services = new MockCoreDistributedServices();
		mock_service_context.addService(ICoreDistributedServices.class, Optional.empty(), mock_core_distributed_services);
		
		//TODO: need the core management db and the underlying management db to test this
		
		@SuppressWarnings("unused")
		ManagementDbActorContext singleton = new ManagementDbActorContext(mock_service_context);		
	}

	@Ignore
	@Test
	public void test_bucketDeletionSingletonActor() {
		
		//TODO
		
		try { Thread.sleep(120000L); } catch (Exception e) {};
	}
	
}
