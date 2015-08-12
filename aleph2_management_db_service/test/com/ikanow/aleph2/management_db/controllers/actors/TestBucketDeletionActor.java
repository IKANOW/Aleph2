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

import java.io.File;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.distributed_services.services.MockCoreDistributedServices;
import com.ikanow.aleph2.management_db.services.ManagementDbActorContext;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

//TODO running this + singleton test causes problems because you end up with multiple singleton actors, not sure best way round it
// (maybe have a shutdown for ManagementDbActorContext?)

@SuppressWarnings("unused")
public class TestBucketDeletionActor {

	@Inject 
	protected IServiceContext _service_context = null;	
	
	protected ICoreDistributedServices _cds = null;
	protected IManagementDbService _core_mgmt_db = null;
	protected ManagementDbActorContext _actor_context = null;

//	@Before
//	public void testSetup() throws Exception {
//		
//		if (null != _service_context) {
//			return;
//		}
//		final String temp_dir = System.getProperty("java.io.tmpdir") + File.separator;
//		
//		// OK we're going to use guice, it was too painful doing this by hand...				
//		Config config = ConfigFactory.parseReader(new InputStreamReader(this.getClass().getResourceAsStream("actor_test.properties")))
//							.withValue("globals.local_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
//							.withValue("globals.local_cached_jar_dir", ConfigValueFactory.fromAnyRef(temp_dir))
//							.withValue("globals.distributed_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
//							.withValue("globals.local_yarn_config_dir", ConfigValueFactory.fromAnyRef(temp_dir));
//		
//		Injector app_injector = ModuleUtils.createInjector(Arrays.asList(), Optional.of(config));	
//		app_injector.injectMembers(this);
//		
//		_cds = _service_context.getService(ICoreDistributedServices.class, Optional.empty()).get();
//		MockCoreDistributedServices mcds = (MockCoreDistributedServices) _cds;
//		mcds.setApplicationName("DataImportManager");
//		
//		new ManagementDbActorContext(_service_context);		
//		_actor_context = ManagementDbActorContext.get();
//		
//		_core_mgmt_db = _service_context.getCoreManagementDbService();		
//	}	
//		
//	public void createBucketInfrastructure() {
//		//TODO create files
//	}
//	
//	@Test
//	public void test_bucketDeletionActor_immediate() {
//		//TODO
//	}
//
//	@Test
//	public void test_bucketDeletionActor_delayed() {
//		//TODO
//	}
}
