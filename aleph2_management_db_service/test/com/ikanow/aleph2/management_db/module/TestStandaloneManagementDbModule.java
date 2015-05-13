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
package com.ikanow.aleph2.management_db.module;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.Test;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.name.Named;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.management_db.module.MockUnderlyingManagementDbModule.MockUnderlyingCrudServiceFactory;
import com.ikanow.aleph2.management_db.services.CoreManagementDbService;
import com.ikanow.aleph2.management_db.services.DataBucketCrudService;

import fj.data.Either;

public class TestStandaloneManagementDbModule {

	@Inject @Named("management_db_service_core") IManagementDbService _core_management_db_service;
	@Inject @Named("management_db_service") IManagementDbService _underlying_management_db_service;
	@Inject DataBucketCrudService _data_bucket_crud_service;
	
	@Test
	public void testStandaloneGuiceSetup() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		StandaloneManagementDbModule module = new StandaloneManagementDbModule((Either.<String[], String>left(
				Arrays.asList("com.ikanow.aleph2.management_db.module.MockUnderlyingManagementDbService", 
								"com.ikanow.aleph2.management_db.module.MockUnderlyingManagementDbModule").toArray(new String[0]))));
		
		Injector injector = module.getInjector();
		
		assertFalse("Injector should not be null", injector == null);
		
		injector.injectMembers(this);
		
		assertFalse("core_management_db_service should not be null", _core_management_db_service == null);		
		assertEquals(CoreManagementDbService.class, _core_management_db_service.getClass());
		
		assertFalse("_data_bucket_crud_service should not be null", _data_bucket_crud_service == null);		
		assertEquals(DataBucketCrudService.class, _data_bucket_crud_service.getClass());
		
		assertFalse("underlying_management_db_service should not be null", _underlying_management_db_service == null);		
		assertEquals(MockUnderlyingManagementDbService.class, _underlying_management_db_service.getClass());
		
		MockUnderlyingManagementDbService underlying_management_db_service = (MockUnderlyingManagementDbService)_underlying_management_db_service;
		
		assertFalse("underlying_management_db_service._crud_factory shold not be null", underlying_management_db_service._crud_factory == null);
		assertEquals(MockUnderlyingCrudServiceFactory.class, underlying_management_db_service._crud_factory.getClass());
	}
	
	@Test
	public void testStandaloneGuiceSetup_shouldFail() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		try {
			@SuppressWarnings("unused")
			StandaloneManagementDbModule module = new StandaloneManagementDbModule((Either.<String[], String>left(
					Arrays.asList("com.ikanow.aleph2.management_db.module.MockUnderlyingManagementDbService").toArray(new String[0]))));
			
			assertFalse("Exception was thrown should have been thrown", true);
		}
		catch (Exception e) {
			assertEquals("class com.google.inject.CreationException", e.getClass().toString());
		}
		// ie missing the module load, so won't know about CRUD factory		
	}
}
