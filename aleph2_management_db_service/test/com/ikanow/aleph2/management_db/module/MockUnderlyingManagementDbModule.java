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

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Scopes;

public class MockUnderlyingManagementDbModule extends AbstractModule {

	public static class MockUnderlyingCrudServiceFactory {
		
	}
	
	public MockUnderlyingManagementDbModule() {}
	
	protected MockUnderlyingCrudServiceFactory _crud_service_factory;
	
	@Inject
	public MockUnderlyingManagementDbModule(
			MockUnderlyingCrudServiceFactory crud_service_factory
			)
	{
		_crud_service_factory = crud_service_factory;
		/**/
		System.out.println("Hello world from: " + this.getClass() + ": underlying=" + crud_service_factory);
	}
	
	public void configure() {
		
		this.bind(MockUnderlyingCrudServiceFactory.class).in(Scopes.SINGLETON);
	}
	
}
