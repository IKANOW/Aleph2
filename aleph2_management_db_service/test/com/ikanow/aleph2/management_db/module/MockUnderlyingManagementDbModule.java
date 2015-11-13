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
package com.ikanow.aleph2.management_db.module;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Scopes;

/**
 * @author acp
 *
 */
public class MockUnderlyingManagementDbModule extends AbstractModule {

	public static interface IMockUnderlyingCrudServiceFactory {
		
	}
	public static class MockUnderlyingCrudServiceFactory implements IMockUnderlyingCrudServiceFactory {
		
	}
	
	public MockUnderlyingManagementDbModule() {}
	
	protected IMockUnderlyingCrudServiceFactory _crud_service_factory;
	
	/** This isn't called unless this module is at the top level 
	 * @param crud_service_factory
	 */
	@Inject
	public MockUnderlyingManagementDbModule(
			IMockUnderlyingCrudServiceFactory crud_service_factory
			)
	{
		_crud_service_factory = crud_service_factory;
		//DEBUG
		//System.out.println("Hello world from: " + this.getClass() + ": underlying=" + crud_service_factory);
	}
	
	public void configure() {
		this.bind(IMockUnderlyingCrudServiceFactory.class).to(MockUnderlyingCrudServiceFactory.class).in(Scopes.SINGLETON);
		this.bind(MockUnderlyingCrudServiceFactory.class);
	}
	
}
