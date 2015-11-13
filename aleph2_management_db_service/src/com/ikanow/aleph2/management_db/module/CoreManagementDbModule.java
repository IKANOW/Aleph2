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
import com.google.inject.name.Named;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.management_db.services.DataBucketCrudService;
import com.ikanow.aleph2.management_db.services.DataBucketStatusCrudService;
import com.ikanow.aleph2.management_db.services.SharedLibraryCrudService;

/** Module to inject "internal" services to this module
 * @author acp
 *
 */
public class CoreManagementDbModule extends AbstractModule {

	/** User constructor when called from StandaloneModuleManagement/the app - will just load the core service
	 */
	public CoreManagementDbModule() {		
	}
	
	/** Guice injector
	 * @param underlying_management_db
	 */
	@Inject
	public CoreManagementDbModule(
			@Named("management_db_service") IManagementDbService underlying_management_db
			)
	{
		//DEBUG
		//System.out.println("Hello world from: " + this.getClass() + ": underlying=" + underlying_management_db);
	}
	
	/* (non-Javadoc)
	 * @see com.google.inject.AbstractModule#configure()
	 */
	@Override
	public void configure() {
		this.bind(DataBucketCrudService.class).in(Scopes.SINGLETON);		
		this.bind(DataBucketStatusCrudService.class).in(Scopes.SINGLETON);		
		this.bind(SharedLibraryCrudService.class).in(Scopes.SINGLETON);		
	}
	
}
