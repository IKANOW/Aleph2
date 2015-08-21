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
