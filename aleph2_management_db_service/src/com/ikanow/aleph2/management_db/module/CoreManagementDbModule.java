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
import com.google.inject.name.Names;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.management_db.services.DataBucketCrudService;

/** Module to inject "internal" services to this module
 * @author acp
 *
 */
public class CoreManagementDbModule extends AbstractModule {

	protected String _underlying_management_db_service_name = null;
	
	/** User constructor
	 * @param underlying_management_db_service_name - the underlying management DB service (Eg com.ikanow.aleph2.management_db.mongodb.services.[Mock]MongoDbManagementDbService)
	 */
	public CoreManagementDbModule(String underlying_management_db_service_name) {
		_underlying_management_db_service_name = underlying_management_db_service_name;
	}
	
	/** Guice injector
	 * @param underlying_management_db
	 */
	@Inject
	public CoreManagementDbModule(
			@Named("management_db_service.underlying") IManagementDbService underlying_management_db
			)
	{
		//DEBUG
		//System.out.println("Hello world from: " + this.getClass() + ": underlying=" + underlying_management_db);
	}
	
	/* (non-Javadoc)
	 * @see com.google.inject.AbstractModule#configure()
	 */
	@SuppressWarnings({ "unchecked" })
	public void configure() {
		
		this.bind(DataBucketCrudService.class).in(Scopes.SINGLETON);
		try {
			this.bind(IManagementDbService.class).annotatedWith(Names.named("management_db_service.underlying"))
					.to((Class<? extends IManagementDbService>) Class.forName(_underlying_management_db_service_name))
					.in(Scopes.SINGLETON);
					;
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("No 'underlying' IManagementDbService defined: " + _underlying_management_db_service_name, e);
		}		
	}
	
}
