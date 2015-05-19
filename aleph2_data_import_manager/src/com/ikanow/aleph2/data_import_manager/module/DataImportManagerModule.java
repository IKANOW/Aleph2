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
package com.ikanow.aleph2.data_import_manager.module;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.ikanow.aleph2.data_import_manager.services.DataImportManager;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICoreDistributedServices;
import com.ikanow.aleph2.management_db.services.CoreDistributedServices;
import com.ikanow.aleph2.storage_service_hdfs.services.HDFSStorageService;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class DataImportManagerModule extends AbstractModule { 
	private static Logger logger = LogManager.getLogger();	

//	public void configure(Binder binder) {
//		try {			
//			// TODO rename differently than  default
//			Config config = ConfigFactory.defaultApplication();					
//			//ModuleUtils.loadModulesFromConfig(config);
//		   // ICoreDistributedServices core_distributed_services = ModuleUtils.getService(ICoreDistributedServices.class, Optional.empty());
//		    //binder.bind(ICoreDistributedServices.class).toInstance(core_distributed_services);
//		    //IStorageService storageService = ModuleUtils.getService(IStorageService.class, Optional.empty());
//		    //binder.bind(ICoreDistributedServices.class).toInstance(core_distributed_services);
//			binder.bind(ICoreDistributedServices.class).to(CoreDistributedServices.class);
//			binder.bind(IStorageService.class).to(HDFSStorageService.class);
//		    binder.bind(DataImportManager.class).in(Scopes.SINGLETON);	
//
//			
//		} catch (Exception e) {
//			logger.error(e);
//		}			
//	}

	@Override
	protected void configure() {
		bind(ICoreDistributedServices.class).to(CoreDistributedServices.class);
		bind(IStorageService.class).to(HDFSStorageService.class);
	    bind(DataImportManager.class).in(Scopes.SINGLETON);	
	}
	


}
