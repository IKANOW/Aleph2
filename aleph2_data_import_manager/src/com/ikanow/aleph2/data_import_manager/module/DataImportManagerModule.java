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


import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import com.ikanow.aleph2.data_import_manager.services.DataImportManager;
import com.ikanow.aleph2.data_model.interfaces.data_access.samples.SampleCustomServiceOne;
import com.ikanow.aleph2.data_model.interfaces.data_access.samples.SampleServiceContextService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICoreDistributedServices;
import com.ikanow.aleph2.data_model.module.DefaultModule;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class DataImportManagerModule implements Module{
	private static Logger logger = LogManager.getLogger();	

	public void configure(Binder binder) {
		try {			
			// TODO rename differently than  default
			Config config = ConfigFactory.defaultApplication();					
			ModuleUtils.loadModulesFromConfig(config);
		    //ICoreDistributedServices core_distributed_services = ModuleUtils.getService(ICoreDistributedServices.class, Optional.empty());

		    binder.bind(DataImportManager.class).in(Scopes.SINGLETON);	

			
		} catch (Exception e) {
			logger.error(e);
		}			
	}
	


}
