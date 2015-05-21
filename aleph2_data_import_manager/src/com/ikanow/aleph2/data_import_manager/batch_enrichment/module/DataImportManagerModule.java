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
package com.ikanow.aleph2.data_import_manager.batch_enrichment.module;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.ikanow.aleph2.data_import_manager.batch_enrichment.services.DataImportManager;
import com.ikanow.aleph2.data_import_manager.services.DataImportManagerActorContext;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.storage_service_hdfs.services.HDFSStorageService;

public class DataImportManagerModule extends AbstractModule { 
	@SuppressWarnings("unused")
	private static Logger logger = LogManager.getLogger();	


	@Override
	protected void configure() {
	    bind(DataImportManager.class).in(Scopes.SINGLETON);
	    bind(DataImportManagerActorContext.class).in(Scopes.SINGLETON);
	    bind(IStorageService.class).to(HDFSStorageService.class).in(Scopes.SINGLETON);
	    
	}
}
