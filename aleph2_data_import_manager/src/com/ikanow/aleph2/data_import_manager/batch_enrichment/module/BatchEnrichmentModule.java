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
import com.ikanow.aleph2.data_import_manager.batch_enrichment.services.mapreduce.BeJobLauncher;
import com.ikanow.aleph2.data_import_manager.batch_enrichment.services.mapreduce.BeJobLoader;

public class BatchEnrichmentModule extends AbstractModule { 
	@SuppressWarnings("unused")
	private static Logger logger = LogManager.getLogger();	


	@Override
	protected void configure() {
	    configureServices();
	}
	
	protected void configureServices(){		
	    bind(BeJobLoader.class);
	    bind(BeJobLauncher.class);
//	    bind(IBeJobService.class).to(BeJobLauncher.class).in(Scopes.SINGLETON);		
	}
}
