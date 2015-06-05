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

import java.io.File;
import java.util.Arrays;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_import_manager.batch_enrichment.services.DataImportManager;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class DataImportManagerMain {

	private static Logger logger = LogManager.getLogger();	
	private static Injector serverInjector = null;
	
	public static Injector createInjector(final String config_path) {
		

		Injector injector = null;
		try {
			Config config = ConfigFactory.parseFile(new File(config_path));
			injector = ModuleUtils.createInjector(Arrays.asList(new DataImportManagerModule()), Optional.of(config));
		} catch (Exception e) {
			logger.error("Error creating injector from DataImportManagerModule:",e);	
		}
		return injector;
	}
	
	public static void start(){
		DataImportManager dataImportManager =  serverInjector.getInstance(DataImportManager.class);		
		dataImportManager.start();		
	}


	public static void stop(){
		DataImportManager dataImportManager =  serverInjector.getInstance(DataImportManager.class);		
		dataImportManager.stop();		
	}

	public static void main(String[] args) {
		if (0 == args.length) {
			System.out.println("First arg must be config file");
			System.exit(-1); 
		}
		serverInjector = createInjector(args[0]);
		if(args.length>0){
			if("stop".equalsIgnoreCase(args[1])){
				Injector serverInjector = Guice.createInjector(new DataImportManagerModule());		
				DataImportManager dataImportManager =  serverInjector.getInstance(DataImportManager.class);		
				dataImportManager.stop();		
				
			}
			//setConfig(args[0]);
			// set config if it is ot already set
		}
		start();
	}

}
