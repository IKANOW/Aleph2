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

import java.util.Arrays;
import java.util.Optional;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_import_manager.services.DataImportManager;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;

public class DataImportManagerMain {

	//private static Injector serverInjector = Guice.createInjector(new DataImportManagerModule());		
	private static Injector serverInjector = createInjector();// = ModuleUtils.createInjector(Arrays.asList(new DataImportManagerModule()), Optional.empty());

	private static Injector createInjector() {
		Injector injector = null;
		try {
			injector = ModuleUtils.createInjector(Arrays.asList(new DataImportManagerModule()), Optional.empty());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return injector;
	}
	
	public static void start(){
		DataImportManager dataImportManager =  serverInjector.getInstance(DataImportManager.class);		
		dataImportManager.start();		
	}

/*	public static void setConfig(String config) {
		if(config!=null){
			// set config as property for guice
			System.setProperty("configPath", config);
			// initialize properties for mongodb driver
			Globals.setIdentity(com.ikanow.infinit.e.data_model.Globals.Identity.IDENTITY_SERVICE);
			Globals.overrideConfigLocation(config);
			PropertyConfigurator.configure(Globals.getLogPropertiesLocation());
		}
	}
	*/

	public static void stop(){
		DataImportManager dataImportManager =  serverInjector.getInstance(DataImportManager.class);		
		dataImportManager.stop();		
	}

	public static void main(String[] args) {
		if(args!=null && args.length>0){
			if("stop".equalsIgnoreCase(args[1])){
				Injector serverInjector = Guice.createInjector(new DataImportManagerModule());		
				DataImportManager dataImportManager =  serverInjector.getInstance(DataImportManager.class);		
				dataImportManager.stop();		
				
			}
			//setConfig(args[0]);
			// set config if it is ot already set
		}
		start();
		stop();
	}

}
