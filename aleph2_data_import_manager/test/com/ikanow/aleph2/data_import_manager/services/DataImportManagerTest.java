package com.ikanow.aleph2.data_import_manager.services;

import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_import_manager.module.DataImportManagerModule;

public class DataImportManagerTest {

	private static Injector serverInjector = Guice.createInjector(new DataImportManagerModule());		

	@Test
	public void testDataImportManagerCreation(){
		DataImportManager dataImportManager =  serverInjector.getInstance(DataImportManager.class);		

	}
}
