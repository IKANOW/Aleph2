package com.ikanow.aleph2.data_import_manager.services;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Optional;

import org.junit.Assert;
import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_import_manager.module.DataImportManagerModule;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.typesafe.config.ConfigFactory;

public class DataImportManagerTest {

	//private static Injector serverInjector = Guice.createInjector(new DataImportManagerModule());		

//	@Test
//	public void testDataImportManagerCreation(){
//		DataImportManager dataImportManager =  serverInjector.getInstance(DataImportManager.class);		
//
//	}
	
	@Test
	public void test() throws Exception {
		Injector serverInjector = ModuleUtils.createInjector(Arrays.asList(new DataImportManagerModule()), Optional.empty());
		DataImportManager dataImportManager = serverInjector.getInstance(DataImportManager.class);
		assertNotNull(dataImportManager);
	}
}
