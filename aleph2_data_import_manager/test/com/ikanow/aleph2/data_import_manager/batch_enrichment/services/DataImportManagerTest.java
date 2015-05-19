package com.ikanow.aleph2.data_import_manager.batch_enrichment.services;

import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.Optional;

import org.junit.Test;

import com.google.inject.Injector;
import com.ikanow.aleph2.data_import_manager.batch_enrichment.module.DataImportManagerModule;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;

public class DataImportManagerTest {

	
	@Test
	public void test() throws Exception {
		Injector serverInjector = ModuleUtils.createInjector(Arrays.asList(new DataImportManagerModule()), Optional.empty());
		DataImportManager dataImportManager = serverInjector.getInstance(DataImportManager.class);
		assertNotNull(dataImportManager);
	}
}
