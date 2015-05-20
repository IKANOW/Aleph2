package com.ikanow.aleph2.data_import_manager.batch_enrichment.actors;

import org.junit.Test;

import com.google.inject.Injector;
import com.ikanow.aleph2.data_import_manager.batch_enrichment.module.DataImportManagerMain;

public class FolderWatchActorTest {
	
	@Test
	public void testTraverseFolders(){
		Injector i = DataImportManagerMain.createInjector();	
		FolderWatcherActor fwa = new FolderWatcherActor();
		fwa.traverseFolders();
	}

}
