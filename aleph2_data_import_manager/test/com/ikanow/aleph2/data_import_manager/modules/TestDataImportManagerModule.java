package com.ikanow.aleph2.data_import_manager.modules;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class TestDataImportManagerModule {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}
	
	/**
	 * This is a passthrough to allow you to test the DIM locally.
	 * Causes eclipse to pull in test dependencies (storm-core) so we can
	 * submit storm jobs.
	 * @param args
	 */
	public static void main(String[] args) {
		DataImportManagerModule.main(args);
	}

}
