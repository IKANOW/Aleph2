package com.ikanow.aleph2.data_import_manager.batch_enrichment.services.mapreduce;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.ikanow.aleph2.data_import_manager.batch_enrichment.actors.BeBucketActor;
import com.ikanow.aleph2.data_import_manager.batch_enrichment.module.DataImportManagerModule;
import com.ikanow.aleph2.data_import_manager.batch_enrichment.services.DataImportManager;
import com.ikanow.aleph2.data_import_manager.batch_enrichment.utils.DataBucketTest;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;

public class BeJobServiceTest extends DataBucketTest {
    private static final Logger logger = LogManager.getLogger(BeJobServiceTest.class);

	protected IBeJobService beJobService;

	protected static boolean useMiniCluster = false;
	
	@Before
	public void setupDependencies() throws Exception {
		super.setupDependencies();
		Injector serverInjector = ModuleUtils.createInjector(Arrays.asList(new DataImportManagerModule(){

			@Override
			protected void configureServices() {
			    bind(DataImportManager.class).in(Scopes.SINGLETON);
			    bind(IBeJobService.class).to(useMiniCluster?MiniClusterBeJobLauncher.class:LocalBeJobLauncher.class).in(Scopes.SINGLETON);
			}
			
		}), Optional.of(config));

		this.beJobService = serverInjector.getInstance(IBeJobService.class);		
		
	} // setup dependencies
	
	@Test
	public void testBeJobService(){
		try {
			
			createEnhancementBeanInDb();
			
			BeBucketActor.launchReadyJobs(fileContext, buckeFullName1, bucketPath1, beJobService, _management_db, null);
			if(beJobService instanceof MiniClusterBeJobLauncher){
				((MiniClusterBeJobLauncher)beJobService).stop();
			}
		} catch (Exception e) {
			logger.error("testBeJobService caught exception");
			fail(e.getMessage());
		}
	}
}
