package com.ikanow.aleph2.data_import_manager.batch_enrichment.services.mapreduce;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.fs.FileContext;
import org.junit.Before;

import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.ikanow.aleph2.data_import_manager.batch_enrichment.actors.BeBucketActor;
import com.ikanow.aleph2.data_import_manager.batch_enrichment.module.DataImportManagerModule;
import com.ikanow.aleph2.data_import_manager.batch_enrichment.services.DataImportManager;
import com.ikanow.aleph2.data_import_manager.batch_enrichment.utils.DataBucketTest;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;

public class BeJobServiceTest extends DataBucketTest {

	protected MiniClusterBeJobLauncher beJobService;

	@Before
	public void setupDependencies() throws Exception {
		super.setupDependencies();
		Injector serverInjector = ModuleUtils.createInjector(Arrays.asList(new DataImportManagerModule(){

			@Override
			protected void configureServices() {
			    bind(DataImportManager.class).in(Scopes.SINGLETON);
			    bind(IBeJobService.class).to(MiniClusterBeJobLauncher.class).in(Scopes.SINGLETON);
			}
			
		}), Optional.of(config));

		this.beJobService = (MiniClusterBeJobLauncher)serverInjector.getInstance(IBeJobService.class);		
		
	} // setup dependencies
	
	public void testBeJobService(){
		try {
			beJobService.start();
			createEnhancementBeanInDb();
			
			BeBucketActor.launchReadyJobs(fileContext, buckeFullName1, bucketPath1, beJobService, _management_db, null);
			beJobService.stop();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
