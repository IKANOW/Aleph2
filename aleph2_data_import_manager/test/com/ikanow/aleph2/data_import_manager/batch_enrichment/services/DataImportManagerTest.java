package com.ikanow.aleph2.data_import_manager.batch_enrichment.services;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.fs.FileContext;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_import_manager.batch_enrichment.actors.BeBucketActor;
import com.ikanow.aleph2.data_import_manager.batch_enrichment.actors.BucketEnrichmentMessage;
import com.ikanow.aleph2.data_import_manager.batch_enrichment.module.DataImportManagerModule;
import com.ikanow.aleph2.data_import_manager.services.DataImportActorContext;
import com.ikanow.aleph2.data_import_manager.services.GeneralInformationService;
import com.ikanow.aleph2.data_import_manager.utils.DirUtils;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.SingleQueryComponent;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.management_db.utils.ActorUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

public class DataImportManagerTest {
    private static final Logger logger = Logger.getLogger(DataImportManagerTest.class);

	@Inject 
	protected IServiceContext _service_context = null;
	
	protected DataImportActorContext _actor_context;
	//protected ManagementDbActorContext _db_actor_context;

	protected Config config = null;
	protected DataImportManager dataImportManager = null;
	
	protected String bucketPath1 = null;
	protected String bucketReadyPath1 = null;
	protected String buckeFullName1= "/misc/bucket1";
	
	protected IManagementDbService _management_db;

	@Before
	public void setupDependencies() throws Exception {
		if (null != _service_context) {
			return;
		}
		
		final String temp_dir = System.getProperty("java.io.tmpdir");
		
		// OK we're going to use guice, it was too painful doing this by hand...				
		config = ConfigFactory.parseReader(new InputStreamReader(this.getClass().getResourceAsStream("/test_data_import_manager.properties")))
							.withValue("globals.local_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
							.withValue("globals.local_cached_jar_dir", ConfigValueFactory.fromAnyRef(temp_dir))
							.withValue("globals.distributed_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
							.withValue("globals.local_yarn_config_dir", ConfigValueFactory.fromAnyRef(temp_dir));
		
		Injector app_injector = ModuleUtils.createInjector(Arrays.asList(), Optional.of(config));	
		app_injector.injectMembers(this);

		_actor_context = new DataImportActorContext(_service_context, new GeneralInformationService());
		app_injector.injectMembers(_actor_context);

		Injector serverInjector = ModuleUtils.createInjector(Arrays.asList(new DataImportManagerModule()), Optional.of(config));
		this.dataImportManager = serverInjector.getInstance(DataImportManager.class);

		createFolderStructure();
		this._management_db = _actor_context.getServiceContext().getCoreManagementDbService();
		
}
	
	protected void createFolderStructure(){
		// create folder structure if it does not exist for testing.		
		FileContext fileContext = _service_context.getStorageService().getUnderlyingPlatformDriver(FileContext.class,Optional.empty()).get();
		logger.info("Root dir:"+_actor_context.getGlobalProperties().distributed_root_dir());
		this.bucketPath1 = _actor_context.getGlobalProperties().distributed_root_dir()+"/data/misc/bucket1";
		this.bucketReadyPath1 = bucketPath1+"/managed_bucket/import/ready";
		DirUtils.createDirectory(fileContext,bucketReadyPath1);
		DirUtils.createDirectory(fileContext,_actor_context.getGlobalProperties().distributed_root_dir()+"/data/misc/bucket2/managed_bucket/import/ready");
		DirUtils.createDirectory(fileContext,_actor_context.getGlobalProperties().distributed_root_dir()+"/data/misc/bucket3/managed_bucket/import/ready");
		StringBuffer sb = new StringBuffer();
		sb.append("bucket1data\r\n");
		DirUtils.createUTF8File(fileContext,bucketReadyPath1+"/bucket1data.txt", sb);
	}
	
	@Test
	@Ignore
	public void testCreate() throws Exception {
		assertNotNull(dataImportManager);
	}

	@Test
	@Ignore
	public void testStartStop() throws Exception {
		assertNotNull(dataImportManager);
		dataImportManager.start();	
		Thread.sleep(3000);
		dataImportManager.stop();		

	}

	@Test
	public void testFolderWatch() throws Exception {
		Injector serverInjector = ModuleUtils.createInjector(Arrays.asList(new DataImportManagerModule()), Optional.of(config));
		DataImportManager dataImportManager = serverInjector.getInstance(DataImportManager.class);
		assertNotNull(dataImportManager);
		dataImportManager.folderWatch();	
		Thread.sleep(3000);
	}

	@Test
	public void testBeBucketActor() throws Exception {
		try {
			Props props = Props.create(BeBucketActor.class,_service_context.getStorageService());
			ActorSystem system = _actor_context.getActorSystem();
		    ActorRef beBucketActor = system.actorOf(props,"beBucket1");		    
			createEnhancementBeanInDb();			
			beBucketActor.tell(new BucketEnrichmentMessage(bucketPath1, "/misc/bucket1", ActorUtils.BATCH_ENRICHMENT_ZOOKEEPER + buckeFullName1),  ActorRef.noSender());
			Thread.sleep(30000);
		} catch (Exception e) {
			logger.error("Caught exception",e);
			fail(e.getMessage());
		}
	}
	
	protected void createEnhancementBeanInDb() throws InterruptedException, ExecutionException, TimeoutException{

		IManagementCrudService<DataBucketBean> dataStore = _management_db.getDataBucketStore();		
		SingleQueryComponent<DataBucketBean> query_comp_full_name = CrudUtils.anyOf(DataBucketBean.class).when("full_name", buckeFullName1);
			Optional<DataBucketBean> oDataBucketBean = dataStore.getObjectBySpec(query_comp_full_name).get(1, TimeUnit.SECONDS);
			logger.debug(oDataBucketBean);
			if(!oDataBucketBean.isPresent()){
				EnrichmentControlMetadataBean ecm1 = BeanTemplateUtils.build(EnrichmentControlMetadataBean.class).with(EnrichmentControlMetadataBean::name,"bucketConfig1_1").with(EnrichmentControlMetadataBean::enabled, false).done().get();
				EnrichmentControlMetadataBean ecm2 = BeanTemplateUtils.build(EnrichmentControlMetadataBean.class).with(EnrichmentControlMetadataBean::name,"bucketConfig1_2").with(EnrichmentControlMetadataBean::enabled, true).done().get();				
				DataBucketBean bean = BeanTemplateUtils.build(DataBucketBean.class).with(DataBucketBean::_id, buckeFullName1).with(DataBucketBean::full_name, buckeFullName1).with(DataBucketBean::batch_enrichment_configs, Arrays.asList(ecm1,ecm2)).done().get();
				dataStore.storeObject(bean);
			}
			
	}
}
