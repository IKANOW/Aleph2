package com.ikanow.aleph2.data_import_manager.batch_enrichment.utils;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.fs.FileContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_import_manager.services.DataImportActorContext;
import com.ikanow.aleph2.data_import_manager.services.GeneralInformationService;
import com.ikanow.aleph2.data_import_manager.utils.DirUtils;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean.MasterEnrichmentType;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.data_import.HarvestControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.SingleQueryComponent;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.data_model.utils.UuidUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

public abstract class DataBucketTest {
    private static final Logger logger = LogManager.getLogger(DataBucketTest.class);

    protected String bucketPath1 = null;
	protected String bucketReadyPath1 = null;
	protected String buckeFullName1= "/misc/bucket1";
	protected IManagementDbService _management_db;
	protected FileContext fileContext =null;
	@Inject 
	protected IServiceContext _service_context = null;
	
	protected DataImportActorContext _actor_context;
	//protected ManagementDbActorContext _db_actor_context;

	protected Config config = null;


	@Before
	public void setupDependencies() throws Exception {
		if (_service_context != null) {
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


		createFolderStructure();
		this._management_db = _actor_context.getServiceContext().getCoreManagementDbService();
		
}

	protected void createFolderStructure(){
		// create folder structure if it does not exist for testing.		
		this.fileContext = _service_context.getStorageService().getUnderlyingPlatformDriver(FileContext.class,Optional.empty()).get();
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

	protected void createEnhancementBeanInDb() throws InterruptedException, ExecutionException, TimeoutException{

		IManagementCrudService<DataBucketBean> dataBucketStore = _management_db.getDataBucketStore();		
		IManagementCrudService<DataBucketStatusBean> dataBucketStatusStore = _management_db.getDataBucketStatusStore();		
		SingleQueryComponent<DataBucketBean> query_comp_full_name = CrudUtils.anyOf(DataBucketBean.class).when("full_name", buckeFullName1);
			Optional<DataBucketBean> oDataBucketBean = dataBucketStore.getObjectBySpec(query_comp_full_name).get(1, TimeUnit.SECONDS);
			logger.debug(oDataBucketBean);
			if(!oDataBucketBean.isPresent()){
				EnrichmentControlMetadataBean ecm1 = BeanTemplateUtils.build(EnrichmentControlMetadataBean.class)
						.with(EnrichmentControlMetadataBean::name,"bucketEnrichmentConfig1_1")
						.with(EnrichmentControlMetadataBean::enabled, false)
						.with(EnrichmentControlMetadataBean::library_ids_or_names,Arrays.asList("dummylib1.jar"))
						.done().get();
				EnrichmentControlMetadataBean ecm2 = BeanTemplateUtils.build(EnrichmentControlMetadataBean.class)
						.with(EnrichmentControlMetadataBean::name,"bucketEnrichmentConfig1_2")
						.with(EnrichmentControlMetadataBean::enabled, true)
						.with(EnrichmentControlMetadataBean::library_ids_or_names,Arrays.asList("dummylib2.jar"))
						.done().get();				
				HarvestControlMetadataBean hcm1 = BeanTemplateUtils.build(HarvestControlMetadataBean.class)
						.with(HarvestControlMetadataBean::name,"bucketEnrichment1")
						.with(HarvestControlMetadataBean::enabled, true)
						.done().get();
				
				Date now = new Date();
				DataBucketBean valid_bucket = BeanTemplateUtils.build(DataBucketBean.class)
						.with(DataBucketBean::_id, buckeFullName1)
						.with(DataBucketBean::full_name, buckeFullName1)
						.with(DataBucketBean::created, now)
						.with(DataBucketBean::modified, now)
						.with(DataBucketBean::batch_enrichment_configs, Arrays.asList(ecm1,ecm2))
						.with(DataBucketBean::harvest_configs, Arrays.asList(hcm1))
						.with(DataBucketBean::master_enrichment_type,MasterEnrichmentType.batch)
						.with(DataBucketBean::display_name, "Test Bucket ")
						.with(DataBucketBean::harvest_technology_name_or_id, "/app/aleph2/library/import/harvest/tech/here/" + 1)
						.with(DataBucketBean::tags, Collections.emptySet())
						.with(DataBucketBean::owner_id, UuidUtils.get().getRandomUuid())
						//.with(DataBucketBean::master_enrichment_type, val)
						.with(DataBucketBean::access_rights, new AuthorizationBean(ImmutableMap.<String, String>builder().put("auth_token", "rw").build()))
						.done().get();
				

				final DataBucketStatusBean status = 
						BeanTemplateUtils.build(DataBucketStatusBean.class)
						.with(DataBucketStatusBean::_id, valid_bucket._id())
						.with(DataBucketStatusBean::bucket_path, valid_bucket.full_name())
						.with(DataBucketStatusBean::suspended, false)
						.done().get();

				dataBucketStatusStore.storeObject(status).get();
				dataBucketStore.storeObject(valid_bucket).get();

				//	Optional<DataBucketBean> oDataBucketBean2 = dataBucketStore.getObjectById(buckeFullName1).get(1, TimeUnit.SECONDS);
				//logger.debug(oDataBucketBean2);

			}
			
	}

}
