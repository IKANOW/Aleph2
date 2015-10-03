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
package com.ikanow.aleph2.data_import_manager.batch_enrichment.actors;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.UntypedActor;

import com.ikanow.aleph2.data_import_manager.services.DataImportActorContext;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.CrudUtils.SingleQueryComponent;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.management_db.utils.ActorUtils;

/** THIS CODE IS GETTING MOVED INTO THE DATA ANALYTICS MANAGER TRIGGER LOGIC
 * @author jfreydank
 */
public class BeBucketActor extends UntypedActor {

	private static final Logger logger = LogManager.getLogger(BeBucketActor.class);

	protected CuratorFramework _curator;
	protected final DataImportActorContext _actor_context;
	protected final IManagementDbService _management_db;
	protected final ICoreDistributedServices _core_distributed_services;
	protected final IStorageService storage_service;
	protected GlobalPropertiesBean _global_properties_Bean = null;

	protected String bucketZkPath = null;
	protected FileContext fileContext = null;

	// not null means agent has been initialized.

	public BeBucketActor(IStorageService storage_service) {
		this._actor_context = DataImportActorContext.get();
		this._global_properties_Bean = _actor_context.getGlobalProperties();
		logger.debug("_global_properties_Bean" + _global_properties_Bean);
		this._core_distributed_services = _actor_context.getDistributedServices();
		this._curator = _core_distributed_services.getCuratorFramework();
		this._management_db = _actor_context.getServiceContext().getCoreManagementDbService();
		this.storage_service = storage_service;
		this.fileContext = storage_service.getUnderlyingPlatformDriver(FileContext.class, Optional.of("hdfs://localhost:8020")).get();
	}

	@Override
	public void postStop() {
		logger.debug("postStop");
		if (bucketZkPath != null) {
			try {
				logger.debug("Deleting bucket path in ZK:" + bucketZkPath);
				_curator.delete().forPath(bucketZkPath);
			} catch (Exception e) {
				logger.error("Caught exception", e);
			}
		}		
	}

	@Override
	public void onReceive(Object message) throws Exception {
		logger.debug("Message received:" + message);
		if (message instanceof BucketEnrichmentMessage) {

			BucketEnrichmentMessage bem = (BucketEnrichmentMessage) message;
			Stat bucketExists = null;
			try {
				this.bucketZkPath = ActorUtils.BATCH_ENRICHMENT_ZOOKEEPER + bem.getBuckeFullName();				
				bucketExists = _curator.checkExists().forPath(bucketZkPath);
			} catch (Exception e) {
				// do nothing on purpose, whole path might not exist in zk
				//logger.debug("Caught exception for zk path:"+bucketZkPath,e);
			}
			if (bucketExists == null) {
				// bucket is not registered yet, grab it and do the processing
				// on this node
				_curator.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(bucketZkPath);
				launchReadyJobs(fileContext,bem.getBuckeFullName(), bem.getBucketPathStr(),_management_db,this.self());

				// stop actor after all the processing
				getContext().stop(getSelf());
			} else {
				logger.debug("Bucket alrady exists in ZK:" + bucketExists);
				// stop actor if node exists
				getContext().stop(getSelf());
			}
		} else {
			logger.debug("unhandeld message:" + message);
			unhandled(message);
		}
	}


	public  static List<String> launchReadyJobs(FileContext fileContext, String bucketFullName, String bucketPathStr,IManagementDbService managementDbService,ActorRef closingSelf) {
		List<String> jobNames = new ArrayList<String>();
		try {
			Path bucketReady = new Path(bucketPathStr + "/managed_bucket/import/ready");
			//Path bucketTmp = new Path(bucketPathStr + "/managed_bucket/import/temp");
			if (fileContext.util().exists(bucketReady)) {
				FileStatus[] statuss = fileContext.util().listStatus(bucketReady);				
				if (statuss.length > 0) {
					logger.debug("Detected " + statuss.length + " ready files.");

					IManagementCrudService<DataBucketBean> dataBucketStore = managementDbService.getDataBucketStore();
					SingleQueryComponent<DataBucketBean> querydatBucketFullName = CrudUtils.anyOf(DataBucketBean.class).when("full_name",bucketFullName);
					
					dataBucketStore.getObjectBySpec(querydatBucketFullName).thenAccept(
							odb -> {
								if (odb.isPresent()) {
									DataBucketBean dataBucketBean = odb.get();
									List<EnrichmentControlMetadataBean> enrichmentConfigs = dataBucketBean.batch_enrichment_configs();
									for (EnrichmentControlMetadataBean ec : enrichmentConfigs) {										
										if (ec.enabled()) {											
											logger.info("starting batch enhancment job: "+bucketFullName+" for "+ec.name());
											// run enhancement job
											
											//TODO (ALEPH-12): this now should communicate with the enrichment actors
											//String jobName = beJobService.runEnhancementJob(bucketFullName, bucketPathStr, ec.name());
											String jobName = Lambdas.get(() -> null);
											if(jobName!=null){
												jobNames.add(jobName);
												logger.info("Enrichment job for , no enrichment enabled:"+bucketFullName +" ec:"+ec.name() +" launched unsuccessfully, jobName = "+jobName);
											}else{
												logger.error("Enrichment job for , no enrichment enabled:"+bucketFullName +" ec:"+ec.name() +" launch was unsuccessful");
											}
										} // if enabled
										else{
											logger.info("Skipping Enrichment, no enrichment enabled:"+bucketFullName +" ec:"+ec.name());
										}
									} // for
								} else {
									logger.info("Skipping Enrichment, no enrichment config found in db: "+bucketFullName );
									if(closingSelf!=null){
									closingSelf.tell(PoisonPill.getInstance(), closingSelf);
									}
								}
							});
				} // status length
				else{
					logger.info("Skipping, no files found in ready folder: "+bucketReady );
				}
			}else {
				logger.info("Skipping,  ready folder does not exist: "+bucketReady );
			}

		} catch (Exception e) {
			logger.error("checkReady caught Exception:", e);
		}
		return jobNames;
	}
}

