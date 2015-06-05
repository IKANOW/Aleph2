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

import java.util.Optional;

import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;

import akka.actor.UntypedActor;

import com.ikanow.aleph2.data_import_manager.services.DataImportActorContext;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.SingleQueryComponent;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.management_db.utils.ActorUtils;

public class BeBucketActor extends UntypedActor {

	private static final Logger logger = Logger.getLogger(BeBucketActor.class);

	protected CuratorFramework _curator;
	protected final DataImportActorContext _context;
	protected final IManagementDbService _management_db;
	protected final ICoreDistributedServices _core_distributed_services;
	protected final IStorageService storage_service;
	protected GlobalPropertiesBean _global_properties_Bean = null;

	protected String bucketZkPath = null;
	protected FileContext fileContext = null;

	private String bucketId;

	// not null means agent has been initialized.

	public BeBucketActor(IStorageService storage_service) {
		this._context = DataImportActorContext.get();
		this._global_properties_Bean = _context.getGlobalProperties();
		logger.debug("_global_properties_Bean" + _global_properties_Bean);
		this._core_distributed_services = _context.getDistributedServices();
		this._curator = _core_distributed_services.getCuratorFramework();
		this._management_db = _context.getServiceContext().getCoreManagementDbService();
		this.storage_service = storage_service;
		this.fileContext = storage_service.getUnderlyingPlatformDriver(FileContext.class, Optional.of("hdfs://localhost:8020")).get();
	}

	@Override
	public void postStop() {
		logger.debug("postStop");
		stopActor();
	}

	protected void stopActor(){
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
			this.bucketZkPath = ActorUtils.BATCH_ENRICHMENT_ZOOKEEPER + "/" + bem.getBucketId();
			Stat bucketExists = _curator.checkExists().forPath(bucketZkPath);
			if (bucketExists == null) {
				// bucket is not registered yet, grab it and do the processing
				// on this node
				_curator.create().creatingParentsIfNeeded().forPath(bucketZkPath);
				checkReady(bucketId, bem.getBucketPathStr());

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

	protected void checkReady(String bucketId, String bucketPathStr) {
		try {
			Path bucketReady = new Path(bucketPathStr + "/managed_bucket/import/ready");
			if (fileContext.util().exists(bucketReady)) {
				FileStatus[] statuss = fileContext.util().listStatus(bucketReady);
				if (statuss.length > 0) {
					logger.debug("Detected " + statuss.length + " ready files.");
					// TODO
					// load cached bucket by id and see if it is enabled
					String fullName = "TODO";
					
					IManagementCrudService<DataBucketBean> bucketStatusStore = _management_db.getDataBucketStore();
					SingleQueryComponent<DataBucketBean> query_comp_full_name = CrudUtils.anyOf(DataBucketBean.class).when("full_name", fullName);
					bucketStatusStore.getObjectBySpec(query_comp_full_name).thenAccept(odb -> {
						 if (odb.isPresent()) {
							
							boolean containsEnrichment = odb.get().batch_enrichment_configs().stream().map(EnrichmentControlMetadataBean::enabled).reduce(false, (a ,b)-> a ||  b);
							//send self a stop message
							if(!containsEnrichment){
								stopActor();
							}else{
								// TODO enrichment work on bucket
							}
						} else { 
							stopActor();
						}
							
						});
					} // status length					
					} 
						
		} catch (Exception e) {
			logger.error("checkReady caught Exception:", e);
		}

	}
}

/*	protected Optional<DataBucketBean> loadBucket(String bucketId, String fullName, String bucketPathStr) {

		Optional<DataBucketBean> odb  bucketStatusStore.getObjectBySpec(query_comp_full_name).thenComposeAsync(bucketBean -> {
			return null;   }).thenAccept(action);
                                        
*/