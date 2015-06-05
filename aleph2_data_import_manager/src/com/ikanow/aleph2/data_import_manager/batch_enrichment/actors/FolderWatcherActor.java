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
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;

import scala.concurrent.duration.Duration;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import akka.actor.UntypedActor;

import com.ikanow.aleph2.data_import_manager.services.DataImportActorContext;
import com.ikanow.aleph2.data_import_manager.utils.DirUtils;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.management_db.utils.ActorUtils;

public class FolderWatcherActor extends UntypedActor {
	

    private static final Logger logger = Logger.getLogger(FolderWatcherActor.class);

	protected CuratorFramework _curator	;
	protected final DataImportActorContext _context;
	protected final IManagementDbService _management_db;
	protected final ICoreDistributedServices _core_distributed_services;
	protected final IStorageService _storage_service;
	protected GlobalPropertiesBean _global_properties_Bean = null; 
	protected List<Path> _bucket_paths = null;
	protected FileContext fileContext = null;
	protected Path dataPath = null;

    public FolderWatcherActor(IStorageService storage_service){
    	this._context = DataImportActorContext.get(); 
    	this._global_properties_Bean = _context.getGlobalProperties();
    	logger.debug("_global_properties_Bean"+_global_properties_Bean);
    	this._core_distributed_services = _context.getDistributedServices();    	
    	this._curator = _core_distributed_services.getCuratorFramework();
    	this._management_db = _context.getServiceContext().getCoreManagementDbService();
    	this._storage_service = storage_service;
		this.fileContext = storage_service.getUnderlyingPlatformDriver(FileContext.class,Optional.of("hdfs://localhost:8020"));
		this.dataPath = new Path(_global_properties_Bean.distributed_root_dir()+"/data");
		this._bucket_paths = detectBucketPaths();
    }
    
	
    private  Cancellable tick = null;
    
	@Override
	public void postStop() {
		logger.debug("postStop");
		if(tick!=null){
			tick.cancel();
		}
	}

	@Override
	public void onReceive(Object message) throws Exception {
		if ("start".equals(message)) {
			logger.debug("Start message received");
			tick = getContext()
			.system()
			.scheduler()
			.schedule(Duration.create(1000, TimeUnit.MILLISECONDS),
					Duration.create(8000, TimeUnit.MILLISECONDS), getSelf(),
					"tick", getContext().dispatcher(), null);				

		}else
		if ("tick".equals(message)) {
			logger.debug("Tick message received");
			traverseFolders();
		}else 	if ("stop".equals(message)) {
				logger.debug("Stop message received");
				if(tick!=null){
					tick.cancel();
				}	
			} else {
				logger.debug("unhandeld message:"+message);
			unhandled(message);
		}
	}

	protected void traverseFolders() {
		logger.debug("traverseFolders for path: "+_bucket_paths);
		try {
			detectBucketPaths();
			if(_bucket_paths!=null){
				logger.debug("traverseFolders for path: "+_bucket_paths);
				for (Path path : _bucket_paths) {
					String bucketPathStr = path.toString();
					String dataPathStr = dataPath.toString();
				    String bucketFullName = createFullName(bucketPathStr, dataPathStr);
				    // create or send message to BatchBucketActors
				    checkAndScheduleBucketAgent(bucketPathStr, bucketFullName);					
				}
			}
		} catch (Exception e) {
			logger.error("traverseFolders Caught Exception:",e);
		}

	}
	
	protected static String createFullName(@Nonnull String bucketPathStr, @Nonnull String dataPathStr) {
		String fullName = bucketPathStr;
		int dataPathPos = bucketPathStr.indexOf(dataPathStr);
		if(dataPathPos>0 && bucketPathStr.length()> dataPathPos + dataPathStr.length()){
			fullName = bucketPathStr.substring(dataPathPos + dataPathStr.length());
		}
		return fullName;
	}

	protected static String createAgentName(@Nonnull String fullName) {
		String agentName = fullName.replace('/', '$').replace('\\', '$')+  UUID.randomUUID().toString();
		return agentName;
		
	}

	protected void checkAndScheduleBucketAgent(String bucketPathStr, String bucketFullName) {
		//curator_framework.
	    logger.debug("checkAndScheduleBucketAgent for Bucket Path: "+bucketPathStr+" ,Bucket id: "+bucketFullName);
		try{
			/*			
			Stat bucketExists = _curator.checkExists().forPath(bucketZkPath);
			if(bucketExists==null ){
*/				//bucket is not registered yet, grab it and do the processing on this node
				// send message to batchBus so some actor will take care if this node
				//_curator.create().creatingParentsIfNeeded().forPath(bucketZkPath);
				ActorRef beActor = getContext().actorOf(Props.create(BeBucketActor.class,_storage_service),bucketFullName);
				String bucketZkPath = ActorUtils.BATCH_ENRICHMENT_ZOOKEEPER + bucketFullName;
				beActor.tell(new BucketEnrichmentMessage(bucketPathStr, bucketFullName, bucketZkPath), getSelf());						
			/*}else{
				// someone else already is working in it
				logger.debug("Path "+bucketZkPath+" is already locked in zookeeper!");
			} */
		}
		catch(Exception e){
			logger.error("Caught Exception",e);
		}
		//_state.data_import_manager_set.addAll();
		//


	}

	protected List<Path> detectBucketPaths(){
		logger.debug("detectBucketPaths");
		if(_bucket_paths==null){
			try {
				// looking for managed_bucket underneath /app/aleph2/data
				_bucket_paths =  new ArrayList<Path>();
				DirUtils.findAllSubdirectories(_bucket_paths, fileContext, dataPath, "managed_bucket",false);
				logger.info("detectBucketPath found "+_bucket_paths.size()+" folders.");
			} catch (Exception e) {
				logger.error("detectBucketPath Caught Exception",e);
			} 
		}
		return _bucket_paths;
	}
}
