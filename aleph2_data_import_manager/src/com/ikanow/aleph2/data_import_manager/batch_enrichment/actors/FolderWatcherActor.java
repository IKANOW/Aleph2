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
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.log4j.Logger;

import scala.concurrent.duration.Duration;
import akka.actor.ActorSystem;
import akka.actor.Cancellable;
import akka.actor.UntypedActor;

import com.ikanow.aleph2.data_import_manager.services.DataImportManagerActorContext;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;

public class FolderWatcherActor extends UntypedActor {
	

    private static final Logger logger = Logger.getLogger(FolderWatcherActor.class);

	protected CuratorFramework curator_framework;
	protected IStorageService storage_service;
	protected final DataImportManagerActorContext _context;
	protected final IManagementDbService _management_db;
	protected final ICoreDistributedServices _core_distributed_services;
	protected final ActorSystem _actor_system;

    public FolderWatcherActor(){
    	_context = DataImportManagerActorContext.get(); 
    	_core_distributed_services = _context.getDistributedServices();
    	this.curator_framework = _core_distributed_services.getCuratorFramework();
    	_actor_system = _core_distributed_services.getAkkaSystem();
    	_management_db = _context.getServiceContext().getManagementDbService();
    }
    
	private final Cancellable tick = getContext()
			.system()
			.scheduler()
			.schedule(Duration.create(1000, TimeUnit.MILLISECONDS),
					Duration.create(5000, TimeUnit.MILLISECONDS), getSelf(),
					"tick", getContext().dispatcher(), null);

	@Override
	public void postStop() {
		logger.debug("postStop");
		tick.cancel();
	}

	@Override
	public void onReceive(Object message) throws Exception {
		if ("tick".equals(message)) {
			logger.debug("Tick message received");
			traverseFolders();
		}else 	if ("stop".equals(message)) {
				logger.debug("Stop message received");
				tick.cancel();
			} else {
			unhandled(message);
		}
	}

	protected void traverseFolders() {
		AbstractFileSystem fs = storage_service.getUnderlyingPlatformDriver(AbstractFileSystem.class,Optional.<String>empty());
		String rootPath = storage_service.getRootPath();
		
		
	}
}
