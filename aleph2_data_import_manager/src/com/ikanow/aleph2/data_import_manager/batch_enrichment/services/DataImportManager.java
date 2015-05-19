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
package com.ikanow.aleph2.data_import_manager.batch_enrichment.services;

import org.apache.curator.framework.CuratorFramework;
import org.apache.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.actor.Props;

import com.google.inject.Inject;
import com.ikanow.aleph2.data_import_manager.batch_enrichment.actors.FolderWatcherActor;
import com.ikanow.aleph2.data_model.interfaces.data_access.IServiceContext;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICoreDistributedServices;

public class DataImportManager  {
	private static final Logger logger = Logger.getLogger(DataImportManager.class);
    private ActorSystem system = null;
    protected ActorRef folderWatchActor = null;
    
//    @Inject
    ICoreDistributedServices core_distributed_services;

//    @Inject
    IStorageService storage_service;
    
    @Inject
    public DataImportManager(@NonNull IServiceContext service_context) {
    	core_distributed_services = service_context.getCoreDistributedServices();
    	storage_service = service_context.getStorageIndexService();
    }
    
	public void start() {
        // Create the 'greeter' actor
		logger.info("DataImportManager starting...");
		system = ActorSystem.create("data_import_manager");
		CuratorFramework curatorFramework = core_distributed_services.getCuratorFramework();
		
		Props props = Props.create(FolderWatcherActor.class,storage_service,curatorFramework);
		folderWatchActor = system.actorOf(props,"folderWatch");
	}

	public void stop() {
		logger.info("DataImportManager stopping...");
		folderWatchActor.tell(PoisonPill.getInstance(), ActorRef.noSender());
	}
}
