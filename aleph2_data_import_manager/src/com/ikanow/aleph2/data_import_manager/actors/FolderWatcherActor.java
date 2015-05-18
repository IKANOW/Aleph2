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
package com.ikanow.aleph2.data_import_manager.actors;

import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.log4j.Logger;

import scala.concurrent.duration.Duration;
import akka.actor.Cancellable;
import akka.actor.UntypedActor;

import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;

public class FolderWatcherActor extends UntypedActor {
	

    private static final Logger logger = Logger.getLogger(FolderWatcherActor.class);

	protected CuratorFramework curator_framework;

	protected IStorageService storage_service;

    public FolderWatcherActor(IStorageService storage_service, CuratorFramework curator_framework){
    	this.storage_service = storage_service;
    	this.curator_framework = curator_framework;
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
		}else 	if ("stop".equals(message)) {
				logger.debug("Stop message received");
				tick.cancel();
			} else {
			unhandled(message);
		}
	}
}
