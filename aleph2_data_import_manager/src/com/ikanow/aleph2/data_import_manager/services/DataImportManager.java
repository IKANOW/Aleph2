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
package com.ikanow.aleph2.data_import_manager.services;

import org.apache.log4j.Logger;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.actor.Props;

import com.ikanow.aleph2.data_import_manager.actors.FolderWatcherActor;

public class DataImportManager {
	private static final Logger logger = Logger.getLogger(DataImportManager.class);
    private ActorSystem system = null;
    protected ActorRef folderWatchActor = null;

	public void start() {		
        // Create the 'greeter' actor
		system = ActorSystem.create("data_import_manager");
		folderWatchActor = system.actorOf(Props.create(FolderWatcherActor.class), "folderWatch");
	}

	public void stop() {
		// TODO Auto-generated method stub
		folderWatchActor.tell(PoisonPill.getInstance(), ActorRef.noSender());
	}

}
