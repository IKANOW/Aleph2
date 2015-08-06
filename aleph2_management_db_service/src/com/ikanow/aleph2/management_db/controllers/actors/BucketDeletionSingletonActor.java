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
package com.ikanow.aleph2.management_db.controllers.actors;

import akka.actor.UntypedActor;

/** This actor is a singleton, ie runs on only one node in the cluster
 *  its role is to monitor the test queue looking for tests that need to expire
 *  then send a message out to any harvesters that might be running them
 *  and then once confirmed stopped, place the test bucket on the delete queue
 *  (ie use Akka's scheduler to monitor pings every 1s if the test queue has anything in it, 10s otherwise?) 
 * @author cburch
 *
 */
public class BucketDeletionSingletonActor extends UntypedActor {

	public BucketDeletionSingletonActor() {
		
	}
	
	/* (non-Javadoc)
	 * @see akka.actor.UntypedActor#onReceive(java.lang.Object)
	 */
	@Override
	public void onReceive(Object arg0) throws Exception {
		// TODO Auto-generated method stub
		
	}

}
