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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
//import com.ikanow.aleph2.management_db.data_model.BucketMgmtMessage.BucketDeletionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketMgmtMessage.BucketMgmtEventBusWrapper;
import com.ikanow.aleph2.management_db.services.ManagementDbActorContext;
import com.ikanow.aleph2.management_db.utils.ActorUtils;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.event.japi.LookupEventBus;

/** This actor is responsible for ensuring that the data for the bucket is actually deleted
 *  Then deletes the bucket itself
 *  Replies to the bucket deletion singleton actor on success (or if the failure indicates that the bucket is already deleted...)
 * @author Alex
 */
public class BucketDeletionActor extends UntypedActor {
	@SuppressWarnings("unused")
	private static final Logger _logger = LogManager.getLogger();	

	protected final ManagementDbActorContext _actor_context;
	protected final IServiceContext _context;
	protected final LookupEventBus<BucketMgmtEventBusWrapper, ActorRef, String> _bucket_deletion_bus;
	
	public BucketDeletionActor() {
		// Attach self to round robin bus:
		_actor_context = ManagementDbActorContext.get();
		_context = _actor_context.getServiceContext();
		_bucket_deletion_bus = _actor_context.getDeletionMgmtBus();		
		_bucket_deletion_bus.subscribe(this.self(), ActorUtils.BUCKET_DELETION_BUS);
	}
	@Override
	public void onReceive(Object arg0) throws Exception {
		//_logger.info("REAL ACTOR Received message from singleton! " + arg0.getClass().toString());
		
		//TODO: delete the state directories
		
		//TODO: delete all the data services
		
		//TODO: delete the HDFS (containing both data and bucket)
		
		//TODO: return the message on success (or if failure indicates that bucket already deleted)
	}
}
