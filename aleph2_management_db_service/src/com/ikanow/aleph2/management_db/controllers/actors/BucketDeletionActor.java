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

import java.util.Optional;



import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;



import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.AssetStateDirectoryBean;
import com.ikanow.aleph2.management_db.data_model.BucketMgmtMessage.BucketDeletionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketMgmtMessage.BucketMgmtEventBusWrapper;
import com.ikanow.aleph2.management_db.services.DataBucketCrudService;
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
	private static final Logger _logger = LogManager.getLogger();	

	protected final ManagementDbActorContext _actor_context;
	protected final IServiceContext _context;
	protected final LookupEventBus<BucketMgmtEventBusWrapper, ActorRef, String> _bucket_deletion_bus;
	protected final IManagementDbService _core_management_db_service;
	protected final DataBucketCrudService _bucket_crud_proxy; //TODO: if we can move tihs to being  a pure ICrudService even better (ie using static functions)
	
	public BucketDeletionActor() {
		// Attach self to round robin bus:
		_actor_context = ManagementDbActorContext.get();
		_context = _actor_context.getServiceContext();
		_bucket_deletion_bus = _actor_context.getDeletionMgmtBus();		
		_bucket_deletion_bus.subscribe(this.self(), ActorUtils.BUCKET_DELETION_BUS);
		_core_management_db_service = _context.getCoreManagementDbService();
		_bucket_crud_proxy = (DataBucketCrudService) _core_management_db_service.getDataBucketStore();
	}
	@Override
	public void onReceive(Object arg0) throws Exception {		
		//_logger.info("REAL ACTOR Received message from singleton! " + arg0.getClass().toString());
		if (!BucketDeletionMessage.class.isAssignableFrom(arg0.getClass())) { // not for me
			_logger.warn("Unexpected message: " + arg0.getClass());
			return;
		}
		final ActorRef self_closure = this.self();		
		final ActorRef sender_closure = this.sender();		
		final BucketDeletionMessage msg = (BucketDeletionMessage) arg0;
		
		// 1) Before we do anything at all, has this bucket already been deleted somehow?
		
		//TODO: static function in DataBucketCrudService?
		if (false) {
			sender_closure.tell(msg, self_closure);
			return;
		}
		
		// 2) OK check for the rare but unpleasant case where it's been deleted		
		
		_bucket_crud_proxy.getObjectById(msg.bucket().full_name())
			.thenAccept(bucket_opt -> {
				if (bucket_opt.isPresent()) {
					// Hasn't been deleted yet - try to delete async and then just exit out
					_bucket_crud_proxy.deleteObjectById(msg.bucket().full_name());
					//(see you in an hour!)
				}
				else { //TODO: what are the success/fail deps on each of these jobs
					// (put them in functions to make this code more readable?)
					
					// Delete the state directories					
					ICrudService<AssetStateDirectoryBean> states = _core_management_db_service.getStateDirectory(Optional.of(msg.bucket()), Optional.empty());
					states.deleteDatastore();
					
					// Delete data in all data services
					deleteAllDataStoresForBucket(msg.bucket());
					
					// Delete the HDFS data (includes all the archived/stored data)
				}
			});
		
		//TODO: delete all the data services
		
		//TODO: delete the HDFS (containing both data and bucket)
		
		//TODO: return the message on success (or if failure indicates that bucket already deleted)
	}
	
	/** Deletes the data in all data services
	 *  TODO: assume default ones for now 
	 * @param bucket - the bucket to cleanse
	 */
	protected void deleteAllDataStoresForBucket(final DataBucketBean bucket) {
		//TODO
	}
}
