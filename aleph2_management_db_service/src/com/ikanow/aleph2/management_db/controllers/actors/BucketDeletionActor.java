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

import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.Optional;





import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;












import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.AssetStateDirectoryBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
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
	protected final IStorageService _storage_service;
	protected final ICrudService<DataBucketBean> _bucket_crud_proxy;
	
	public BucketDeletionActor() {
		// Attach self to round robin bus:
		_actor_context = ManagementDbActorContext.get();
		_context = _actor_context.getServiceContext();
		_bucket_deletion_bus = _actor_context.getDeletionMgmtBus();		
		_bucket_deletion_bus.subscribe(this.self(), ActorUtils.BUCKET_DELETION_BUS);
		_core_management_db_service = _context.getCoreManagementDbService();
		_storage_service = _context.getStorageService();
		_bucket_crud_proxy = _core_management_db_service.getDataBucketStore();
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
		
		if (DataBucketCrudService.doesBucketPathExist(msg.bucket(), _storage_service, Optional.empty())) {
			sender_closure.tell(msg, self_closure);
			return;
		}
		
		if (msg.data_only()) { // 2) purge is a bit simpler
			
			// 2a) Delete the state directories					
			final ICrudService<AssetStateDirectoryBean> states = _core_management_db_service.getStateDirectory(Optional.of(msg.bucket()), Optional.empty());
			states.deleteDatastore();
			
			// 2b) Delete data in all data services
			deleteAllDataStoresForBucket(msg.bucket(), _context, false);
			
			// If we got this far then remove from the queue
			sender_closure.tell(msg, self_closure);
		}
		else { // 3) OK check for the rare but unpleasant case where the bucket wasn't deleted
		
			_bucket_crud_proxy.getObjectById(msg.bucket().full_name())
				.thenAccept(bucket_opt -> {
					if (bucket_opt.isPresent()) {
						// Hasn't been deleted yet - try to delete async and then just exit out
						_bucket_crud_proxy.deleteObjectById(msg.bucket().full_name());
						//(see you in an hour!)
					}
					else { 
						
						// 3a) Delete the state directories					
						final ICrudService<AssetStateDirectoryBean> states = _core_management_db_service.getStateDirectory(Optional.of(msg.bucket()), Optional.empty());
						states.deleteDatastore();
						
						// 3b) Delete data in all data services
						deleteAllDataStoresForBucket(msg.bucket(), _context, true);
						
						// 3c) Delete the HDFS data (includes all the archived/stored data)
						try {
							DataBucketCrudService.removeBucketPath(msg.bucket(), _storage_service, Optional.empty());
							
							// If we got this far then delete the bucket forever
							sender_closure.tell(msg, self_closure);
						}
						catch (Exception e) {
							// failed to delete the bucket
						}
					}
				});
		}
	}
	
	/** Deletes the data in all data services
	 *  TODO: assume default ones for now 
	 * @param bucket - the bucket to cleanse
	 */
	public static CompletableFuture<Collection<BasicMessageBean>> deleteAllDataStoresForBucket(final DataBucketBean bucket, final IServiceContext service_context, boolean delete_bucket) {
		
		// Currently the only supported data service is the search index
		
		final Optional<ISearchIndexService> search_index = service_context.getSearchIndexService();
	
		final LinkedList<CompletableFuture<BasicMessageBean>> vals = new LinkedList<>();
		
		search_index.ifPresent(index -> {
			vals.add(index.handleBucketDeletionRequest(bucket, delete_bucket));
		});
		
		if (!delete_bucket) { // (Else will be deleted in the main actor fn)
			try {
				DataBucketCrudService.removeBucketPath(bucket, service_context.getStorageService(), 
						Optional.of("TODO"));
			}
			catch (Exception e) {
				vals.add(CompletableFuture.completedFuture(
							new BasicMessageBean(new Date(), false, "BucketDeletionActor", "deleteAllDataStoresForBucket", null, ErrorUtils.getLongForm("{0}", e), null)
						));
			}
		}		
		
		return CompletableFuture.allOf(vals.toArray(new CompletableFuture[0]))
				.thenApply(__ -> {
					return vals.stream().map(x -> x.join()).collect(Collectors.toList());
				});
	}
}
