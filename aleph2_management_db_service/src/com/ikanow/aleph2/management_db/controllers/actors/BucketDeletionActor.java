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

import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.shared.AssetStateDirectoryBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Patterns;
import com.ikanow.aleph2.data_model.utils.SetOnce;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionRetryMessage;
import com.ikanow.aleph2.management_db.data_model.BucketMgmtMessage.BucketDeletionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketMgmtMessage.BucketMgmtEventBusWrapper;
import com.ikanow.aleph2.management_db.services.DataBucketCrudService;
import com.ikanow.aleph2.management_db.services.ManagementDbActorContext;
import com.ikanow.aleph2.management_db.utils.ActorUtils;
import com.ikanow.aleph2.management_db.utils.MgmtCrudUtils;

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
	protected final IManagementDbService _core_mgmt_db;
	protected final IStorageService _storage_service;
	protected final SetOnce<ICrudService<DataBucketBean>> _bucket_crud_proxy = new SetOnce<>();
	
	public BucketDeletionActor() {
		// Attach self to round robin bus:
		_actor_context = ManagementDbActorContext.get();
		_context = _actor_context.getServiceContext();
		_bucket_deletion_bus = _actor_context.getDeletionMgmtBus();		
		_bucket_deletion_bus.subscribe(this.self(), ActorUtils.BUCKET_DELETION_BUS);
		_core_mgmt_db = _context.getCoreManagementDbService();
		_storage_service = _context.getStorageService();
	}
	@Override
	public void onReceive(Object arg0) throws Exception {
		if (!_bucket_crud_proxy.isSet()) { // (for some reason, core_mdb.anything() can fail in the c'tor)
			_bucket_crud_proxy.set(_core_mgmt_db.getDataBucketStore());
		}
		//_logger.info("REAL ACTOR Received message from singleton! " + arg0.getClass().toString());
		if (!BucketDeletionMessage.class.isAssignableFrom(arg0.getClass())) { // not for me
			_logger.debug("Unexpected message: " + arg0.getClass());
			return;
		}
		final ActorRef self_closure = this.self();		
		final ActorRef sender_closure = this.sender();		
		final BucketDeletionMessage msg = (BucketDeletionMessage) arg0;

		// 1) Before we do anything at all, has this bucket already been deleted somehow?
		
		if (!DataBucketCrudService.doesBucketPathExist(msg.bucket(), _storage_service, Optional.empty())) {			
			sender_closure.tell(msg, self_closure);
			return;
		}
		
		if (msg.data_only()) { // 2) purge is a bit simpler
			
			// 2a) DON'T Delete the state stores (this has to be done by hand by the harvester if desired)			
			
			// 2b) Delete data in all data services
			deleteAllDataStoresForBucket(msg.bucket(), _context, false);
			
			notifyHarvesterOfPurge(msg.bucket(), _core_mgmt_db.getDataBucketStatusStore(), _core_mgmt_db.getRetryStore(BucketActionRetryMessage.class));
			
			// If we got this far then remove from the queue
			sender_closure.tell(msg, self_closure);
		}
		else { // 3) OK check for the rare but unpleasant case where the bucket wasn't deleted
			
			final QueryComponent<DataBucketBean> bucket_selector = CrudUtils.allOf(DataBucketBean.class).when(DataBucketBean::full_name, msg.bucket().full_name());
			_bucket_crud_proxy.get().getObjectBySpec(bucket_selector)
				.thenAccept(bucket_opt -> {
					if (bucket_opt.isPresent()) {
						
						// Hasn't been deleted yet - try to delete async and then just exit out
						CompletableFuture<Boolean> deleted = _bucket_crud_proxy.get().deleteObjectBySpec(bucket_selector);
						//(see you in an hour!)
						
						//(some logging)
						deleted.thenAccept(b -> {
							_logger.warn(ErrorUtils.get("Problem: deleting bucket {0} not yet removed from bucket store: retrying delete: {1}", msg.bucket().full_name(), b));							
						})
						.exceptionally(t -> {
							_logger.error(ErrorUtils.get("Problem: deleting bucket {1} not yet removed from bucket store: retrying delete failed: {0}", t, msg.bucket().full_name()));
							return null;
						});						
					}
					else { 						
						// 3a) Delete the state directories					
						deleteAllStateObjectsForBucket(msg.bucket(), _core_mgmt_db, false);
						
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
	
	/** Removes all "user" state related to the bucket being deleted/purged
	 * @param bucket
	 * @param core_management_db_service
	 */
	public static void deleteAllStateObjectsForBucket(final DataBucketBean bucket, final IManagementDbService core_management_db_service, boolean delete_bucket) {
		final ICrudService<AssetStateDirectoryBean> states = core_management_db_service.getStateDirectory(Optional.of(bucket), Optional.empty());
		
		states.getObjectsBySpec(CrudUtils.allOf(AssetStateDirectoryBean.class)).thenAccept(cursor -> {
			StreamSupport.stream(cursor.spliterator(), true).forEach(bean -> {
				
				Optional.ofNullable( 
						Patterns.match(bean.state_type()).<ICrudService<JsonNode>>andReturn()
							.when(t -> AssetStateDirectoryBean.StateDirectoryType.analytic_thread == t, 
										__ -> core_management_db_service.getBucketAnalyticThreadState(JsonNode.class, bucket, Optional.of(bean.collection_name())))
							.when(t -> AssetStateDirectoryBean.StateDirectoryType.harvest == t, 
										__ -> core_management_db_service.getBucketHarvestState(JsonNode.class, bucket, Optional.of(bean.collection_name())))
							.when(t -> AssetStateDirectoryBean.StateDirectoryType.enrichment == t, 
										__ -> core_management_db_service.getBucketEnrichmentState(JsonNode.class, bucket, Optional.of(bean.collection_name())))
							.otherwise(__ -> null)
					)
					.ifPresent(to_delete -> to_delete.deleteDatastore());					
			});
		});
		
	}
	
	/** Notifies the external harvester of the purge operation
	 * @param bucket
	 * @param retry_store
	 * @return
	 */
	public static CompletableFuture<Collection<BasicMessageBean>> notifyHarvesterOfPurge(final DataBucketBean to_purge, final ICrudService<DataBucketStatusBean> status_store, final ICrudService<BucketActionRetryMessage> retry_store)
	{
		return status_store.getObjectBySpec(CrudUtils.allOf(DataBucketStatusBean.class).when(DataBucketStatusBean::bucket_path, to_purge.full_name()))
			.thenCompose(status -> {
				if (status.isPresent()) {
					final BucketActionMessage.PurgeBucketActionMessage purge_msg = new BucketActionMessage.PurgeBucketActionMessage(to_purge, new HashSet<String>(status.get().node_affinity()));
					final CompletableFuture<Collection<BasicMessageBean>> management_results =
							MgmtCrudUtils.applyRetriableManagementOperation(to_purge, 
									ManagementDbActorContext.get(), retry_store, purge_msg,
									source -> new BucketActionMessage.PurgeBucketActionMessage(to_purge, new HashSet<String>(Arrays.asList(source))));
					return management_results;
				}
				else {
					return CompletableFuture.completedFuture(
							Arrays.asList(new BasicMessageBean(new Date(), false, "CoreManagementDbService", "purgeBucket", null, "No bucket status for " + to_purge.full_name(), null)));
				}
			});		
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
			vals.add(service_context.getStorageService().handleBucketDeletionRequest(bucket, false));
		}		
		
		return CompletableFuture.allOf(vals.toArray(new CompletableFuture[0]))
				.thenApply(__ -> {
					return vals.stream().map(x -> x.join()).collect(Collectors.toList());
				});
	}
	/* (non-Javadoc)
	 * @see akka.actor.UntypedActor#postStop()
	 */
	@Override
	public void postStop() {
		_bucket_deletion_bus.unsubscribe(this.self(), ActorUtils.BUCKET_DELETION_BUS);		
		_logger.info("BucketDeletionActor has stopped.");								
	}
}
