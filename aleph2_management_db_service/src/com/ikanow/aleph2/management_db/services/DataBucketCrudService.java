/*******************************************************************************
 * Copyright 2015, The IKANOW Open Source Project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package com.ikanow.aleph2.management_db.services;

import java.io.FileNotFoundException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.interfaces.data_services.IColumnarService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IDocumentService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.data_services.ITemporalService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IBasicSearchService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean.AnalyticThreadComplexTriggerBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean.AnalyticThreadComplexTriggerBean.TriggerType;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.data_import.HarvestControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean.MasterEnrichmentType;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.ProjectBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.SingleBeanQueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.SingleQueryComponent;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.FutureUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.SetOnce;
import com.ikanow.aleph2.data_model.utils.TimeUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils.MethodNamingHelper;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent;
import com.ikanow.aleph2.data_model.utils.FutureUtils.ManagementFuture;
import com.ikanow.aleph2.management_db.controllers.actors.BucketActionSupervisor;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage.BucketActionCollectedRepliesMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionRetryMessage;
import com.ikanow.aleph2.management_db.data_model.BucketMgmtMessage.BucketDeletionMessage;
import com.ikanow.aleph2.management_db.utils.ManagementDbErrorUtils;
import com.ikanow.aleph2.management_db.utils.MgmtCrudUtils;
import com.ikanow.aleph2.management_db.utils.MgmtCrudUtils.SuccessfulNodeType;

import fj.data.Validation;

import java.util.stream.Stream;

//TODO (ALEPH-19): ... handle the update poll messaging, needs an indexed "next poll" query, do a findAndMod from every thread every minute
//TODO (ALEPH-19): If I change a bucket again, need to cancel anything in the retry bin for that bucket (or if I re-created a deleted bucket)

/** CRUD service for Data Bucket with management proxy
 * @author acp
 */
public class DataBucketCrudService implements IManagementCrudService<DataBucketBean> {
	@SuppressWarnings("unused")
	private static final Logger _logger = LogManager.getLogger();	
	
	protected final IStorageService _storage_service;	
	protected final IManagementDbService _underlying_management_db;
	
	protected final SetOnce<ICrudService<DataBucketBean>> _underlying_data_bucket_db = new SetOnce<>();
	protected final SetOnce<ICrudService<DataBucketStatusBean>> _underlying_data_bucket_status_db = new SetOnce<>();
	protected final SetOnce<ICrudService<BucketActionRetryMessage>> _bucket_action_retry_store = new SetOnce<>();
	protected final SetOnce<ICrudService<BucketDeletionMessage>> _bucket_deletion_queue = new SetOnce<>();
	
	protected final ManagementDbActorContext _actor_context;
	protected final IServiceContext _service_context;
	
	/** Guice invoked constructor
	 */
	@Inject
	public DataBucketCrudService(final IServiceContext service_context, ManagementDbActorContext actor_context)
	{
		_underlying_management_db = service_context.getService(IManagementDbService.class, Optional.empty()).get();
				
		_storage_service = service_context.getStorageService();
		
		_actor_context = actor_context;
		_service_context = service_context;
		
		ModuleUtils.getAppInjector().thenRun(() -> initialize());					
	}
	
	/** Work around for Guice circular development issues
	 */
	protected void initialize() {
		_underlying_data_bucket_db.set(_underlying_management_db.getDataBucketStore());
		_underlying_data_bucket_status_db.set(_underlying_management_db.getDataBucketStatusStore());
		_bucket_action_retry_store.set(_underlying_management_db.getRetryStore(BucketActionRetryMessage.class));
		_bucket_deletion_queue.set(_underlying_management_db.getBucketDeletionQueue(BucketDeletionMessage.class));
		
		// Handle some simple optimization of the data bucket CRUD repo:
		Executors.newSingleThreadExecutor().submit(() -> {
			_underlying_data_bucket_db.get().optimizeQuery(Arrays.asList(
					BeanTemplateUtils.from(DataBucketBean.class).field(DataBucketBean::full_name)));
		});		
		
	}
	
	/** User constructor, for wrapping
	 */
	public DataBucketCrudService(final IServiceContext service_context,
			final IManagementDbService underlying_management_db, 
			final IStorageService storage_service,
			final ICrudService<DataBucketBean> underlying_data_bucket_db,
			final ICrudService<DataBucketStatusBean> underlying_data_bucket_status_db			
			)
	{
		_service_context = service_context;
		_underlying_management_db = underlying_management_db;
		_underlying_data_bucket_db.set(underlying_data_bucket_db);
		_underlying_data_bucket_status_db.set(underlying_data_bucket_status_db);
		_bucket_action_retry_store.set(_underlying_management_db.getRetryStore(BucketActionRetryMessage.class));
		_bucket_deletion_queue.set(_underlying_management_db.getBucketDeletionQueue(BucketDeletionMessage.class));		
		_actor_context = ManagementDbActorContext.get();		
		_storage_service = storage_service;
	}
		
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getFilteredRepo(java.lang.String, java.util.Optional, java.util.Optional)
	 */
	public IManagementCrudService<DataBucketBean> getFilteredRepo(
			final String authorization_fieldname,
			final Optional<AuthorizationBean> client_auth,
			final Optional<ProjectBean> project_auth) 
	{
		return new DataBucketCrudService(_service_context, _underlying_management_db.getFilteredDb(client_auth, project_auth), 
				_storage_service,
				_underlying_data_bucket_db.get().getFilteredRepo(authorization_fieldname, client_auth, project_auth),
				_underlying_data_bucket_status_db.get().getFilteredRepo(authorization_fieldname, client_auth, project_auth)
				);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#storeObject(java.lang.Object, boolean)
	 */
	public ManagementFuture<Supplier<Object>> storeObject(final DataBucketBean new_object, final boolean replace_if_present)
	{		
		try {			
			// New bucket vs update - get the old bucket (we'll do this non-concurrently at least for now)
			
			final Optional<DataBucketBean> old_bucket = Lambdas.get(() -> {
				try {
					if (replace_if_present && (null != new_object._id())) {
						return _underlying_data_bucket_db.get().getObjectById(new_object._id()).get();
					}
					else {
						return Optional.<DataBucketBean>empty();
					}
				}
				catch (Exception e) {
					throw new RuntimeException(e);
				}
			});

			// Validation (also generates a clone of the bucket with the data_locations written in)
			
			final Tuple2<DataBucketBean, Collection<BasicMessageBean>> validation_info = validateBucket(new_object, old_bucket, true, false);
			
			if (!validation_info._2().isEmpty() && validation_info._2().stream().anyMatch(m -> !m.success())) {
				return FutureUtils.createManagementFuture(
						FutureUtils.returnError(new RuntimeException("Bucket not valid, see management channels")),
						CompletableFuture.completedFuture(validation_info._2())
						);
			}
			return storeValidatedObject(validation_info._1(), old_bucket, validation_info._2(), replace_if_present);
		}
		catch (Exception e) {
			// This is a serious enough exception that we'll just leave here
			return FutureUtils.createManagementFuture(
					FutureUtils.returnError(e));			
		}
	}
	/** Worker function for storeObject
	 * @param new_object - the bucket to create
	 * @param old_bucket - the version of the bucket being overwritte, if an update
	 * @param validation_info - validation info to be presented to the user
	 * @param replace_if_present - update move
	 * @return - the user return value
	 * @throws Exception
	 */
	public ManagementFuture<Supplier<Object>> storeValidatedObject(
			final DataBucketBean new_object, final Optional<DataBucketBean> old_bucket, 
			final Collection<BasicMessageBean> validation_info, boolean replace_if_present) throws Exception
	{
		final MethodNamingHelper<DataBucketStatusBean> helper = BeanTemplateUtils.from(DataBucketStatusBean.class); 
			
		// Error if a bucket status doesn't exist - must create a bucket status before creating the bucket
		// (note the above validation ensures the bucket has an _id)
		// (obviously need to block here until we're sure..)
		
		final CompletableFuture<Optional<DataBucketStatusBean>> corresponding_status = 
				_underlying_data_bucket_status_db.get().getObjectById(new_object._id(), 
						Arrays.asList(helper.field(DataBucketStatusBean::_id), 
										helper.field(DataBucketStatusBean::node_affinity), 
										helper.field(DataBucketStatusBean::confirmed_master_enrichment_type), 
										helper.field(DataBucketStatusBean::confirmed_suspended), 
										helper.field(DataBucketStatusBean::confirmed_multi_node_enabled), 											
										helper.field(DataBucketStatusBean::suspended), 
										helper.field(DataBucketStatusBean::quarantined_until)), true);					
				
		if (!corresponding_status.get().isPresent()) {
			return FutureUtils.createManagementFuture(
					FutureUtils.returnError(new RuntimeException(
							ErrorUtils.get(ManagementDbErrorUtils.BUCKET_CANNOT_BE_CREATED_WITHOUT_BUCKET_STATUS, new_object.full_name()))),
					CompletableFuture.completedFuture(Collections.emptyList())
					);				
		}
		
		// Some fields like multi-node, you can only change if the bucket status is set to suspended, to make
		// the control logic easy
		old_bucket.ifPresent(ob -> {
			validation_info.addAll(checkForInactiveOnlyUpdates(new_object, ob, corresponding_status.join().get()));
			// (corresponding_status present and completed because of above check) 
		});
		if (!validation_info.isEmpty() && validation_info.stream().anyMatch(m -> !m.success())) {
			return FutureUtils.createManagementFuture(
					FutureUtils.returnError(new RuntimeException("Bucket not valid, see management channels")),
					CompletableFuture.completedFuture(validation_info)
					);
		}
		
		// Made it this far, try to set the next_poll_time in the status object
		if ( null != new_object.poll_frequency()) {
			//get the next poll time
			final Date next_poll_time = TimeUtils.getSchedule(new_object.poll_frequency(), Optional.of(new Date())).success();
			//update the status
			_underlying_data_bucket_status_db.get().updateObjectById(new_object._id(), CrudUtils.update(DataBucketStatusBean.class).set(DataBucketStatusBean::next_poll_date, next_poll_time));
		}
		
		// Create the directories
		
		try {
			createFilePaths(new_object, _storage_service);
		}
		catch (Exception e) { // Error creating directory, haven't created object yet so just back out now
			return FutureUtils.createManagementFuture(
					FutureUtils.returnError(e));			
		}
		
		// OK if the bucket is validated we can store it (and create a status object)
				
		final CompletableFuture<Supplier<Object>> ret_val = _underlying_data_bucket_db.get().storeObject(new_object, replace_if_present);

		// Get the status and then decide whether to broadcast out the new/update message
		
		final boolean is_suspended = DataBucketStatusCrudService.bucketIsSuspended(corresponding_status.get().get());
		final CompletableFuture<Collection<BasicMessageBean>> mgmt_results = old_bucket.isPresent()
				? requestUpdatedBucket(new_object, old_bucket.get(), corresponding_status.get().get(), _actor_context, _underlying_data_bucket_status_db.get(), _bucket_action_retry_store.get())
				: requestNewBucket(new_object, is_suspended, _underlying_data_bucket_status_db.get(), _actor_context);
			
		// Update the status depending on the results of the management channels
				
		return FutureUtils.createManagementFuture(ret_val,
				MgmtCrudUtils.handleUpdatingStatus(new_object, corresponding_status.get().get(), is_suspended, mgmt_results, _underlying_data_bucket_status_db.get())					
									.thenApply(msgs -> Stream.concat(msgs.stream(), validation_info.stream()).collect(Collectors.toList())));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#storeObject(java.lang.Object)
	 */
	public ManagementFuture<Supplier<Object>> storeObject(final DataBucketBean new_object) {
		return this.storeObject(new_object, false);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#storeObjects(java.util.List, boolean)
	 */
	public ManagementFuture<Tuple2<Supplier<List<Object>>, Supplier<Long>>> storeObjects(final List<DataBucketBean> new_objects, final boolean continue_on_error) {
		throw new RuntimeException("This method is not supported, call storeObject on each object separately");
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#storeObjects(java.util.List)
	 */
	public ManagementFuture<Tuple2<Supplier<List<Object>>, Supplier<Long>>> storeObjects(final List<DataBucketBean> new_objects) {
		return this.storeObjects(new_objects, false);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#optimizeQuery(java.util.List)
	 */
	public ManagementFuture<Boolean> optimizeQuery(final List<String> ordered_field_list) {
		return FutureUtils.createManagementFuture(_underlying_data_bucket_db.get().optimizeQuery(ordered_field_list));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deregisterOptimizedQuery(java.util.List)
	 */
	public boolean deregisterOptimizedQuery(final List<String> ordered_field_list) {
		return _underlying_data_bucket_db.get().deregisterOptimizedQuery(ordered_field_list);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	public ManagementFuture<Optional<DataBucketBean>> getObjectBySpec(final QueryComponent<DataBucketBean> unique_spec) {
		return FutureUtils.createManagementFuture(_underlying_data_bucket_db.get().getObjectBySpec(unique_spec));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.List, boolean)
	 */
	public ManagementFuture<Optional<DataBucketBean>> getObjectBySpec(
			final QueryComponent<DataBucketBean> unique_spec,
			final List<String> field_list, final boolean include) {
		return FutureUtils.createManagementFuture(_underlying_data_bucket_db.get().getObjectBySpec(unique_spec, field_list, include));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectById(java.lang.Object)
	 */
	public ManagementFuture<Optional<DataBucketBean>> getObjectById(final Object id) {
		return FutureUtils.createManagementFuture(_underlying_data_bucket_db.get().getObjectById(id));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectById(java.lang.Object, java.util.List, boolean)
	 */
	public ManagementFuture<Optional<DataBucketBean>> getObjectById(final Object id,
			final List<String> field_list, final boolean include) {
		return FutureUtils.createManagementFuture(_underlying_data_bucket_db.get().getObjectById(id, field_list, include));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	public ManagementFuture<ICrudService.Cursor<DataBucketBean>> getObjectsBySpec(
			final QueryComponent<DataBucketBean> spec)
	{
		return FutureUtils.createManagementFuture(_underlying_data_bucket_db.get().getObjectsBySpec(spec));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.List, boolean)
	 */
	public ManagementFuture<ICrudService.Cursor<DataBucketBean>> getObjectsBySpec(
			final QueryComponent<DataBucketBean> spec, final List<String> field_list,
			final boolean include)
	{
		return FutureUtils.createManagementFuture(_underlying_data_bucket_db.get().getObjectsBySpec(spec, field_list, include));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#countObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	public ManagementFuture<Long> countObjectsBySpec(final QueryComponent<DataBucketBean> spec) {
		return FutureUtils.createManagementFuture(_underlying_data_bucket_db.get().countObjectsBySpec(spec));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#countObjects()
	 */
	public ManagementFuture<Long> countObjects() {
		return FutureUtils.createManagementFuture(_underlying_data_bucket_db.get().countObjects());
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deleteObjectById(java.lang.Object)
	 */
	public ManagementFuture<Boolean> deleteObjectById(final Object id) {		
		final CompletableFuture<Optional<DataBucketBean>> result = _underlying_data_bucket_db.get().getObjectById(id);		
		try {
			if (result.get().isPresent()) {
				return this.deleteBucket(result.get().get());
			}
			else {
				return FutureUtils.createManagementFuture(CompletableFuture.completedFuture(false));
			}
		}
		catch (Exception e) {
			// This is a serious enough exception that we'll just leave here
			return FutureUtils.createManagementFuture(
					FutureUtils.returnError(e));			
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deleteObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	public ManagementFuture<Boolean> deleteObjectBySpec(final QueryComponent<DataBucketBean> unique_spec) {
		final CompletableFuture<Optional<DataBucketBean>> result = _underlying_data_bucket_db.get().getObjectBySpec(unique_spec);
		try {
			if (result.get().isPresent()) {
				return this.deleteBucket(result.get().get());
			}
			else {
				return FutureUtils.createManagementFuture(CompletableFuture.completedFuture(false));
			}
		}
		catch (Exception e) {
			// This is a serious enough exception that we'll just leave here
			return FutureUtils.createManagementFuture(
					FutureUtils.returnError(e));			
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deleteObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	public ManagementFuture<Long> deleteObjectsBySpec(final QueryComponent<DataBucketBean> spec) {
		
		// Need to do these one by one:

		final CompletableFuture<Cursor<DataBucketBean>> to_delete = _underlying_data_bucket_db.get().getObjectsBySpec(spec);
		
		try {
			return MgmtCrudUtils.applyCrudPredicate(to_delete, this::deleteBucket)._1();
		}
		catch (Exception e) {
			// This is a serious enough exception that we'll just leave here
			return FutureUtils.createManagementFuture(
					FutureUtils.returnError(e));
		}
	}

	/** Internal function to delete the bucket, while notifying active users of the bucket
	 * @param to_delete
	 * @return a management future containing the result 
	 */
	private ManagementFuture<Boolean> deleteBucket(final DataBucketBean to_delete) {
		try {
			// Also delete the file paths (currently, just add ".deleted" to top level path) 
			deleteFilePath(to_delete, _storage_service);
			
			// Add to the deletion queue (do it before trying to delete the bucket in case this bucket deletion fails - if so then delete queue will retry every hour)
			final Date to_delete_date = Timestamp.from(Instant.now().plus(1L, ChronoUnit.MINUTES));
			final CompletableFuture<Supplier<Object>> enqueue_delete = this._bucket_deletion_queue.get().storeObject(new BucketDeletionMessage(to_delete, to_delete_date, false));
			
			final CompletableFuture<Boolean> delete_reply = enqueue_delete
																.thenCompose(__ -> _underlying_data_bucket_db.get().deleteObjectById(to_delete._id()));

			return FutureUtils.denestManagementFuture(delete_reply
				.thenCompose(del_reply -> {		
					if (!del_reply) { // Didn't find an object to delete, just return that information to the user
						return CompletableFuture.completedFuture(Optional.empty());
					}
					else { //Get the status and delete it 
						
						final CompletableFuture<Optional<DataBucketStatusBean>> future_status_bean =
								_underlying_data_bucket_status_db.get().updateAndReturnObjectBySpec(
										CrudUtils.allOf(DataBucketStatusBean.class).when(DataBucketStatusBean::_id, to_delete._id()),
										Optional.empty(), CrudUtils.update(DataBucketStatusBean.class).deleteObject(), Optional.of(true), Collections.emptyList(), false);
						
						return future_status_bean;
					}
				})
				.thenApply(status_bean -> {
					if (!status_bean.isPresent()) {
						return FutureUtils.createManagementFuture(delete_reply);
					}
					else {
						final BucketActionMessage.DeleteBucketActionMessage delete_message = new
								BucketActionMessage.DeleteBucketActionMessage(to_delete, 
										new HashSet<String>(
												Optional.ofNullable(
														status_bean.isPresent() ? status_bean.get().node_affinity() : null)
												.orElse(Collections.emptyList())
												));
						
						final CompletableFuture<Collection<BasicMessageBean>> management_results =
							MgmtCrudUtils.applyRetriableManagementOperation(to_delete, 
									_actor_context, _bucket_action_retry_store.get(), 
									delete_message, source -> {
										return new BucketActionMessage.DeleteBucketActionMessage(
												delete_message.bucket(),
												new HashSet<String>(Arrays.asList(source)));	
									});
						
						// Convert BucketActionCollectedRepliesMessage into a management side-channel:
						return FutureUtils.createManagementFuture(delete_reply, management_results);											
					}
				}));
		}
		catch (Exception e) {
			// This is a serious enough exception that we'll just leave here
			return FutureUtils.createManagementFuture(
					FutureUtils.returnError(e));			
		}
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deleteDatastore()
	 */
	public ManagementFuture<Boolean> deleteDatastore() {
		throw new RuntimeException("This method is not supported");
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getRawService()
	 */
	public IManagementCrudService<JsonNode> getRawService() {
		throw new RuntimeException("DataBucketCrudService.getRawService not supported");
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getSearchService()
	 */
	public Optional<IBasicSearchService<DataBucketBean>> getSearchService() {
		return _underlying_data_bucket_db.get().getSearchService();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getUnderlyingPlatformDriver(java.lang.Class, java.util.Optional)
	 */
	@SuppressWarnings("unchecked")
	public <T> Optional<T> getUnderlyingPlatformDriver(final Class<T> driver_class,
			final Optional<String> driver_options)
	{
		if (driver_class == ICrudService.class) {
			return (Optional<T>) Optional.of(_underlying_data_bucket_db.get());
		}
		else {
			throw new RuntimeException("DataBucketCrudService.getUnderlyingPlatformDriver not supported");
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#updateObjectById(java.lang.Object, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent)
	 */
	@Override
	public 
	ManagementFuture<Boolean> updateObjectById(
			final Object id, final UpdateComponent<DataBucketBean> update) {
		throw new RuntimeException("DataBucketCrudService.update* not supported (use storeObject with replace_if_present: true)");
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#updateObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.Optional, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent)
	 */
	@Override
	public ManagementFuture<Boolean> updateObjectBySpec(
			final QueryComponent<DataBucketBean> unique_spec,
			final Optional<Boolean> upsert,
			final UpdateComponent<DataBucketBean> update) {
		throw new RuntimeException("DataBucketCrudService.update* not supported (use storeObject with replace_if_present: true)");
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#updateObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.Optional, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent)
	 */
	@Override
	public ManagementFuture<Long> updateObjectsBySpec(
			final QueryComponent<DataBucketBean> spec,
			final Optional<Boolean> upsert,
			final UpdateComponent<DataBucketBean> update) {
		throw new RuntimeException("DataBucketCrudService.update* not supported (use storeObject with replace_if_present: true)");
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#updateAndReturnObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.Optional, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent, java.util.Optional, java.util.List, boolean)
	 */
	@Override
	public ManagementFuture<Optional<DataBucketBean>> updateAndReturnObjectBySpec(
			final QueryComponent<DataBucketBean> unique_spec,
			final Optional<Boolean> upsert,
			final UpdateComponent<DataBucketBean> update,
			final Optional<Boolean> before_updated, List<String> field_list,
			final boolean include) {
		throw new RuntimeException("DataBucketCrudService.update* not supported (use storeObject with replace_if_present: true)");
	}
	
	/////////////////////////////////////////////////////////////////////////////////////
	
	// UTILITIES
	
	/** Standalone bucket validation
	 * @param bucket
	 * @return
	 */
	public Tuple2<DataBucketBean, Collection<BasicMessageBean>> validateBucket(final DataBucketBean bucket, final boolean allow_system_names) {
		try {
			return validateBucket(bucket, Optional.empty(), false, allow_system_names);
		} catch (InterruptedException | ExecutionException e) {
			return Tuples._2T(bucket, Arrays.asList(ErrorUtils.buildErrorMessage("DataBucketCrudService", "validateBucket", ErrorUtils.getLongForm("Unknown error = {0}" , e))));
		}
	}
	
	/** Validates whether the new or updated bucket is valid: both in terms of authorization and in terms of format
	 * @param bucket
	 * @return
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	protected Tuple2<DataBucketBean, Collection<BasicMessageBean>> validateBucket(final DataBucketBean bucket, final Optional<DataBucketBean> old_version, boolean do_full_checks, final boolean allow_system_names) throws InterruptedException, ExecutionException {
		
		// (will live with this being mutable)
		final LinkedList<BasicMessageBean> errors = new LinkedList<BasicMessageBean>();

		final JsonNode bucket_json = BeanTemplateUtils.toJson(bucket);
		
		/////////////////

		// PHASE 1
		
		// Check for missing fields
		
		ManagementDbErrorUtils.NEW_BUCKET_ERROR_MAP.keySet().stream()
			.filter(s -> !bucket_json.has(s) || (bucket_json.get(s).isTextual() && bucket_json.get(s).asText().isEmpty()))
			.forEach(s -> 
				errors.add(MgmtCrudUtils.createValidationError(
						ErrorUtils.get(ManagementDbErrorUtils.NEW_BUCKET_ERROR_MAP.get(s), Optional.ofNullable(bucket.full_name()).orElse("(unknown)")))));
		
		// We have a full name if we're here, so no check for uniqueness
		
		// Check for some bucket path restrictions
		if (null != bucket.full_name()) {
			if (!bucketPathFormatValidationCheck(bucket.full_name())) {
				errors.add(MgmtCrudUtils.createValidationError(
						ErrorUtils.get(ManagementDbErrorUtils.BUCKET_FULL_NAME_FORMAT_ERROR, Optional.ofNullable(bucket.full_name()).orElse("(unknown)"))));
				
				return Tuples._2T(bucket, errors); // (this is catastrophic obviously)			
			}
			
			if (!old_version.isPresent()) { // (create not update)
				if (do_full_checks) {
					if (this._underlying_data_bucket_db.get().countObjectsBySpec(CrudUtils.allOf(DataBucketBean.class)
																		.when(DataBucketBean::full_name, bucket.full_name())
																	).get() > 0)
					{			
						errors.add(MgmtCrudUtils.createValidationError(
								ErrorUtils.get(ManagementDbErrorUtils.BUCKET_FULL_NAME_UNIQUENESS, Optional.ofNullable(bucket.full_name()).orElse("(unknown)"))));
						
						return Tuples._2T(bucket, errors); // (this is catastrophic obviously)
					}
				}
			}
		}		
		else return Tuples._2T(bucket, errors); // (this is catastrophic obviously)

		// Some static validation moved into a separate function for testability
		
		errors.addAll(staticValidation(bucket, allow_system_names));		

		// OK before I do any more stateful checking, going to stop if we have logic errors first 
		
		if (!errors.isEmpty()) {
			return Tuples._2T(bucket, errors);
		}
		
		/////////////////
		
		// PHASE 2
				
		//TODO (ALEPH-19): multi buckets - authorization; other - authorization
		
		if (do_full_checks) {
		
			final CompletableFuture<Collection<BasicMessageBean>> bucket_path_errors_future = validateOtherBucketsInPathChain(bucket);
			
			errors.addAll(bucket_path_errors_future.join());
			
			// OK before I do any more stateful checking, going to stop if we have logic errors first 
			
			if (!errors.isEmpty()) {
				return Tuples._2T(bucket, errors);
			}
		}
		
		/////////////////
		
		// PHASE 3
		
		// Finally Check whether I am allowed to update the various fields if old_version.isPresent()
		
		if (old_version.isPresent()) {
			final DataBucketBean old_bucket = old_version.get();
			if (!bucket.full_name().equals(old_bucket.full_name())) {
				errors.add(MgmtCrudUtils.createValidationError(ErrorUtils.get(ManagementDbErrorUtils.BUCKET_UPDATE_FULLNAME_CHANGED, bucket.full_name(), old_bucket.full_name())));
			}
			if (!bucket.owner_id().equals(old_bucket.owner_id())) {
				errors.add(MgmtCrudUtils.createValidationError(ErrorUtils.get(ManagementDbErrorUtils.BUCKET_UPDATE_OWNERID_CHANGED, bucket.full_name(), old_bucket.owner_id())));
			}
		}
		
		/////////////////
		
		// PHASE 4 - DATA SCHEMA NOT MESSAGES AT THIS POINT CAN BE INFO, YOU NEED TO CHECK THE SUCCESS()

		Tuple2<Map<String, String>, List<BasicMessageBean>> schema_validation = validateSchema(bucket, _service_context);
		
		errors.addAll(schema_validation._2());
		
		return Tuples._2T(
				BeanTemplateUtils.clone(bucket).with(DataBucketBean::data_locations, schema_validation._1()).done(), 
				errors);
	}
	
	
	/** Static / simple bucket validation utility method
	 * @param bucket - the bucket to test
	 * @return - a list of errors
	 */
	protected static final LinkedList<BasicMessageBean> staticValidation(final DataBucketBean bucket, final boolean allow_system_names) {
		LinkedList<BasicMessageBean> errors = new LinkedList<>();
		
		// More full_name checks
		
		if (!allow_system_names) {
			if (bucket.full_name().startsWith("/aleph2_")) {
				errors.add(MgmtCrudUtils.createValidationError(
						ErrorUtils.get(ManagementDbErrorUtils.BUCKET_FULL_NAME_RESERVED_ERROR, Optional.ofNullable(bucket.full_name()).orElse("(unknown)"))));				
			}
		}
				
		// More complex missing field checks
		
		// - if has enrichment then must have harvest_technology_name_or_id (1) - REMOVED THIS .. eg can upload data/copy directly into Kafka/file system 
		// - if has harvest_technology_name_or_id then must have harvest_configs (2)
		// - if has enrichment then must have master_enrichment_type (3)
		// - if master_enrichment_type == batch/both then must have either batch_enrichment_configs or batch_enrichment_topology (4)
		// - if master_enrichment_type == streaming/both then must have either streaming_enrichment_configs or streaming_enrichment_topology (5)
		//(- for now ... don't support streaming_and_batch, the current enrichment logic doesn't support it (X1))

		//(1, 3, 4, 5)
		if (((null != bucket.batch_enrichment_configs()) && !bucket.batch_enrichment_configs().isEmpty())
				|| ((null != bucket.streaming_enrichment_configs()) && !bucket.streaming_enrichment_configs().isEmpty())
				|| (null != bucket.batch_enrichment_topology())
				|| (null != bucket.streaming_enrichment_topology()))
		{
			if (null == bucket.master_enrichment_type()) { // (3)
				errors.add(MgmtCrudUtils.createValidationError(ErrorUtils.get(ManagementDbErrorUtils.ENRICHMENT_BUT_NO_MASTER_ENRICHMENT_TYPE, bucket.full_name())));				
			}
			else if (DataBucketBean.MasterEnrichmentType.streaming_and_batch == bucket.master_enrichment_type()) { //(X1)
				errors.add(MgmtCrudUtils.createValidationError(ErrorUtils.get(ManagementDbErrorUtils.STREAMING_AND_BATCH_NOT_SUPPORTED, bucket.full_name())));
			}
			else {
				// (4)
				if ((DataBucketBean.MasterEnrichmentType.batch == bucket.master_enrichment_type())
					 || (DataBucketBean.MasterEnrichmentType.streaming_and_batch == bucket.master_enrichment_type()))
				{
					if ((null == bucket.batch_enrichment_topology()) && 
							((null == bucket.batch_enrichment_configs()) || bucket.batch_enrichment_configs().isEmpty()))
					{
						errors.add(MgmtCrudUtils.createValidationError(ErrorUtils.get(ManagementDbErrorUtils.BATCH_ENRICHMENT_NO_CONFIGS, bucket.full_name())));
					}
				}
				// (5)
				if ((DataBucketBean.MasterEnrichmentType.streaming == bucket.master_enrichment_type())
						 || (DataBucketBean.MasterEnrichmentType.streaming_and_batch == bucket.master_enrichment_type()))
				{
					if ((null == bucket.streaming_enrichment_topology()) && 
							((null == bucket.streaming_enrichment_configs()) || bucket.streaming_enrichment_configs().isEmpty()))
					{
						errors.add(MgmtCrudUtils.createValidationError(ErrorUtils.get(ManagementDbErrorUtils.STREAMING_ENRICHMENT_NO_CONFIGS, bucket.full_name())));
					}
				}
			}
		}
		// (2)
		if ((null != bucket.harvest_technology_name_or_id()) &&
				((null == bucket.harvest_configs()) || bucket.harvest_configs().isEmpty()))
		{			
			errors.add(MgmtCrudUtils.createValidationError(ErrorUtils.get(ManagementDbErrorUtils.HARVEST_BUT_NO_HARVEST_CONFIG, bucket.full_name())));
		}
		
		// Embedded object field rules
		
		final Consumer<Tuple2<String, List<String>>> list_test = list -> {
			if (null != list._2()) for (String s: list._2()) {
				if ((s == null) || s.isEmpty()) {
					errors.add(MgmtCrudUtils.createValidationError(ErrorUtils.get(ManagementDbErrorUtils.FIELD_MUST_NOT_HAVE_ZERO_LENGTH, bucket.full_name(), list._1())));
				}
			}
			
		};
		
		// Lists mustn't have zero-length elements
		
		// (NOTE: these strings are just for error messing so not critical, which is we we're not using MethodNamingHelper)
		
		// (6)
		if ((null != bucket.harvest_technology_name_or_id()) && (null != bucket.harvest_configs())) {
			for (int i = 0; i < bucket.harvest_configs().size(); ++i) {
				final HarvestControlMetadataBean hmeta = bucket.harvest_configs().get(i);
				if ((null == hmeta.entry_point()) && (null == hmeta.module_name_or_id())) { // if either of these are set, don't need library names
					list_test.accept(Tuples._2T("harvest_configs" + Integer.toString(i) + ".library_ids_or_names", hmeta.library_names_or_ids()));
				}
			}
		}

		// (7) 
		BiConsumer<Tuple2<String, EnrichmentControlMetadataBean>, Boolean> enrichment_test = (emeta, allowed_empty_list) -> {
			if (Optional.ofNullable(emeta._2().enabled()).orElse(true)) {
				if (!allowed_empty_list)
					if ((null == emeta._2().entry_point()) && (null == emeta._2().module_name_or_id()) //if either of these are set then dont' need library names
							&& 
						((null == emeta._2().library_names_or_ids()) || emeta._2().library_names_or_ids().isEmpty()))
					{
						errors.add(MgmtCrudUtils.createValidationError(ErrorUtils.get(ManagementDbErrorUtils.INVALID_ENRICHMENT_CONFIG_ELEMENTS_NO_LIBS, bucket.full_name(), emeta._1())));				
					}
			}
			list_test.accept(Tuples._2T(emeta._1() + ".library_ids_or_names", emeta._2().library_names_or_ids()));
			list_test.accept(Tuples._2T(emeta._1() + ".dependencies", emeta._2().dependencies()));
		};		
		if (null != bucket.batch_enrichment_topology()) {
			enrichment_test.accept(Tuples._2T("batch_enrichment_topology", bucket.batch_enrichment_topology()), true);
		}
		if (null != bucket.batch_enrichment_configs()) {
			for (int i = 0; i < bucket.batch_enrichment_configs().size(); ++i) {
				final EnrichmentControlMetadataBean emeta = bucket.batch_enrichment_configs().get(i);
				enrichment_test.accept(Tuples._2T("batch_enrichment_configs." + Integer.toString(i), emeta), false);
			}
		}
		if (null != bucket.streaming_enrichment_topology()) {
			enrichment_test.accept(Tuples._2T("streaming_enrichment_topology", bucket.streaming_enrichment_topology()), true);
		}
		if (null != bucket.streaming_enrichment_configs()) {
			for (int i = 0; i < bucket.streaming_enrichment_configs().size(); ++i) {
				final EnrichmentControlMetadataBean emeta = bucket.streaming_enrichment_configs().get(i);
				enrichment_test.accept(Tuples._2T("streaming_enrichment_configs." + Integer.toString(i), emeta), false);
			}
		}
		
		// Multi-buckets logic 
		
		// - if a multi bucket than cannot have any of: enrichment or harvest (8)
		// - multi-buckets cannot be nested (TODO: ALEPH-19, leave this one for later)		
		
		//(8)
		if ((null != bucket.multi_bucket_children()) && !bucket.multi_bucket_children().isEmpty()) {
			if (null != bucket.harvest_technology_name_or_id()) {
				errors.add(MgmtCrudUtils.createValidationError(ErrorUtils.get(ManagementDbErrorUtils.MULTI_BUCKET_CANNOT_HARVEST, bucket.full_name())));				
			}
		}
				
		/////////////////
		
		// PHASE 1b: ANALYIC VALIDATION
				
		errors.addAll(validateAnalyticBucket(bucket).stream().map(MgmtCrudUtils::createValidationError).collect(Collectors.toList()));
				
		return errors;
	}
	
	/** Checks active buckets for changes that will cause problems unless the bucket is suspended first (currently: only multi_node_enabled)
	 * @param new_bucket - the proposed new bucket
	 * @param old_bucket - the existing bucket (note may not have reflected changes based on errors in the management info)
	 * @param corresponding_status - the existing status (note that in V1/V2 sync scenarios may have been changed at the same time as the bucket)
	 * @return a collection of errors and warnings
	 */
	protected static Collection<BasicMessageBean> checkForInactiveOnlyUpdates(
			final DataBucketBean new_bucket,
			final DataBucketBean old_bucket,
			final DataBucketStatusBean corresponding_status)
	{
		final LinkedList<BasicMessageBean> errors = new LinkedList<>();		
		if (!Optional.ofNullable(corresponding_status.confirmed_suspended())
						.orElse(Optional.ofNullable(corresponding_status.suspended())
						.orElse(false)))
		{
			if (new_bucket.multi_node_enabled() != corresponding_status.confirmed_multi_node_enabled()) {
				errors.add(MgmtCrudUtils.createValidationError(
						ErrorUtils.get(ManagementDbErrorUtils.BUCKET_UPDATE_ILLEGAL_FIELD_CHANGED_ACTIVE, 
								new_bucket.full_name(), "multi_node_enabled")));				
			}
			if (new_bucket.master_enrichment_type() != corresponding_status.confirmed_master_enrichment_type()) {
				errors.add(MgmtCrudUtils.createValidationError(
						ErrorUtils.get(ManagementDbErrorUtils.BUCKET_UPDATE_ILLEGAL_FIELD_CHANGED_ACTIVE, 
								new_bucket.full_name(), "master_enrichment_type")));				
			}
		}
		return errors;
	}

	/** Validates that all enabled schema point to existing services and are well formed
	 * @param bucket
	 * @param service_context
	 * @return
	 */
	protected static Tuple2<Map<String, String>, List<BasicMessageBean>> validateSchema(final DataBucketBean bucket, final IServiceContext service_context) {
		final List<BasicMessageBean> errors = new LinkedList<>();
		final Map<String, String> data_locations = new LinkedHashMap<>(); // icky MUTABLE code)
		
		// Generic data schema:
		errors.addAll(
				validateBucketTimes(bucket).stream()
					.map(s -> MgmtCrudUtils.createValidationError(s)).collect(Collectors.toList()));
		
		// Specific data schema
		if (null != bucket.data_schema()) {
			// Columnar
			if ((null != bucket.data_schema().columnar_schema()) && Optional.ofNullable(bucket.data_schema().columnar_schema().enabled()).orElse(true))
			{
				errors.addAll(service_context.getService(IColumnarService.class, Optional.ofNullable(bucket.data_schema().columnar_schema().service_name()))
								.map(s -> s.validateSchema(bucket.data_schema().columnar_schema(), bucket))
								.map(s -> { 
									if ((null != s._1()) && !s._1().isEmpty()) data_locations.put("columnar_schema", s._1());
									return s._2(); 
								})
								.orElse(Arrays.asList(MgmtCrudUtils.createValidationError(
										ErrorUtils.get(ManagementDbErrorUtils.SCHEMA_ENABLED_BUT_SERVICE_NOT_PRESENT, bucket.full_name(), "columnar_schema")))));
			}
			// Document
			if ((null != bucket.data_schema().document_schema()) && Optional.ofNullable(bucket.data_schema().document_schema().enabled()).orElse(true))
			{
				errors.addAll(service_context.getService(IDocumentService.class, Optional.ofNullable(bucket.data_schema().document_schema().service_name()))
								.map(s -> s.validateSchema(bucket.data_schema().document_schema(), bucket))
								.map(s -> { 
									if ((null != s._1()) && !s._1().isEmpty()) data_locations.put("document_schema", s._1());
									return s._2(); 
								})
								.orElse(Arrays.asList(MgmtCrudUtils.createValidationError(
										ErrorUtils.get(ManagementDbErrorUtils.SCHEMA_ENABLED_BUT_SERVICE_NOT_PRESENT, bucket.full_name(), "document_schema")))));
			}
			// Search Index
			if ((null != bucket.data_schema().search_index_schema()) && Optional.ofNullable(bucket.data_schema().search_index_schema().enabled()).orElse(true))
			{
				errors.addAll(service_context.getService(ISearchIndexService.class, Optional.ofNullable(bucket.data_schema().search_index_schema().service_name()))
								.map(s -> s.validateSchema(bucket.data_schema().search_index_schema(), bucket))
								.map(s -> { 
									if ((null != s._1()) && !s._1().isEmpty()) data_locations.put("search_index_schema", s._1());
									return s._2(); 
								})
								.orElse(Arrays.asList(MgmtCrudUtils.createValidationError(
										ErrorUtils.get(ManagementDbErrorUtils.SCHEMA_ENABLED_BUT_SERVICE_NOT_PRESENT, bucket.full_name(), "search_index_schema")))));
			}
			// Storage
			if ((null != bucket.data_schema().storage_schema()) && Optional.ofNullable(bucket.data_schema().storage_schema().enabled()).orElse(true))
			{
				errors.addAll(service_context.getService(IStorageService.class, Optional.ofNullable(bucket.data_schema().storage_schema().service_name()))
								.map(s -> s.validateSchema(bucket.data_schema().storage_schema(), bucket))
								.map(s -> { 
									if ((null != s._1()) && !s._1().isEmpty()) data_locations.put("storage_schema", s._1());
									return s._2(); 
								})
								.orElse(Arrays.asList(MgmtCrudUtils.createValidationError(
										ErrorUtils.get(ManagementDbErrorUtils.SCHEMA_ENABLED_BUT_SERVICE_NOT_PRESENT, bucket.full_name(), "storage_schema")))));
			}
			if ((null != bucket.data_schema().temporal_schema()) && Optional.ofNullable(bucket.data_schema().temporal_schema().enabled()).orElse(true))
			{
				errors.addAll(service_context.getService(ITemporalService.class, Optional.ofNullable(bucket.data_schema().temporal_schema().service_name()))
								.map(s -> s.validateSchema(bucket.data_schema().temporal_schema(), bucket))
								.map(s -> { 
									if ((null != s._1()) && !s._1().isEmpty()) data_locations.put("temporal_schema", s._1());
									return s._2(); 
								})
								.orElse(Arrays.asList(MgmtCrudUtils.createValidationError(
										ErrorUtils.get(ManagementDbErrorUtils.SCHEMA_ENABLED_BUT_SERVICE_NOT_PRESENT, bucket.full_name(), "temporal_schema")))));
			}
			//TODO (ALEPH-19) Data warehouse schema, graph schame, geospatial schema
			if (null != bucket.data_schema().geospatial_schema())
			{
				errors.add(MgmtCrudUtils.createValidationError(
						ErrorUtils.get(ManagementDbErrorUtils.SCHEMA_ENABLED_BUT_SERVICE_NOT_PRESENT, bucket.full_name(), "geospatial_schema")));
			}
			if (null != bucket.data_schema().graph_schema())
			{
				errors.add(MgmtCrudUtils.createValidationError(
						ErrorUtils.get(ManagementDbErrorUtils.SCHEMA_ENABLED_BUT_SERVICE_NOT_PRESENT, bucket.full_name(), "graph_schema")));
			}
			if (null != bucket.data_schema().data_warehouse_schema())
			{
				errors.add(MgmtCrudUtils.createValidationError(
						ErrorUtils.get(ManagementDbErrorUtils.SCHEMA_ENABLED_BUT_SERVICE_NOT_PRESENT, bucket.full_name(), "data_warehouse_schema")));
			}
		}
		return Tuples._2T(data_locations, errors);
	}
	
	/** A utility routine to check whether the bucket is allowed to be in this location
	 * @param bucket - the bucket to validate
	 * @return a future containing validation errors based on the path
	 */
	private CompletableFuture<Collection<BasicMessageBean>> validateOtherBucketsInPathChain(final DataBucketBean bucket) {
		final MethodNamingHelper<DataBucketBean> helper = BeanTemplateUtils.from(DataBucketBean.class);
		final String bucket_full_name = normalizeBucketPath(bucket.full_name(), true);

		return this._underlying_data_bucket_db.get().getObjectsBySpec(getPathQuery(bucket_full_name), 
				Arrays.asList(helper.field(DataBucketBean::full_name)), true)
				.thenApply(cursor -> {
					return StreamSupport.stream(cursor.spliterator(), false)
						.filter(b -> (null == b.full_name()))
						.<BasicMessageBean>map(b -> {
							final String norm_name = normalizeBucketPath(b.full_name(), true);							
							if (norm_name.startsWith(bucket_full_name)) {
								//TODO (ALEPH-19) call out other function, create BasicMessageBean error if not
								return null;
							}
							else if (norm_name.startsWith(bucket_full_name)) {								
								//TODO (ALEPH-19) call out other function, create BasicMessageBean error if not
								return null;
							}
							else { // we're good
								return null;
							}
						})								
						.filter(err -> err != null)
						.collect(Collectors.toList());
				});
	}
	
	/** Ensures that the bucket.full_name starts and ends with /s
	 * @param bucket_full_name
	 * @param ending_with_separator - for path operations we want to treat the bucket as ending with /, for the DB it should be /path/.../to ie not ending with / 
	 * @return
	 */
	public static String normalizeBucketPath(final String bucket_full_name, boolean ending_with_separator) {
		return Optional.of(bucket_full_name)
					.map(b -> {
						if (ending_with_separator) return b.endsWith("/") ? b : (b + "/");
						else return b.endsWith("/") ? b.substring(0, b.length() - 1) : b;
					})
					.map(b -> b.startsWith("/") ? b : ("/" + b))
					.get();
	}
	
	
	/** Creates the somewhat complex query that finds all buckets before or after this one on the path
	 * @param normalized_bucket_full_name
	 * @return the query to use in "validateOtherBucketsInPathChain"
	 */
	protected static QueryComponent<DataBucketBean> getPathQuery(final String normalized_bucket_full_name) {		
		final Matcher m = Pattern.compile("(/(?:[^/]*/)*)([^/]*/)").matcher(normalized_bucket_full_name);
		// This must match by construction of full_name above
		m.matches();
		
		final char first_char_of_final_path_inc = (char)(m.group(2).charAt(0) + 1);
		
		// Anything _before_ me in the chain
		
		final String[] parts = normalized_bucket_full_name.substring(1).split("[/]");
		
		// Going old school, it's getting late (lol@me):
		SingleBeanQueryComponent<DataBucketBean> component1 = CrudUtils.anyOf(DataBucketBean.class);
		for (int i = 0; i < parts.length; ++i) {
			String path = parts[0];
			for (int j = 0; j < i; ++j) {
				path += "/" + parts[j];
			}
			component1 = component1.when(DataBucketBean::full_name, path);
			component1 = component1.when(DataBucketBean::full_name, "/" + path);
			component1 = component1.when(DataBucketBean::full_name, path + "/");
			component1 = component1.when(DataBucketBean::full_name, "/" + path + "/");
		}
		
		// Anything _after_ me in the chain
		final SingleQueryComponent<DataBucketBean> component2 = CrudUtils.allOf(DataBucketBean.class)
						.rangeAbove(DataBucketBean::full_name, normalized_bucket_full_name, true)
						.rangeBelow(DataBucketBean::full_name, m.group(1) + first_char_of_final_path_inc, true);
		
		return CrudUtils.anyOf(component1, component2);
	}
	
	/** Notify interested harvesters in the creation of a new bucket
	 * @param new_object - the bucket just created
	 * @param is_suspended - whether it's initially suspended
	 * @param actor_context - the actor context for broadcasting out requests
	 * @return the management side channel from the harvesters
	 */
	public static CompletableFuture<Collection<BasicMessageBean>> requestNewBucket(
			final DataBucketBean new_object, final boolean is_suspended,
			final ICrudService<DataBucketStatusBean> status_store,
			final ManagementDbActorContext actor_context 
			)
	{
		// OK if we're here then it's time to notify any interested harvesters

		final BucketActionMessage.NewBucketActionMessage new_message = 
				new BucketActionMessage.NewBucketActionMessage(new_object, is_suspended);
		
		final CompletableFuture<BucketActionCollectedRepliesMessage> f =
				BucketActionSupervisor.askBucketActionActor(Optional.empty(),
						actor_context.getBucketActionSupervisor(), actor_context.getActorSystem(),
						(BucketActionMessage)new_message, 
						Optional.empty());
		
		final CompletableFuture<Collection<BasicMessageBean>> management_results =
				f.<Collection<BasicMessageBean>>thenApply(replies -> {
					return replies.replies(); 
				});
		
		// Apply the affinity to the bucket status (which must exist, by construction):
		// (node any node information coming back from streaming enrichment is filtered out by the getSuccessfulNodes call below)
		final CompletableFuture<Boolean> update_future = MgmtCrudUtils.applyNodeAffinity(new_object._id(), status_store, MgmtCrudUtils.getSuccessfulNodes(management_results, SuccessfulNodeType.harvest_only));

		// Convert BucketActionCollectedRepliesMessage into a management side-channel:
		// (combine the 2 futures but then only return the management results, just need for the update to have completed)
		return management_results.thenCombine(update_future, (mgmt, update) -> mgmt);							
	}	
	
	/** Notify interested harvesters that a bucket has been updated
	 * @param new_object
	 * @param old_version
	 * @param status
	 * @param actor_context
	 * @param retry_store
	 * @return
	 */
	public static CompletableFuture<Collection<BasicMessageBean>> requestUpdatedBucket(
			final DataBucketBean new_object, 
			final DataBucketBean old_version,
			final DataBucketStatusBean status,
			final ManagementDbActorContext actor_context,
			final ICrudService<DataBucketStatusBean> status_store,
			final ICrudService<BucketActionRetryMessage> retry_store
			)
	{
		// First off, a couple of special cases relating to node affinity
		final boolean multi_node_enabled = Optional.ofNullable(new_object.multi_node_enabled()).orElse(false);
		final Set<String> node_affinity = Optional.ofNullable(status.node_affinity())
				.map(na -> {
					if (multi_node_enabled && (1 == status.node_affinity().size())) {
						//(this might indicate that we've moved from single node -> multi node
						return Collections.<String>emptyList();
					}
					else if (!multi_node_enabled && (status.node_affinity().size() > 1)) {
						//(this definitely indicates that we've moved from multi node -> single node)						
						return Collections.<String>emptyList();
					}
					else return na;
				})
				.map(na -> na.stream().collect(Collectors.toSet()))
				.orElse(Collections.emptySet());
				
		final BucketActionMessage.UpdateBucketActionMessage update_message = 
				new BucketActionMessage.UpdateBucketActionMessage(new_object, !status.suspended(), old_version, node_affinity);
		
		final CompletableFuture<Collection<BasicMessageBean>> management_results =
			MgmtCrudUtils.applyRetriableManagementOperation(new_object, 
					actor_context, retry_store, update_message,
					source -> new BucketActionMessage.UpdateBucketActionMessage
							(new_object, !status.suspended(), old_version, new HashSet<String>(Arrays.asList(source))));
		
		// Special case: if the bucket has no node affinity (something went wrong earlier) but now it does, then update:
		if (node_affinity.isEmpty()) {
			final CompletableFuture<Boolean> update_future = MgmtCrudUtils.applyNodeAffinity(new_object._id(), status_store, MgmtCrudUtils.getSuccessfulNodes(management_results, SuccessfulNodeType.harvest_only));
			return management_results.thenCombine(update_future, (mgmt, update) -> mgmt);							
		}
		else {
			return management_results;
		}
	}	
	
	public static final String DELETE_TOUCH_FILE = ".DELETED";
	
	/** Utility to add ".DELETED" to the designated bucket
	 * @param to_delete
	 * @param storage_service
	 * @throws Exception
	 */
	protected static void deleteFilePath(final DataBucketBean to_delete, final IStorageService storage_service) throws Exception {
		final FileContext dfs = storage_service.getUnderlyingPlatformDriver(FileContext.class, Optional.empty()).get();
		
		final String bucket_root = storage_service.getBucketRootPath() + "/" + to_delete.full_name() + IStorageService.BUCKET_SUFFIX;
		
		try (final FSDataOutputStream out = 
				dfs.create(new Path(bucket_root + "/" + DELETE_TOUCH_FILE), EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE)))
		{} //(ie close after creating)
	}

	/** Deletes the entire bucket, ie data and state
	 * @param to_delete
	 * @param storage_service
	 */
	public static void removeBucketPath(final DataBucketBean to_delete, final IStorageService storage_service, Optional<String> extra_path) throws Exception
	{ 
		final FileContext dfs = storage_service.getUnderlyingPlatformDriver(FileContext.class, Optional.empty()).get();
		final String bucket_root = storage_service.getBucketRootPath() + "/" + to_delete.full_name() + IStorageService.BUCKET_SUFFIX;
		dfs.delete(new Path(bucket_root + extra_path.map(s -> "/" + s).orElse("")), true);
	}
	
	/** Check if bucket exists (or the path within the bucket if "file" optional specified
	 * @param to_check
	 * @param storage_service
	 * @param file - the file path in the bucket (checks bucket root path if left blank)
	 * @return
	 * @throws Exception
	 */
	public static boolean doesBucketPathExist(final DataBucketBean to_check, final IStorageService storage_service, final Optional<String> file) throws Exception {
		final FileContext dfs = storage_service.getUnderlyingPlatformDriver(FileContext.class, Optional.empty()).get();
		final String bucket_root = storage_service.getBucketRootPath() + "/" + to_check.full_name() + IStorageService.BUCKET_SUFFIX;
		
		try {
			dfs.getFileStatus(new Path(bucket_root + file.map(s -> "/" + s).orElse("")));
			return true;
		}
		catch (FileNotFoundException fe) {
			return false;
		} 
		
	}	
	
	/** Bucket valdiation rules:
	 *  in the format /path/to/<etc>/here where:
	 *  - leading /, no trailing /
	 *  - no /../, /./ or //s 
	 *  - no " "s or ","s or ":"s or ';'s
	 * @param bucket_path
	 * @return
	 */
	public static boolean bucketPathFormatValidationCheck(final String bucket_path) {
		return 0 != Stream.of(bucket_path)
				.filter(p -> p.startsWith("/")) // not allowed for/example
				.filter(p -> !p.endsWith("/")) // not allowed /for/example/
				.filter(p -> !Pattern.compile("/[.]+(?:/|$)").matcher(p).find()) // not allowed /./ or /../
				.filter(p -> !Pattern.compile("//").matcher(p).find())
				.filter(p -> !Pattern.compile("(?:\\s|[,:;])").matcher(p).find())
				.count();
	}
	
	public static final FsPermission DEFAULT_DIR_PERMS = FsPermission.valueOf("drwxrwxrwx");	
	
	/** Create the different default files needed by the bucket in the distributed file system
	 * @param bucket
	 * @param storage_service
	 * @throws Exception
	 */
	public static void createFilePaths(final DataBucketBean bucket, final IStorageService storage_service) throws Exception {
		final FileContext dfs = storage_service.getUnderlyingPlatformDriver(FileContext.class, Optional.empty()).get();
	
		final String bucket_root = storage_service.getBucketRootPath() + "/" + bucket.full_name();		
		
		// Check if a "delete touch file is present, bail if so"
		if (doesBucketPathExist(bucket, storage_service, Optional.of(DELETE_TOUCH_FILE))) {
			throw new RuntimeException(ErrorUtils.get(ManagementDbErrorUtils.DELETE_TOUCH_FILE_PRESENT, bucket.full_name()));			
		}
		
		Arrays.asList(
				"",
				"logs",
				"logs/harvest",
				"logs/enrichment",
				"logs/storage",
				"assets",
				"import",
				"import/temp",
				"import/temp/upload",
				"import/transient",
				"import/stored",
				"import/stored/raw",
				"import/stored/json",
				"import/stored/processed",
				"import/ready"
				)
				.stream()
				.map(s -> new Path(bucket_root + IStorageService.BUCKET_SUFFIX + s))
				.forEach(Lambdas.wrap_consumer_u(p -> {
					dfs.mkdir(p, DEFAULT_DIR_PERMS, true); //(note perm is & with umask)
					try { dfs.setPermission(p, DEFAULT_DIR_PERMS); } catch (Exception e) {} // (not supported in all FS)
				}));
	}
	
	private static final Pattern VALID_ANALYTIC_JOB_NAME = Pattern.compile("[a-zA-Z0-9_]+");
	private static final Pattern VALID_DATA_SERVICES = Pattern.compile("(?:batch|stream)|(?:(?:search_index_service|storage_service|document_service)(?:[.][a-zA-Z_-]+)?)");
	private static final Pattern VALID_RESOURCE_ID = Pattern.compile("(:?[a-zA-Z0-9_$]*|/[^:]+|/[^:]+:[a-zA-Z0-9_$]*)");
		//(first is internal job, second is external only, third is both) 
	
	/** Validate buckets for their analytic content (lots depends on the specific technology, so that is delegated to the technology)
	 * @param bean
	 * @return
	 */
	protected static List<String> validateAnalyticBucket(final DataBucketBean bean) {
		final LinkedList<String> errs = new LinkedList<String>();
		if ((null != bean.analytic_thread()) && Optional.ofNullable(bean.analytic_thread().enabled()).orElse(true)) {
			
			// Check that no enrichment is specified
			if (MasterEnrichmentType.none != Optional.ofNullable(bean.master_enrichment_type()).orElse(MasterEnrichmentType.none)) {
				errs.add(ErrorUtils.get(ManagementDbErrorUtils.ANALYTIC_JOB_ENRICHMENT_SPECIFIED, bean.full_name()));
			}		
			
			final AnalyticThreadBean analytic_thread = bean.analytic_thread();

			// 1) Jobs can be empty
			
			final Collection<AnalyticThreadJobBean> jobs = Optionals.ofNullable(bean.analytic_thread().jobs());
			
			jobs.stream().filter(job -> Optional.ofNullable(job.enabled()).orElse(true)).forEach(job -> {

				// 2) Some basic checks
				
				final String job_identifier = Optional.ofNullable(job.name()).orElse(BeanTemplateUtils.toJson(job).toString());
				if ((null == job.name()) || !VALID_ANALYTIC_JOB_NAME.matcher(job.name()).matches()) {
					errs.add(ErrorUtils.get(ManagementDbErrorUtils.ANALYTIC_JOB_MALFORMED_NAME, bean.full_name(), job_identifier));					
				}
				
				if (null == job.analytic_technology_name_or_id()) {
					errs.add(ErrorUtils.get(ManagementDbErrorUtils.ANALYTIC_BUT_NO_ANALYTIC_TECH, bean.full_name(), job_identifier));
				}
				
				if (null == job.analytic_type() || (MasterEnrichmentType.none == job.analytic_type())) {
					errs.add(ErrorUtils.get(ManagementDbErrorUtils.ANALYTIC_JOB_MUST_HAVE_TYPE, bean.full_name(), job_identifier));					
				}
				
				if (null != job.analytic_type() && (MasterEnrichmentType.streaming_and_batch == job.analytic_type())) {
					//(temporary restriction - jobs cannot be both batch and streaming, even though that option exists as a enum)
					errs.add(ErrorUtils.get(ManagementDbErrorUtils.STREAMING_AND_BATCH_NOT_SUPPORTED, bean.full_name()));					
				}				
				
				// 3) Inputs
				
				final List<AnalyticThreadJobBean.AnalyticThreadJobInputBean> inputs = 
						Optionals.ofNullable(job.inputs()).stream().filter(i -> Optional.ofNullable(i.enabled()).orElse(true))
						.collect(Collectors.toList());

				// we'll allow inputs to be empty (or at least leave it up to the technology) - perhaps a job is hardwired to get external inputs?
				
				// enabled inputs must not be non-empty
				
				inputs.forEach(input -> {

					// 3.1) basic checks
					
					if ((null == input.data_service()) || !VALID_DATA_SERVICES.matcher(input.data_service()).matches()) {
						errs.add(ErrorUtils.get(ManagementDbErrorUtils.ANALYTIC_INPUT_MALFORMED_DATA_SERVICE, bean.full_name(), job_identifier, 
								input.data_service(), VALID_DATA_SERVICES.toString()));											
					}
					if ((null == input.resource_name_or_id()) || !VALID_RESOURCE_ID.matcher(input.resource_name_or_id()).matches()) {
						errs.add(ErrorUtils.get(ManagementDbErrorUtils.ANALYTIC_INPUT_MALFORMED_RESOURCE_ID, bean.full_name(), job_identifier, 
								input.resource_name_or_id(), VALID_RESOURCE_ID.toString()));											
					}
					
					// 3.2) input config
					
					final Optional<String> o_tmin = Optionals.of(() -> job.global_input_config().time_min()).map(Optional::of).orElse(Optionals.of(() -> input.config().time_min()));
					final Optional<String> o_tmax = Optionals.of(() -> job.global_input_config().time_max()).map(Optional::of).orElse(Optionals.of(() -> input.config().time_max()));
					o_tmin.ifPresent(tmin -> {
						final Validation<String, Date> test = TimeUtils.getSchedule(tmin, Optional.empty());
						if (test.isFail()) {
							errs.add(ErrorUtils.get(ManagementDbErrorUtils.ANALYTIC_INPUT_MALFORMED_DATE, bean.full_name(), job_identifier, "tmin", tmin, test.fail())); 
						}
					});
					o_tmax.ifPresent(tmax -> {
						final Validation<String, Date> test = TimeUtils.getSchedule(tmax, Optional.empty());
						if (test.isFail()) {
							errs.add(ErrorUtils.get(ManagementDbErrorUtils.ANALYTIC_INPUT_MALFORMED_DATE, bean.full_name(), job_identifier, "tmax", tmax, test.fail())); 
						}						
					});
				});
				
				// 4) Outputs
				
				// (will allow null output here, since the technology might be fine with that, though it will normally result in downstream errors)

				if (null != job.output()) {
					
					final boolean is_transient = Optional.ofNullable(job.output().is_transient()).orElse(false);
					if (is_transient) {
						// If transient have to set a streaming/batch type
						if (MasterEnrichmentType.none == Optional.ofNullable(job.output().transient_type()).orElse(MasterEnrichmentType.none)) {
							errs.add(ErrorUtils.get(ManagementDbErrorUtils.ANALYTIC_OUTPUT_TRANSIENT_MISSING_FIELD, bean.full_name(), job_identifier, "transient_type")); 
						}
						// Must not have a sub-bucket path
						if (null != job.output().sub_bucket_path()) {
							errs.add(ErrorUtils.get(ManagementDbErrorUtils.ANALYTIC_OUTPUT_TRANSIENT_ILLEGAL_FIELD, bean.full_name(), job_identifier, "sub_bucket_path")); 
						}
						// TODO (ALEPH-12): Don't currently support "is_transient" with "preserve_data"
						if (Optional.ofNullable(job.output().preserve_existing_data()).orElse(false)) {
							errs.add(ErrorUtils.get("Due to temporary bug, don't currently support is_transient:true and preserve_existing_data:false - please contact the developers"));
						}
					}
				}				
			});
			
			// 5) Triggers
			
			if (null != analytic_thread.trigger_config()) {
				if (null != analytic_thread.trigger_config().schedule()) {
					final Validation<String, Date> test = TimeUtils.getSchedule(analytic_thread.trigger_config().schedule(), Optional.empty());
					if (test.isFail()) {
						errs.add(ErrorUtils.get(ManagementDbErrorUtils.ANALYTIC_TRIGGER_MALFORMED_DATE, bean.full_name(), analytic_thread.trigger_config().schedule(), test.fail()));
					}					
				}			
				Optional.ofNullable(analytic_thread.trigger_config().trigger()).ifPresent(trigger -> errs.addAll(validateAnalyticTrigger(bean, trigger)));
			}
		}
		return errs;
	}

	/** Recursive check for triggers in analytic beans
	 * @param trigger
	 * @return
	 */
	private static List<String> validateAnalyticTrigger(final DataBucketBean bucket, final AnalyticThreadComplexTriggerBean trigger) {
		final LinkedList<String> errs = new LinkedList<String>();

		// Either: specify resource id or boolean equation type...
		if ((null != trigger.resource_name_or_id()) && (null != trigger.op())
				||
			(null == trigger.resource_name_or_id()) && (null == trigger.op()))
		{
			errs.add(ErrorUtils.get(ManagementDbErrorUtils.ANALYTIC_TRIGGER_ILLEGAL_COMBO, bucket.full_name()));
		}
		// if resource id then must have corresponding type (unless custom - custom analytic tech will have to do its own error validation)
		if ((null != trigger.resource_name_or_id()) ^ (null != trigger.type())) {
			errs.add(ErrorUtils.get(ManagementDbErrorUtils.ANALYTIC_TRIGGER_ILLEGAL_COMBO, bucket.full_name()));
		}		
		// (If custom then must specifiy a custom technology)
		if ((TriggerType.custom == trigger.type()) && (null == trigger.custom_analytic_technology_name_or_id())) {
			errs.add(ErrorUtils.get(ManagementDbErrorUtils.ANALYTIC_CUSTOM_TRIGGER_NOT_COMPLETE, bucket.full_name()));
		}
		// If generating a boolean equation then must have equation terms
		if ((null != trigger.op()) && Optionals.ofNullable(trigger.dependency_list()).isEmpty()) {
			errs.add(ErrorUtils.get(ManagementDbErrorUtils.ANALYTIC_TRIGGER_ILLEGAL_COMBO, bucket.full_name()));
		}
		
		final Stream<AnalyticThreadComplexTriggerBean> other_trigger_configs = Optionals.ofNullable(trigger.dependency_list()).stream();
		
		// Recursive check
		other_trigger_configs.forEach(other_trigger -> errs.addAll(validateAnalyticTrigger(bucket, other_trigger)));		
		
		return errs;
	}
	
	/** Check times inside the bucket and some of its standard schemas
	 * @param bean
	 * @return
	 */
	protected static List<String> validateBucketTimes(final DataBucketBean bean) {
		final LinkedList<String> errs = new LinkedList<String>();
		
		if (null != bean.poll_frequency()) {
			TimeUtils.getDuration(bean.poll_frequency())
				.f().forEach(s -> errs.add(ErrorUtils.get(ManagementDbErrorUtils.BUCKET_INVALID_TIME, bean.full_name(), "poll_frequency", s)));			
		}
		
		if (null != bean.data_schema()) {
			if (null != bean.data_schema().temporal_schema()) {
				if (Optional.ofNullable(bean.data_schema().temporal_schema().enabled()).orElse(true)) {
					// Grouping times
					if (null != bean.data_schema().temporal_schema().grouping_time_period()) {
						TimeUtils.getTimePeriod(bean.data_schema().temporal_schema().grouping_time_period())
							.f().forEach(s -> errs.add(ErrorUtils.get(ManagementDbErrorUtils.BUCKET_INVALID_TIME, bean.full_name(), "data_schema.temporal_schema.grouping_time_period", s)));						
					}
					// Max ages
					if (null != bean.data_schema().temporal_schema().cold_age_max()) {
						TimeUtils.getDuration(bean.data_schema().temporal_schema().cold_age_max())
							.f().forEach(s -> errs.add(ErrorUtils.get(ManagementDbErrorUtils.BUCKET_INVALID_TIME, bean.full_name(), "data_schema.temporal_schema.cold_age_max", s)));
					}
					if (null != bean.data_schema().temporal_schema().hot_age_max()) {
						TimeUtils.getDuration(bean.data_schema().temporal_schema().hot_age_max())
							.f().forEach(s -> errs.add(ErrorUtils.get(ManagementDbErrorUtils.BUCKET_INVALID_TIME, bean.full_name(), "data_schema.temporal_schema.hot_age_max", s)));
					}
					if (null != bean.data_schema().temporal_schema().exist_age_max()) {
						TimeUtils.getDuration(bean.data_schema().temporal_schema().exist_age_max())
							.f().forEach(s -> errs.add(ErrorUtils.get(ManagementDbErrorUtils.BUCKET_INVALID_TIME, bean.full_name(), "data_schema.temporal_schema.exist_age_max", s)));
					}
				}
			}
			if (null != bean.data_schema().storage_schema()) { 
				if (Optional.ofNullable(bean.data_schema().storage_schema().enabled()).orElse(true)) {
					// JSON
					if (null != bean.data_schema().storage_schema().json()) {
						if (null != bean.data_schema().storage_schema().json().grouping_time_period()) {
							TimeUtils.getTimePeriod(bean.data_schema().storage_schema().json().grouping_time_period())
								.f().forEach(s -> errs.add(ErrorUtils.get(ManagementDbErrorUtils.BUCKET_INVALID_TIME, bean.full_name(), "data_schema.storage_schema.json.grouping_time_period", s)));						
						}
						if (null != bean.data_schema().storage_schema().json().exist_age_max()) {
							TimeUtils.getDuration(bean.data_schema().storage_schema().json().exist_age_max())
								.f().forEach(s -> errs.add(ErrorUtils.get(ManagementDbErrorUtils.BUCKET_INVALID_TIME, bean.full_name(), "data_schema.storage_schema.json.exist_age_max", s)));						
						}
					}
					// RAW
					if (null != bean.data_schema().storage_schema().raw()) {
						if (null != bean.data_schema().storage_schema().raw().grouping_time_period()) {
							TimeUtils.getTimePeriod(bean.data_schema().storage_schema().raw().grouping_time_period())
								.f().forEach(s -> errs.add(ErrorUtils.get(ManagementDbErrorUtils.BUCKET_INVALID_TIME, bean.full_name(), "data_schema.storage_schema.raw.grouping_time_period", s)));						
						}
						if (null != bean.data_schema().storage_schema().raw().exist_age_max()) {
							TimeUtils.getDuration(bean.data_schema().storage_schema().raw().exist_age_max())
								.f().forEach(s -> errs.add(ErrorUtils.get(ManagementDbErrorUtils.BUCKET_INVALID_TIME, bean.full_name(), "data_schema.storage_schema.raw.exist_age_max", s)));						
						}
					}
					// PROCESSED
					if (null != bean.data_schema().storage_schema().processed()) {
						if (null != bean.data_schema().storage_schema().processed().grouping_time_period()) {
							TimeUtils.getTimePeriod(bean.data_schema().storage_schema().processed().grouping_time_period())
								.f().forEach(s -> errs.add(ErrorUtils.get(ManagementDbErrorUtils.BUCKET_INVALID_TIME, bean.full_name(), "data_schema.storage_schema.processed.grouping_time_period", s)));						
						}
						if (null != bean.data_schema().storage_schema().processed().exist_age_max()) {
							TimeUtils.getDuration(bean.data_schema().storage_schema().processed().exist_age_max())
								.f().forEach(s -> errs.add(ErrorUtils.get(ManagementDbErrorUtils.BUCKET_INVALID_TIME, bean.full_name(), "data_schema.storage_schema.processed.exist_age_max", s)));						
						}
					}
				}				
			}
		}		
		return errs;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService#getCrudService()
	 */
	@Override
	public Optional<ICrudService<DataBucketBean>> getCrudService() {
		return Optional.of(this);
	}
}
