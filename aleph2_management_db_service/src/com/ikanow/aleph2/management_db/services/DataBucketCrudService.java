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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
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
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IBasicSearchService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.MockServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
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
import com.ikanow.aleph2.data_model.utils.LoggingUtils;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
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
import com.ikanow.aleph2.management_db.utils.BucketValidationUtils;
import com.ikanow.aleph2.management_db.utils.DataServiceUtils;
import com.ikanow.aleph2.management_db.utils.ManagementDbErrorUtils;
import com.ikanow.aleph2.management_db.utils.MgmtCrudUtils;

import java.util.stream.Stream;

//TODO (ALEPH-19): ... handle the update poll messaging, needs an indexed "next poll" query, do a findAndMod from every thread every minute
//TODO (ALEPH-19): If I change a bucket again, need to cancel anything in the retry bin for that bucket (or if I re-created a deleted bucket)

/** CRUD service for Data Bucket with management proxy
 * @author acp
 */
public class DataBucketCrudService implements IManagementCrudService<DataBucketBean> {
	@SuppressWarnings("unused")
	private static final Logger _logger = LogManager.getLogger();	
	
	protected final Provider<IStorageService> _storage_service;	
	protected final Provider<IManagementDbService> _underlying_management_db;
	
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
		_underlying_management_db = service_context.getServiceProvider(IManagementDbService.class, Optional.empty()).get();
				
		_storage_service = service_context.getServiceProvider(IStorageService.class, Optional.empty()).get();
		
		_actor_context = actor_context;
		_service_context = service_context;
		
		ModuleUtils.getAppInjector().thenRun(() -> initialize());					
	}
	
	/** Work around for Guice circular development issues
	 */
	protected void initialize() {
		_underlying_data_bucket_db.set(_underlying_management_db.get().getDataBucketStore());
		_underlying_data_bucket_status_db.set(_underlying_management_db.get().getDataBucketStatusStore());
		_bucket_action_retry_store.set(_underlying_management_db.get().getRetryStore(BucketActionRetryMessage.class));
		_bucket_deletion_queue.set(_underlying_management_db.get().getBucketDeletionQueue(BucketDeletionMessage.class));
		
		// Handle some simple optimization of the data bucket CRUD repo:
		Executors.newSingleThreadExecutor().submit(() -> {
			_underlying_data_bucket_db.get().optimizeQuery(Arrays.asList(
					BeanTemplateUtils.from(DataBucketBean.class).field(DataBucketBean::full_name)));
		});		
		
	}
	
	/** User constructor, for wrapping
	 */
	public DataBucketCrudService(final IServiceContext service_context,
			final Provider<IManagementDbService> underlying_management_db, 
			final Provider<IStorageService> storage_service,
			final ICrudService<DataBucketBean> underlying_data_bucket_db,
			final ICrudService<DataBucketStatusBean> underlying_data_bucket_status_db			
			)
	{
		_service_context = service_context;
		_underlying_management_db = underlying_management_db;
		_underlying_data_bucket_db.set(underlying_data_bucket_db);
		_underlying_data_bucket_status_db.set(underlying_data_bucket_status_db);
		_bucket_action_retry_store.set(_underlying_management_db.get().getRetryStore(BucketActionRetryMessage.class));
		_bucket_deletion_queue.set(_underlying_management_db.get().getBucketDeletionQueue(BucketDeletionMessage.class));		
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
		return new DataBucketCrudService(_service_context, 
				new MockServiceContext.MockProvider<IManagementDbService>(_underlying_management_db.get().getFilteredDb(client_auth, project_auth)), 
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
			createFilePaths(new_object, _storage_service.get());
		}
		catch (Exception e) { // Error creating directory, haven't created object yet so just back out now
			
			return FutureUtils.createManagementFuture(
					FutureUtils.returnError(e));			
		}
		// OK if the bucket is validated we can store it (and create a status object)
				
		final CompletableFuture<Supplier<Object>> ret_val = _underlying_data_bucket_db.get().storeObject(new_object, replace_if_present);
		final boolean is_suspended = DataBucketStatusCrudService.bucketIsSuspended(corresponding_status.get().get());

		// Register the bucket update with any applicable data services		
		
		final  Multimap<IDataServiceProvider, String> data_service_info = DataServiceUtils.selectDataServices(new_object.data_schema(), _service_context);
		final Optional<Multimap<IDataServiceProvider, String>> old_data_service_info = old_bucket.map(old -> DataServiceUtils.selectDataServices(old.data_schema(), _service_context));
		
		final List<CompletableFuture<Collection<BasicMessageBean>>> ds_update_results = data_service_info.asMap().entrySet().stream()
			.map(kv -> 
					kv.getKey()
						.onPublishOrUpdate(new_object, old_bucket, is_suspended, 
							kv.getValue().stream().collect(Collectors.toSet()), 
							old_data_service_info
								.map(old_map -> old_map.get(kv.getKey()))
								.map(old_servs -> old_servs.stream().collect(Collectors.toSet()))
								.orElse(Collections.emptySet())
							)
			)
			.collect(Collectors.toList())
			;
		
		// Process old data services that are no longer in use
		final List<CompletableFuture<Collection<BasicMessageBean>>> old_ds_update_results = old_data_service_info.map(old_ds_info -> {
			return old_ds_info.asMap().entrySet().stream()
					.filter(kv -> !data_service_info.containsKey(kv.getKey()))
					.<CompletableFuture<Collection<BasicMessageBean>>>map(kv -> 
						kv.getKey().onPublishOrUpdate(new_object, old_bucket, is_suspended, Collections.emptySet(), kv.getValue().stream().collect(Collectors.toSet())))
					.collect(Collectors.toList())
					;					
		})
		.orElse(Collections.emptyList());
		
		//(combine)
		@SuppressWarnings("unchecked")
		CompletableFuture<Collection<BasicMessageBean>> all_service_registration_complete[] =
				Stream.concat(ds_update_results.stream(), old_ds_update_results.stream()).toArray(CompletableFuture[]::new);
		
		// Get the status and then decide whether to broadcast out the new/update message
		
		final CompletableFuture<Collection<BasicMessageBean>> mgmt_results = 
				CompletableFuture.allOf(all_service_registration_complete).thenCombine(				
						old_bucket.isPresent()
							? requestUpdatedBucket(new_object, old_bucket.get(), corresponding_status.get().get(), _actor_context, _underlying_data_bucket_status_db.get(), _bucket_action_retry_store.get())
							: requestNewBucket(new_object, is_suspended, _underlying_data_bucket_status_db.get(), _actor_context)
						,
						(__, harvest_results) -> {
							return (Collection<BasicMessageBean>)
									Stream.concat(
										Arrays.stream(all_service_registration_complete).flatMap(s -> s.join().stream())
										,
										harvest_results.stream()
									)
									.collect(Collectors.toList())
									;
						})
						.exceptionally(t -> Arrays.asList(ErrorUtils.buildErrorMessage(this.getClass().getSimpleName(), "storeValidatedObject", ErrorUtils.get("{0}", t))))
						;
			
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
			deleteFilePath(to_delete, _storage_service.get());
			//delete the logging path as well if it exists
			deleteFilePath(LoggingUtils.convertBucketToLoggingBucket(to_delete), _storage_service.get());
			
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
			if (!BucketValidationUtils.bucketPathFormatValidationCheck(bucket.full_name())) {
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
		
		errors.addAll(BucketValidationUtils.staticValidation(bucket, allow_system_names));		

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

		Tuple2<Map<String, String>, List<BasicMessageBean>> schema_validation = BucketValidationUtils.validateSchema(bucket, _service_context);
		
		errors.addAll(schema_validation._2());
		
		return Tuples._2T(
				BeanTemplateUtils.clone(bucket).with(DataBucketBean::data_locations, schema_validation._1()).done(), 
				errors);
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
			final boolean multi_node_enabled = Optional.ofNullable(new_bucket.multi_node_enabled()).orElse(false);
			final MasterEnrichmentType master_enrichment_type = Optional.ofNullable(new_bucket.master_enrichment_type()).orElse(MasterEnrichmentType.none);
			
			if ((null != corresponding_status.confirmed_multi_node_enabled()) 
					&& (Boolean.valueOf(multi_node_enabled) != corresponding_status.confirmed_multi_node_enabled()))
			{
				errors.add(MgmtCrudUtils.createValidationError(
						ErrorUtils.get(ManagementDbErrorUtils.BUCKET_UPDATE_ILLEGAL_FIELD_CHANGED_ACTIVE, 
								new_bucket.full_name(), "multi_node_enabled")));				
			}
			if ((null != corresponding_status.confirmed_master_enrichment_type()) 
					&& (master_enrichment_type != corresponding_status.confirmed_master_enrichment_type()))
			{
				errors.add(MgmtCrudUtils.createValidationError(
						ErrorUtils.get(ManagementDbErrorUtils.BUCKET_UPDATE_ILLEGAL_FIELD_CHANGED_ACTIVE, 
								new_bucket.full_name(), "master_enrichment_type")));				
			}
		}
		return errors;
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
		// (note any node information coming back from streaming enrichment is filtered out by the getSuccessfulNodes call below)
		final CompletableFuture<Boolean> update_future = MgmtCrudUtils.applyNodeAffinityWrapper(new_object, status_store, management_results);

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
		final boolean lock_to_nodes = Optional.ofNullable(new_object.lock_to_nodes()).orElse(true);
		if (node_affinity.isEmpty()) {
			final CompletableFuture<Boolean> update_future = MgmtCrudUtils.applyNodeAffinityWrapper(new_object, status_store, management_results);
			return management_results.thenCombine(update_future, (mgmt, update) -> mgmt);							
		}
		else if (!lock_to_nodes && Optional.ofNullable(status.confirmed_suspended()).orElse(false)) { // previously had a node affinity, remove now that we're definitely suspended
			final CompletableFuture<Boolean> update_future = status_store.updateObjectById(new_object._id(), 
					CrudUtils.update(DataBucketStatusBean.class).unset(DataBucketStatusBean::node_affinity));			
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
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService#getCrudService()
	 */
	@Override
	public Optional<ICrudService<DataBucketBean>> getCrudService() {
		return Optional.of(this);
	}
}
