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
package com.ikanow.aleph2.management_db.services;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
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
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.data_import.HarvestControlMetadataBean;
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
import com.ikanow.aleph2.management_db.utils.ManagementDbErrorUtils;
import com.ikanow.aleph2.management_db.utils.MgmtCrudUtils;

import java.util.stream.Stream;

//TODO (ALEPH-19): Need an additional bucket service that is responsible for actually deleting the data (for now, add a .DELETED file just so we know)
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
	
	protected final ICrudService<DataBucketBean> _underlying_data_bucket_db;
	protected final ICrudService<DataBucketStatusBean> _underlying_data_bucket_status_db;
	protected final ICrudService<BucketActionRetryMessage> _bucket_action_retry_store;
	
	protected final ManagementDbActorContext _actor_context;
	protected final IServiceContext _service_context;
	
	/** Guice invoked constructor
	 */
	@Inject
	public DataBucketCrudService(final IServiceContext service_context, ManagementDbActorContext actor_context)
	{
		_underlying_management_db = service_context.getService(IManagementDbService.class, Optional.empty()).get();
		
		_underlying_data_bucket_db = _underlying_management_db.getDataBucketStore();
		_underlying_data_bucket_status_db = _underlying_management_db.getDataBucketStatusStore();
		_bucket_action_retry_store = _underlying_management_db.getRetryStore(BucketActionRetryMessage.class);
		
		_storage_service = service_context.getStorageService();
		
		_actor_context = actor_context;
		_service_context = service_context;
		
		// Handle some simple optimization of the data bucket CRUD repo:
		Executors.newSingleThreadExecutor().submit(() -> {
			_underlying_data_bucket_db.optimizeQuery(Arrays.asList(
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
		_underlying_data_bucket_db = underlying_data_bucket_db;
		_underlying_data_bucket_status_db = underlying_data_bucket_status_db;
		_bucket_action_retry_store = _underlying_management_db.getRetryStore(BucketActionRetryMessage.class);
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
				_underlying_data_bucket_db.getFilteredRepo(authorization_fieldname, client_auth, project_auth),
				_underlying_data_bucket_status_db.getFilteredRepo(authorization_fieldname, client_auth, project_auth)
				);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#storeObject(java.lang.Object, boolean)
	 */
	public ManagementFuture<Supplier<Object>> storeObject(final DataBucketBean new_object, final boolean replace_if_present)
	{
		final MethodNamingHelper<DataBucketStatusBean> helper = BeanTemplateUtils.from(DataBucketStatusBean.class); 
		
		try {			
			// New bucket vs update - get the old bucket (we'll do this non-concurrently at least for now)
			
			final Optional<DataBucketBean> old_bucket = Lambdas.get(() -> {
				try {
					if (replace_if_present && (null != new_object._id())) {
						return _underlying_data_bucket_db.getObjectById(new_object._id()).get();
					}
					else {
						return Optional.<DataBucketBean>empty();
					}
				}
				catch (Exception e) {
					throw new RuntimeException(e);
				}
			});

			// Validation
			
			final Collection<BasicMessageBean> validation_info = validateBucket(new_object, old_bucket);
			
			if (!validation_info.isEmpty() && validation_info.stream().anyMatch(m -> !m.success())) {
				return FutureUtils.createManagementFuture(
						FutureUtils.returnError(new RuntimeException("Bucket not valid, see management channels")),
						CompletableFuture.completedFuture(validation_info)
						);
			}

			// Error if a bucket status doesn't exist - must create a bucket status before creating the bucket
			// (note the above validation ensures the bucket has an _id)
			// (obviously need to block here until we're sure..)
			
			final CompletableFuture<Optional<DataBucketStatusBean>> corresponding_status = 
					_underlying_data_bucket_status_db.getObjectById(new_object._id(), 
							Arrays.asList(helper.field(DataBucketStatusBean::_id), 
											helper.field(DataBucketStatusBean::node_affinity), 
											helper.field(DataBucketStatusBean::suspended), 
											helper.field(DataBucketStatusBean::quarantined_until)), true);					
					
			if (!corresponding_status.get().isPresent()) {
				return FutureUtils.createManagementFuture(
						FutureUtils.returnError(new RuntimeException(
								ErrorUtils.get(ManagementDbErrorUtils.BUCKET_CANNOT_BE_CREATED_WITHOUT_BUCKET_STATUS, new_object.full_name()))),
						CompletableFuture.completedFuture(Collections.emptyList())
						);				
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
					
			final CompletableFuture<Supplier<Object>> ret_val = _underlying_data_bucket_db.storeObject(new_object, replace_if_present);

			// Get the status and then decide whether to broadcast out the new/update message
			
			final boolean is_suspended = DataBucketStatusCrudService.bucketIsSuspended(corresponding_status.get().get());
			final CompletableFuture<Collection<BasicMessageBean>> mgmt_results = old_bucket.isPresent()
					? requestUpdatedBucket(new_object, old_bucket.get(), corresponding_status.get().get(), _actor_context, _bucket_action_retry_store)
					: requestNewBucket(new_object, is_suspended, _underlying_data_bucket_status_db, _actor_context);
				
			// If we got no responses then leave the object but suspend it
					
			return FutureUtils.createManagementFuture(ret_val,
					MgmtCrudUtils.handlePossibleEmptyNodeAffinity(new_object, is_suspended, mgmt_results, _underlying_data_bucket_status_db)					
										.thenApply(msgs -> Stream.concat(msgs.stream(), validation_info.stream()).collect(Collectors.toList())));
		}
		catch (Exception e) {
			// This is a serious enough exception that we'll just leave here
			return FutureUtils.createManagementFuture(
					FutureUtils.returnError(e));			
		}
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
		return FutureUtils.createManagementFuture(_underlying_data_bucket_db.optimizeQuery(ordered_field_list));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deregisterOptimizedQuery(java.util.List)
	 */
	public boolean deregisterOptimizedQuery(final List<String> ordered_field_list) {
		return _underlying_data_bucket_db.deregisterOptimizedQuery(ordered_field_list);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	public ManagementFuture<Optional<DataBucketBean>> getObjectBySpec(final QueryComponent<DataBucketBean> unique_spec) {
		return FutureUtils.createManagementFuture(_underlying_data_bucket_db.getObjectBySpec(unique_spec));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.List, boolean)
	 */
	public ManagementFuture<Optional<DataBucketBean>> getObjectBySpec(
			final QueryComponent<DataBucketBean> unique_spec,
			final List<String> field_list, final boolean include) {
		return FutureUtils.createManagementFuture(_underlying_data_bucket_db.getObjectBySpec(unique_spec, field_list, include));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectById(java.lang.Object)
	 */
	public ManagementFuture<Optional<DataBucketBean>> getObjectById(final Object id) {
		return FutureUtils.createManagementFuture(_underlying_data_bucket_db.getObjectById(id));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectById(java.lang.Object, java.util.List, boolean)
	 */
	public ManagementFuture<Optional<DataBucketBean>> getObjectById(final Object id,
			final List<String> field_list, final boolean include) {
		return FutureUtils.createManagementFuture(_underlying_data_bucket_db.getObjectById(id, field_list, include));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	public ManagementFuture<ICrudService.Cursor<DataBucketBean>> getObjectsBySpec(
			final QueryComponent<DataBucketBean> spec)
	{
		return FutureUtils.createManagementFuture(_underlying_data_bucket_db.getObjectsBySpec(spec));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.List, boolean)
	 */
	public ManagementFuture<ICrudService.Cursor<DataBucketBean>> getObjectsBySpec(
			final QueryComponent<DataBucketBean> spec, final List<String> field_list,
			final boolean include)
	{
		return FutureUtils.createManagementFuture(_underlying_data_bucket_db.getObjectsBySpec(spec, field_list, include));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#countObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	public ManagementFuture<Long> countObjectsBySpec(final QueryComponent<DataBucketBean> spec) {
		return FutureUtils.createManagementFuture(_underlying_data_bucket_db.countObjectsBySpec(spec));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#countObjects()
	 */
	public ManagementFuture<Long> countObjects() {
		return FutureUtils.createManagementFuture(_underlying_data_bucket_db.countObjects());
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deleteObjectById(java.lang.Object)
	 */
	public ManagementFuture<Boolean> deleteObjectById(final Object id) {		
		final CompletableFuture<Optional<DataBucketBean>> result = _underlying_data_bucket_db.getObjectById(id);		
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
		final CompletableFuture<Optional<DataBucketBean>> result = _underlying_data_bucket_db.getObjectBySpec(unique_spec);
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

		final CompletableFuture<Cursor<DataBucketBean>> to_delete = _underlying_data_bucket_db.getObjectsBySpec(spec);
		
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
			
			final CompletableFuture<Boolean> delete_reply = _underlying_data_bucket_db.deleteObjectById(to_delete._id());

			return FutureUtils.denestManagementFuture(delete_reply
				.thenCompose(del_reply -> {			
					if (!del_reply) { // Didn't find an object to delete, just return that information to the user
						return CompletableFuture.completedFuture(Optional.empty());
					}
					else { //Get the status and delete it 
						
						final CompletableFuture<Optional<DataBucketStatusBean>> future_status_bean =
								_underlying_data_bucket_status_db.updateAndReturnObjectBySpec(
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
							MgmtCrudUtils.applyRetriableManagementOperation(_actor_context, _bucket_action_retry_store, 
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
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getRawCrudService()
	 */
	public IManagementCrudService<JsonNode> getRawCrudService() {
		throw new RuntimeException("DataBucketCrudService.getRawCrudService not supported");
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getSearchService()
	 */
	public Optional<IBasicSearchService<DataBucketBean>> getSearchService() {
		return _underlying_data_bucket_db.getSearchService();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getUnderlyingPlatformDriver(java.lang.Class, java.util.Optional)
	 */
	@SuppressWarnings("unchecked")
	public <T> Optional<T> getUnderlyingPlatformDriver(final Class<T> driver_class,
			final Optional<String> driver_options)
	{
		if (driver_class == ICrudService.class) {
			return (Optional<T>) Optional.of(_underlying_data_bucket_db);
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
	
	/** Validates whether the new or updated bucket is valid: both in terms of authorization and in terms of format
	 * @param bucket
	 * @return
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	protected Collection<BasicMessageBean> validateBucket(final DataBucketBean bucket, final Optional<DataBucketBean> old_version) throws InterruptedException, ExecutionException {
		
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
				
				return errors; // (this is catastrophic obviously)			
			}
			
			if (!old_version.isPresent()) { // (create not update)
				if (this._underlying_data_bucket_db.countObjectsBySpec(CrudUtils.allOf(DataBucketBean.class)
																	.when(DataBucketBean::full_name, bucket.full_name())
																).get() > 0)
				{			
					errors.add(MgmtCrudUtils.createValidationError(
							ErrorUtils.get(ManagementDbErrorUtils.BUCKET_FULL_NAME_UNIQUENESS, Optional.ofNullable(bucket.full_name()).orElse("(unknown)"))));
					
					return errors; // (this is catastrophic obviously)
				}
			}
		}		
		// More complex missing field checks
		
		// - if has enrichment then must have harvest_technology_name_or_id (1) 
		// - if has harvest_technology_name_or_id then must have harvest_configs (2)
		// - if has enrichment then must have master_enrichment_type (3)
		// - if master_enrichment_type == batch/both then must have either batch_enrichment_configs or batch_enrichment_topology (4)
		// - if master_enrichment_type == streaming/both then must have either streaming_enrichment_configs or streaming_enrichment_topology (5)

		//(1, 3, 4, 5)
		if (((null != bucket.batch_enrichment_configs()) && !bucket.batch_enrichment_configs().isEmpty())
				|| ((null != bucket.streaming_enrichment_configs()) && !bucket.streaming_enrichment_configs().isEmpty())
				|| (null != bucket.batch_enrichment_topology())
				|| (null != bucket.streaming_enrichment_topology()))
		{
			if (null == bucket.harvest_technology_name_or_id()) { //(1)
				errors.add(MgmtCrudUtils.createValidationError(ErrorUtils.get(ManagementDbErrorUtils.ENRICHMENT_BUT_NO_HARVEST_TECH, bucket.full_name())));
			}
			if (null == bucket.master_enrichment_type()) { // (3)
				errors.add(MgmtCrudUtils.createValidationError(ErrorUtils.get(ManagementDbErrorUtils.ENRICHMENT_BUT_NO_MASTER_ENRICHMENT_TYPE, bucket.full_name())));				
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
		
		// (6)
		if ((null != bucket.harvest_technology_name_or_id()) && (null != bucket.harvest_configs())) {
			for (int i = 0; i < bucket.harvest_configs().size(); ++i) {
				final HarvestControlMetadataBean hmeta = bucket.harvest_configs().get(i);
				list_test.accept(Tuples._2T("harvest_configs" + Integer.toString(i) + ".library_ids_or_names", hmeta.library_ids_or_names()));
			}
		}

		// (7)
		Consumer<Tuple2<String, EnrichmentControlMetadataBean>> enrichment_test = emeta -> {
			if (Optional.ofNullable(emeta._2().enabled()).orElse(true)) {
				if ((null == emeta._2().library_ids_or_names()) || emeta._2().library_ids_or_names().isEmpty()) {
					errors.add(MgmtCrudUtils.createValidationError(ErrorUtils.get(ManagementDbErrorUtils.INVALID_ENRICHMENT_CONFIG_ELEMENTS_NO_LIBS, bucket.full_name(), emeta._1())));				
				}
			}
			list_test.accept(Tuples._2T(emeta._1() + ".library_ids_or_names", emeta._2().library_ids_or_names()));
			list_test.accept(Tuples._2T(emeta._1() + ".dependencies", emeta._2().dependencies()));
		};		
		if (null != bucket.batch_enrichment_topology()) {
			enrichment_test.accept(Tuples._2T("batch_enrichment_topology", bucket.batch_enrichment_topology()));
		}
		if (null != bucket.batch_enrichment_configs()) {
			for (int i = 0; i < bucket.batch_enrichment_configs().size(); ++i) {
				final EnrichmentControlMetadataBean emeta = bucket.batch_enrichment_configs().get(i);
				enrichment_test.accept(Tuples._2T("batch_enrichment_configs." + Integer.toString(i), emeta));
			}
		}
		if (null != bucket.streaming_enrichment_topology()) {
			enrichment_test.accept(Tuples._2T("streaming_enrichment_topology", bucket.streaming_enrichment_topology()));
		}
		if (null != bucket.streaming_enrichment_configs()) {
			for (int i = 0; i < bucket.streaming_enrichment_configs().size(); ++i) {
				final EnrichmentControlMetadataBean emeta = bucket.streaming_enrichment_configs().get(i);
				enrichment_test.accept(Tuples._2T("streaming_enrichment_configs." + Integer.toString(i), emeta));
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
		
		// OK before I do any more stateful checking, going to stop if we have logic errors first 
		
		if (!errors.isEmpty()) {
			return errors;
		}
		
		/////////////////
		
		// PHASE 2
				
		//TODO (ALEPH-19): multi buckets - authorization; other - authorization
		
		final CompletableFuture<Collection<BasicMessageBean>> bucket_path_errors_future = validateOtherBucketsInPathChain(bucket);
		
		errors.addAll(bucket_path_errors_future.join());
		
		// OK before I do any more stateful checking, going to stop if we have logic errors first 
		
		if (!errors.isEmpty()) {
			return errors;
		}
		
		/////////////////
		
		// PHASE 3
		
		// Finally Check whether I am allowed to update the various fields if old_version.isPresent()
		
		if (old_version.isPresent()) {
			final DataBucketBean old_bucket = old_version.get();
			if (!bucket.full_name().equals(old_bucket.full_name())) {
				errors.add(MgmtCrudUtils.createValidationError(ErrorUtils.get(ManagementDbErrorUtils.BUCKET_UPDATE_FULLNAME_CHANGED, bucket.full_name())));
			}
			if (!bucket.owner_id().equals(old_bucket.owner_id())) {
				errors.add(MgmtCrudUtils.createValidationError(ErrorUtils.get(ManagementDbErrorUtils.BUCKET_UPDATE_OWNERID_CHANGED, bucket.full_name())));
			}
		}
		
		/////////////////
		
		// PHASE 4 - DATA SCHEMA NOT MESSAGES AT THIS POINT CAN BE INFO, YOU NEED TO CHECK THE SUCCESS()

		errors.addAll(validateSchema(bucket, _service_context));
		
		return errors;
	}
	
	/** Validates that all enabled schema point to existing services and are well formed
	 * @param bucket
	 * @param service_context
	 * @return
	 */
	protected static List<BasicMessageBean> validateSchema(final DataBucketBean bucket, final IServiceContext service_context) {
		List<BasicMessageBean> errors = new LinkedList<>();
		
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
								.orElse(Arrays.asList(MgmtCrudUtils.createValidationError(
										ErrorUtils.get(ManagementDbErrorUtils.SCHEMA_ENABLED_BUT_SERVICE_NOT_PRESENT, bucket.full_name(), "columnar_schema")))));
			}
			// Document
			if ((null != bucket.data_schema().document_schema()) && Optional.ofNullable(bucket.data_schema().document_schema().enabled()).orElse(true))
			{
				errors.addAll(service_context.getService(IDocumentService.class, Optional.ofNullable(bucket.data_schema().document_schema().service_name()))
								.map(s -> s.validateSchema(bucket.data_schema().document_schema(), bucket))
								.orElse(Arrays.asList(MgmtCrudUtils.createValidationError(
										ErrorUtils.get(ManagementDbErrorUtils.SCHEMA_ENABLED_BUT_SERVICE_NOT_PRESENT, bucket.full_name(), "document_schema")))));
			}
			// Search Index
			if ((null != bucket.data_schema().search_index_schema()) && Optional.ofNullable(bucket.data_schema().search_index_schema().enabled()).orElse(true))
			{
				errors.addAll(service_context.getService(ISearchIndexService.class, Optional.ofNullable(bucket.data_schema().search_index_schema().service_name()))
								.map(s -> s.validateSchema(bucket.data_schema().search_index_schema(), bucket))
								.orElse(Arrays.asList(MgmtCrudUtils.createValidationError(
										ErrorUtils.get(ManagementDbErrorUtils.SCHEMA_ENABLED_BUT_SERVICE_NOT_PRESENT, bucket.full_name(), "search_index_schema")))));
			}
			// Storage
			if ((null != bucket.data_schema().storage_schema()) && Optional.ofNullable(bucket.data_schema().storage_schema().enabled()).orElse(true))
			{
				errors.addAll(service_context.getService(IStorageService.class, Optional.ofNullable(bucket.data_schema().storage_schema().service_name()))
								.map(s -> s.validateSchema(bucket.data_schema().storage_schema(), bucket))
								.orElse(Arrays.asList(MgmtCrudUtils.createValidationError(
										ErrorUtils.get(ManagementDbErrorUtils.SCHEMA_ENABLED_BUT_SERVICE_NOT_PRESENT, bucket.full_name(), "storage_schema")))));
			}
			if ((null != bucket.data_schema().temporal_schema()) && Optional.ofNullable(bucket.data_schema().temporal_schema().enabled()).orElse(true))
			{
				errors.addAll(service_context.getService(ITemporalService.class, Optional.ofNullable(bucket.data_schema().temporal_schema().service_name()))
								.map(s -> s.validateSchema(bucket.data_schema().temporal_schema(), bucket))
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
		return errors;
	}
	
	/** A utility routine to check whether the bucket is allowed to be in this location
	 * @param bucket - the bucket to validate
	 * @return a future containing validation errors based on the path
	 */
	private CompletableFuture<Collection<BasicMessageBean>> validateOtherBucketsInPathChain(final DataBucketBean bucket) {
		final MethodNamingHelper<DataBucketBean> helper = BeanTemplateUtils.from(DataBucketBean.class);
		final String bucket_full_name = normalizeBucketPath(bucket.full_name(), true);

		return this._underlying_data_bucket_db.getObjectsBySpec(getPathQuery(bucket_full_name), 
				Arrays.asList(helper.field(DataBucketBean::full_name), helper.field(DataBucketBean::access_rights)), true)
				.thenApply(cursor -> {
					return StreamSupport.stream(cursor.spliterator(), false)
						.filter(b -> (null == b.full_name()) || (null == b.access_rights()))
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
				Optional.ofNullable(new_object.multi_node_enabled()).orElse(false)
				? 					
				BucketActionSupervisor.askDistributionActor(
						actor_context.getBucketActionSupervisor(), actor_context.getActorSystem(),
						(BucketActionMessage)new_message, 
						Optional.empty())
				:
				BucketActionSupervisor.askChooseActor(
						actor_context.getBucketActionSupervisor(), actor_context.getActorSystem(),
						(BucketActionMessage)new_message, 
						Optional.empty());
		
		final CompletableFuture<Collection<BasicMessageBean>> management_results =
				f.<Collection<BasicMessageBean>>thenApply(replies -> {
					return replies.replies(); 
				});
		
		// Apply the affinity to the bucket status (which must exist, by construction):
		// (node any node information coming back from streaming enrichment is filtered out by the getSuccessfulNodes call below)
		final CompletableFuture<Boolean> update_future = MgmtCrudUtils.applyNodeAffinity(new_object._id(), status_store, MgmtCrudUtils.getSuccessfulNodes(management_results));

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
			final ICrudService<BucketActionRetryMessage> retry_store
			)
	{
		final BucketActionMessage.UpdateBucketActionMessage update_message = 
				new BucketActionMessage.UpdateBucketActionMessage(new_object, !status.suspended(), old_version,  
						new HashSet<String>(
								null == status.node_affinity() ? Collections.emptySet() : status.node_affinity()));
		
		return MgmtCrudUtils.applyRetriableManagementOperation(actor_context, retry_store, update_message,
				source -> new BucketActionMessage.UpdateBucketActionMessage
							(new_object, !status.suspended(), old_version, new HashSet<String>(Arrays.asList(source))));
	}	
	
	public static final String DELETE_TOUCH_FILE = ".DELETED";
	
	protected static void deleteFilePath(final DataBucketBean to_delete, final IStorageService storage_service) throws Exception {
		final FileContext dfs = storage_service.getUnderlyingPlatformDriver(FileContext.class, Optional.empty()).get();
		
		final String bucket_root = storage_service.getRootPath() + "/" + to_delete.full_name();
		
		try (final FSDataOutputStream out = 
				dfs.create(new Path(bucket_root + "/" + DELETE_TOUCH_FILE), EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE)))
		{} //(ie close after creating)
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
	
	protected static void createFilePaths(final DataBucketBean bucket, final IStorageService storage_service) throws Exception {
		final FileContext dfs = storage_service.getUnderlyingPlatformDriver(FileContext.class, Optional.empty()).get();
	
		final String bucket_root = storage_service.getRootPath() + "/" + bucket.full_name();
		
		// Check if a "delete touch file is present, bail if so"
		try {
			dfs.getFileStatus(new Path(bucket_root + "/" + DELETE_TOUCH_FILE));
			throw new RuntimeException(ErrorUtils.get(ManagementDbErrorUtils.DELETE_TOUCH_FILE_PRESENT, bucket.full_name()));
		}
		catch (FileNotFoundException fe) {} // (fine just carry on)
		
		Arrays.asList(
				bucket_root + "/managed_bucket",
				bucket_root + "/managed_bucket/logs",
				bucket_root + "/managed_bucket/logs/harvest",
				bucket_root + "/managed_bucket/logs/enrichment",
				bucket_root + "/managed_bucket/logs/storage",
				bucket_root + "/managed_bucket/assets",
				bucket_root + "/managed_bucket/import",
				bucket_root + "/managed_bucket/temp",
				bucket_root + "/managed_bucket/stored",
				bucket_root + "/managed_bucket/stored/raw",
				bucket_root + "/managed_bucket/stored/json",
				bucket_root + "/managed_bucket/stored/processed",
				bucket_root + "/managed_bucket/ready"
				)
				.stream()
				.map(s -> new Path(s))
				.forEach(Lambdas.wrap_consumer_u(p -> dfs.mkdir(p, FsPermission.getDefault(), true)));
	}
	
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
					// Grouping times
					if (null != bean.data_schema().storage_schema().json_grouping_time_period()) {
						TimeUtils.getTimePeriod(bean.data_schema().storage_schema().json_grouping_time_period())
							.f().forEach(s -> errs.add(ErrorUtils.get(ManagementDbErrorUtils.BUCKET_INVALID_TIME, bean.full_name(), "data_schema.storage_schema.json_grouping_time_period", s)));						
					}
					if (null != bean.data_schema().storage_schema().raw_grouping_time_period()) {
						TimeUtils.getTimePeriod(bean.data_schema().storage_schema().raw_grouping_time_period())
							.f().forEach(s -> errs.add(ErrorUtils.get(ManagementDbErrorUtils.BUCKET_INVALID_TIME, bean.full_name(), "data_schema.storage_schema.raw_grouping_time_period", s)));						
					}
					if (null != bean.data_schema().storage_schema().processed_grouping_time_period()) {
						TimeUtils.getTimePeriod(bean.data_schema().storage_schema().raw_grouping_time_period())
							.f().forEach(s -> errs.add(ErrorUtils.get(ManagementDbErrorUtils.BUCKET_INVALID_TIME, bean.full_name(), "data_schema.storage_schema.processed_grouping_time_period", s)));						
					}
					// Max ages
					if (null != bean.data_schema().storage_schema().json_exist_age_max()) {
						TimeUtils.getDuration(bean.data_schema().storage_schema().json_exist_age_max())
							.f().forEach(s -> errs.add(ErrorUtils.get(ManagementDbErrorUtils.BUCKET_INVALID_TIME, bean.full_name(), "data_schema.storage_schema.json_exist_age_max", s)));						
					}
					if (null != bean.data_schema().storage_schema().raw_exist_age_max()) {
						TimeUtils.getDuration(bean.data_schema().storage_schema().raw_exist_age_max())
							.f().forEach(s -> errs.add(ErrorUtils.get(ManagementDbErrorUtils.BUCKET_INVALID_TIME, bean.full_name(), "data_schema.storage_schema.raw_exist_age_max", s)));						
					}
					if (null != bean.data_schema().storage_schema().processed_exist_age_max()) {
						TimeUtils.getDuration(bean.data_schema().storage_schema().json_exist_age_max())
							.f().forEach(s -> errs.add(ErrorUtils.get(ManagementDbErrorUtils.BUCKET_INVALID_TIME, bean.full_name(), "data_schema.storage_schema.processed_exist_age_max", s)));						
					}
				}				
			}
		}		
		return errs;
	}
}
