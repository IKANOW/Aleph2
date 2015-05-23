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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.checkerframework.checker.nullness.qual.NonNull;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IBasicSearchService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.ProjectBean;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.FutureUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent;
import com.ikanow.aleph2.data_model.utils.FutureUtils.ManagementFuture;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.management_db.controllers.actors.BucketActionSupervisor;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage.BucketActionCollectedRepliesMessage;import com.ikanow.aleph2.management_db.data_model.BucketActionRetryMessage;


/**
 * @author acp
 *
 */
public class DataBucketCrudService implements IManagementCrudService<DataBucketBean> {

	protected final IManagementDbService _underlying_management_db;
	
	protected final ICrudService<DataBucketBean> _underlying_data_bucket_db;
	protected final ICrudService<DataBucketStatusBean> _underlying_data_bucket_status_db;
	protected final ICrudService<BucketActionRetryMessage> _bucket_action_retry_store;
	
	protected final ManagementDbActorContext _actor_context;
	
	/** Guice invoked constructor
	 * @param underlying_management_db
	 */
	@Inject
	public DataBucketCrudService(final IServiceContext service_context, ManagementDbActorContext actor_context)
	{
		_underlying_management_db = service_context.getService(IManagementDbService.class, Optional.empty());
		_underlying_data_bucket_db = _underlying_management_db.getDataBucketStore();
		_underlying_data_bucket_status_db = _underlying_management_db.getDataBucketStatusStore();
		_bucket_action_retry_store = _underlying_management_db.getRetryStore(BucketActionRetryMessage.class);
		
		_actor_context = actor_context;
	}

	/** User constructor, for wrapping
	 * @param underlying_management_db
	 * @param underlying_data_bucket_db
	 */
	public DataBucketCrudService(final @NonNull IManagementDbService underlying_management_db, 
			final @NonNull ICrudService<DataBucketBean> underlying_data_bucket_db)
	{
		_underlying_management_db = underlying_management_db;
		_underlying_data_bucket_db = underlying_data_bucket_db;
		_underlying_data_bucket_status_db = _underlying_management_db.getDataBucketStatusStore();
		_bucket_action_retry_store = _underlying_management_db.getRetryStore(BucketActionRetryMessage.class);
		_actor_context = ManagementDbActorContext.get();		
	}
		
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getFilteredRepo(java.lang.String, java.util.Optional, java.util.Optional)
	 */
	@NonNull
	public IManagementCrudService<DataBucketBean> getFilteredRepo(
			final @NonNull String authorization_fieldname,
			final @NonNull Optional<AuthorizationBean> client_auth,
			final @NonNull Optional<ProjectBean> project_auth) 
	{
		return new DataBucketCrudService(_underlying_management_db.getFilteredDb(client_auth, project_auth), 
				_underlying_data_bucket_db.getFilteredRepo(authorization_fieldname, client_auth, project_auth));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#storeObject(java.lang.Object, boolean)
	 */
	@NonNull
	public ManagementFuture<Supplier<Object>> storeObject(final @NonNull DataBucketBean new_object, final boolean replace_if_present)
	{
		//TODO Bucket validation:
		// - Does the user have access rights on the parent directories
		// - Do the directories exist? 
		
		if (replace_if_present && (null != new_object._id())) {
			//TODO check if the object is present, if not then it's an update return that instead
		}
		try {
			// OK if the bucket is validated we can store it (and create a status object)
					
			CompletableFuture<Supplier<Object>> ret_val = _underlying_data_bucket_db.storeObject(new_object, false);

			// Check if the low level store has failed:
			Object id = null;
			try {
				id = ret_val.get().get().toString();
			}
			catch (Exception e) { // just pass the raw result back up
				return FutureUtils.createManagementFuture(ret_val);
			}			
			// Create the directories
			
			// We've created a new bucket but is it enabled or not?
			
			CompletableFuture<Optional<DataBucketStatusBean>> bucket_status = _underlying_data_bucket_status_db.getObjectById(id);
			@SuppressWarnings("unused")
			Optional<DataBucketStatusBean> status = null;
			try {
				status = bucket_status.get();
			}
			catch (Exception e) { // just pass the raw result back up
				// Hmm not sure what's going on - just treat this like the status didn't exist:
				status = Optional.empty();
			}			

			// OK if we're here then it's time to notify any interested harvesters

			
			//TODO
		}
		catch (Exception e) {
			//TODO
		}
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#storeObject(java.lang.Object)
	 */
	@NonNull
	public ManagementFuture<Supplier<Object>> storeObject(final @NonNull DataBucketBean new_object) {
		return this.storeObject(new_object, false);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#storeObjects(java.util.List, boolean)
	 */
	@NonNull
	public ManagementFuture<Tuple2<Supplier<List<Object>>, Supplier<Long>>> storeObjects(final @NonNull List<DataBucketBean> new_objects, final boolean continue_on_error) {
		throw new RuntimeException("This method is not supported, call storeObject on each object separately");
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#storeObjects(java.util.List)
	 */
	@NonNull
	public ManagementFuture<Tuple2<Supplier<List<Object>>, Supplier<Long>>> storeObjects(final @NonNull List<DataBucketBean> new_objects) {
		return this.storeObjects(new_objects, false);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#optimizeQuery(java.util.List)
	 */
	@NonNull
	public ManagementFuture<Boolean> optimizeQuery(final @NonNull List<String> ordered_field_list) {
		return FutureUtils.createManagementFuture(_underlying_data_bucket_db.optimizeQuery(ordered_field_list));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deregisterOptimizedQuery(java.util.List)
	 */
	public boolean deregisterOptimizedQuery(final @NonNull List<String> ordered_field_list) {
		return _underlying_data_bucket_db.deregisterOptimizedQuery(ordered_field_list);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	@NonNull
	public ManagementFuture<Optional<DataBucketBean>> getObjectBySpec(final @NonNull QueryComponent<DataBucketBean> unique_spec) {
		return FutureUtils.createManagementFuture(_underlying_data_bucket_db.getObjectBySpec(unique_spec));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.List, boolean)
	 */
	@NonNull
	public ManagementFuture<Optional<DataBucketBean>> getObjectBySpec(
			final @NonNull QueryComponent<DataBucketBean> unique_spec,
			final @NonNull List<String> field_list, final boolean include) {
		return FutureUtils.createManagementFuture(_underlying_data_bucket_db.getObjectBySpec(unique_spec, field_list, include));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectById(java.lang.Object)
	 */
	@NonNull
	public ManagementFuture<Optional<DataBucketBean>> getObjectById(final @NonNull Object id) {
		return FutureUtils.createManagementFuture(_underlying_data_bucket_db.getObjectById(id));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectById(java.lang.Object, java.util.List, boolean)
	 */
	@NonNull
	public ManagementFuture<Optional<DataBucketBean>> getObjectById(final @NonNull Object id,
			final @NonNull List<String> field_list, final boolean include) {
		return FutureUtils.createManagementFuture(_underlying_data_bucket_db.getObjectById(id, field_list, include));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	@NonNull
	public ManagementFuture<ICrudService.Cursor<DataBucketBean>> getObjectsBySpec(
			final @NonNull QueryComponent<DataBucketBean> spec)
	{
		return FutureUtils.createManagementFuture(_underlying_data_bucket_db.getObjectsBySpec(spec));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.List, boolean)
	 */
	@NonNull
	public ManagementFuture<ICrudService.Cursor<DataBucketBean>> getObjectsBySpec(
			final @NonNull QueryComponent<DataBucketBean> spec, final @NonNull List<String> field_list,
			final boolean include)
	{
		return FutureUtils.createManagementFuture(_underlying_data_bucket_db.getObjectsBySpec(spec, field_list, include));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#countObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	@NonNull
	public ManagementFuture<Long> countObjectsBySpec(final @NonNull QueryComponent<DataBucketBean> spec) {
		return FutureUtils.createManagementFuture(_underlying_data_bucket_db.countObjectsBySpec(spec));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#countObjects()
	 */
	@NonNull
	public ManagementFuture<Long> countObjects() {
		return FutureUtils.createManagementFuture(_underlying_data_bucket_db.countObjects());
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deleteObjectById(java.lang.Object)
	 */
	@NonNull
	public ManagementFuture<Boolean> deleteObjectById(final @NonNull Object id) {		
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
	@NonNull
	public ManagementFuture<Boolean> deleteObjectBySpec(final @NonNull QueryComponent<DataBucketBean> unique_spec) {
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
	public ManagementFuture<Long> deleteObjectsBySpec(final @NonNull QueryComponent<DataBucketBean> spec) {
		
		// Need to do these one by one:

		final CompletableFuture<Cursor<DataBucketBean>> to_delete = _underlying_data_bucket_db.getObjectsBySpec(spec);
		
		try {
			final List<Tuple2<Boolean, CompletableFuture<Collection<BasicMessageBean>>>> collected_deletes =
				StreamSupport.stream(to_delete.get().spliterator(), false)
					.<Tuple2<Boolean, CompletableFuture<Collection<BasicMessageBean>>>>map(bucket -> {
						final ManagementFuture<Boolean> single_delete = deleteBucket(bucket);
						try { // check it doesn't do anything horrible
							return Tuples._2T(single_delete.get(), single_delete.getManagementResults());
						}
						catch (Exception e) {
							// Something went wrong, this is bad - just carry on though, there's not much to be
							// done and this shouldn't ever happen anyway
							return null;
						}
					})
					.filter(reply -> null != reply)
					.collect(Collectors.toList());
			
			final long deleted = collected_deletes.stream().collect(Collectors.summingLong(reply -> reply._1() ? 1 : 0));
			
			final List<CompletableFuture<Collection<BasicMessageBean>>> replies = 
					collected_deletes.stream()
							.<CompletableFuture<Collection<BasicMessageBean>>>map(reply -> reply._2())
							.collect(Collectors.toList());
			
			final CompletableFuture<Void> all_done_future = CompletableFuture.allOf(replies.toArray(new CompletableFuture[replies.size()]));										
			
			return (ManagementFuture<Long>) FutureUtils.createManagementFuture(
					CompletableFuture.completedFuture(deleted), 
					all_done_future.thenApply(__ -> 
							replies.stream().flatMap(reply -> reply.join().stream()).collect(Collectors.toList())));
			//(note: join shouldn't be able to throw here since we've already called .get() without incurring an exception if we're here)
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
	@NonNull
	private ManagementFuture<Boolean> deleteBucket(final @NonNull DataBucketBean to_delete) {
		try {
			final CompletableFuture<Boolean> delete_reply = _underlying_data_bucket_db.deleteObjectById(to_delete._id());
			
			if (delete_reply.get()) {
			
				// Get the status and delete it:
				
				final CompletableFuture<Optional<DataBucketStatusBean>> future_status_bean =
					_underlying_data_bucket_status_db.updateAndReturnObjectBySpec(
							CrudUtils.allOf(DataBucketStatusBean.class).when(DataBucketStatusBean::_id, to_delete._id()),
							Optional.empty(), CrudUtils.update(DataBucketStatusBean.class).deleteObject(), Optional.of(true), Collections.emptyList(), false);

				final Supplier<Optional<DataBucketStatusBean>> lambda = () -> {
					try {
						return future_status_bean.get(); 
					}
					catch (Exception e) { // just treat this as not finding anything
						return Optional.empty();
					}					
				};
				final Optional<DataBucketStatusBean> status_bean = lambda.get();
				
				BucketActionMessage.DeleteBucketActionMessage delete_message = new
						BucketActionMessage.DeleteBucketActionMessage(to_delete, 
								new HashSet<String>(
										Optional.ofNullable(
												status_bean.isPresent() ? status_bean.get().node_affinity() : null)
										.orElse(Collections.emptyList())
										));
				
				final CompletableFuture<BucketActionCollectedRepliesMessage> f =
						BucketActionSupervisor.askDistributionActor(
								_actor_context.getBucketActionSupervisor(), 
								(BucketActionMessage)delete_message, 
								Optional.empty());
				
				CompletableFuture<Collection<BasicMessageBean>> management_results =
						f.<Collection<BasicMessageBean>>thenApply(replies -> {
							// (enough has gone wrong already - just fire and forget this)
							replies.timed_out().stream().forEach(source -> {
								_bucket_action_retry_store.storeObject(
										new BucketActionRetryMessage(source,
												// (can't clone the message because it's not a bean, but the c'tor is very simple)
												new BucketActionMessage.DeleteBucketActionMessage(
														delete_message.bucket(),
														new HashSet<String>(Arrays.asList(source))
														)										
												));
							});
							return replies.replies(); 
						});

				// Convert BucketActionCollectedRepliesMessage into a management side-channel:
				return FutureUtils.createManagementFuture(delete_reply, management_results);					
			}	
			else {
				return FutureUtils.createManagementFuture(delete_reply);
			}
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
	@NonNull
	public ManagementFuture<Boolean> deleteDatastore() {
		throw new RuntimeException("This method is not supported");
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getRawCrudService()
	 */
	@NonNull
	public IManagementCrudService<JsonNode> getRawCrudService() {
		throw new RuntimeException("DataBucketCrudService.getRawCrudService not supported");
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getSearchService()
	 */
	@NonNull
	public Optional<IBasicSearchService<DataBucketBean>> getSearchService() {
		return _underlying_data_bucket_db.getSearchService();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getUnderlyingPlatformDriver(java.lang.Class, java.util.Optional)
	 */
	@SuppressWarnings("unchecked")
	@NonNull
	public <T> T getUnderlyingPlatformDriver(final @NonNull Class<T> driver_class,
			final @NonNull Optional<String> driver_options)
	{
		if (driver_class == ICrudService.class) {
			return (@NonNull T) _underlying_data_bucket_db;
		}
		else {
			throw new RuntimeException("DataBucketCrudService.getUnderlyingPlatformDriver not supported");
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#updateObjectById(java.lang.Object, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent)
	 */
	@Override
	public @NonNull 
	ManagementFuture<Boolean> updateObjectById(
			final @NonNull Object id, final @NonNull UpdateComponent<DataBucketBean> update) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#updateObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.Optional, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent)
	 */
	@Override
	public @NonNull ManagementFuture<Boolean> updateObjectBySpec(
			final @NonNull QueryComponent<DataBucketBean> unique_spec,
			final @NonNull Optional<Boolean> upsert,
			final @NonNull UpdateComponent<DataBucketBean> update) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#updateObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.Optional, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent)
	 */
	@Override
	public @NonNull ManagementFuture<Long> updateObjectsBySpec(
			final @NonNull QueryComponent<DataBucketBean> spec,
			final @NonNull Optional<Boolean> upsert,
			final @NonNull UpdateComponent<DataBucketBean> update) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#updateAndReturnObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.Optional, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent, java.util.Optional, java.util.List, boolean)
	 */
	@Override
	public @NonNull ManagementFuture<Optional<DataBucketBean>> updateAndReturnObjectBySpec(
			final @NonNull QueryComponent<DataBucketBean> unique_spec,
			final @NonNull Optional<Boolean> upsert,
			final @NonNull UpdateComponent<DataBucketBean> update,
			final @NonNull Optional<Boolean> before_updated, @NonNull List<String> field_list,
			final boolean include) {
		// TODO Auto-generated method stub
		return null;
	}

}
