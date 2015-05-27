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
 ******************************************************************************/
package com.ikanow.aleph2.management_db.utils;

import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.checkerframework.checker.nullness.qual.NonNull;

import scala.Tuple2;

import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.Cursor;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.FutureUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.data_model.utils.FutureUtils.ManagementFuture;
import com.ikanow.aleph2.management_db.controllers.actors.BucketActionSupervisor;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionRetryMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage.BucketActionCollectedRepliesMessage;
import com.ikanow.aleph2.management_db.services.ManagementDbActorContext;

/** A collection of useful functional shortcuts applicable across a number of different CRUD services etc 
 * @author acp
 */
public class MgmtCrudUtils {

	/** Simple converter from string error into basic message bean
	 * @param error
	 * @return
	 */
	public static BasicMessageBean createValidationError(final @NonNull String error) {
		return new BasicMessageBean(
				new Date(), // date
				false, // success
				"CoreManagementDbService",
				BucketActionMessage.NewBucketActionMessage.class.getSimpleName(),
				null, // message code
				error,
				null // details						
			);
	}
	
	/** Applies a management operation that might need to be retried, collating the results and converting into
	 *  something that can be placed in the management side channel of a ManagementFuture
	 * @param actor_context - the actor context (used to send the message)
	 * @param retry_store - the retry CRUD store
	 * @param mgmt_operation - the update or delete message to apply
	 * @param clone_lambda_with_source - a function taking a string source that timed out and returning a clone of the original message but with the handling client replaced with this source
	 * @return
	 */
	static public <T extends BucketActionMessage> CompletableFuture<Collection<BasicMessageBean>> applyRetriableManagementOperation(
			final @NonNull ManagementDbActorContext actor_context,
			final @NonNull ICrudService<BucketActionRetryMessage> retry_store,
			final @NonNull T mgmt_operation,
			final @NonNull Function<String, T> clone_lambda_with_source
			)
	{		
		final CompletableFuture<BucketActionCollectedRepliesMessage> f =
				BucketActionSupervisor.askDistributionActor(
						actor_context.getBucketActionSupervisor(), 
						(BucketActionMessage)mgmt_operation, 
						Optional.empty());
		
		final CompletableFuture<Collection<BasicMessageBean>> management_results =
				f.<Collection<BasicMessageBean>>thenApply(replies -> {
					// (enough has gone wrong already - just fire and forget this)
					replies.timed_out().stream().forEach(source -> {
						retry_store.storeObject(
								new BucketActionRetryMessage(source, clone_lambda_with_source.apply(source)
										));
					});
					return replies.replies(); 
				});
		
		return management_results;
	}
	
	/** Handles the case where no nodes reply - still perform the operation but then suspend the bucket (user will have to unsuspend once nodes are available)
	 * @param bucket
	 * @param is_suspended
	 * @param return_from_handlers
	 * @param status_store
	 * @return
	 */
	static public CompletableFuture<Collection<BasicMessageBean>> handlePossibleEmptyNodeAffinity(final @NonNull DataBucketBean bucket,
			final boolean is_suspended,
			final CompletableFuture<Collection<BasicMessageBean>> return_from_handlers,
			final @NonNull ICrudService<DataBucketStatusBean> status_store
			)
	{
		return return_from_handlers.thenApply(results -> {
			if (results.isEmpty()) { // uh oh, nobody answered, so we're going to generate an error after all and suspend it
				if (!is_suspended) { // suspend it
					try {
						status_store.updateObjectById(bucket._id(),
								CrudUtils.update(DataBucketStatusBean.class).set(DataBucketStatusBean::suspended, true)
								).get();
					}
					catch (Exception e) {
						return Arrays.asList(createValidationError(
								ErrorUtils.getLongForm("{1}: {0}", e, bucket.full_name()))
								);
					} 
					// (wait until complete)
				}
				return Arrays.asList(createValidationError(
						ErrorUtils.get(ManagementDbErrorUtils.NO_DATA_IMPORT_MANAGERS_STARTED_SUSPENDED, bucket.full_name()))
						);
			}
			else {
				return results;
			}
		});
	}
	
	/** Applies the node affinity obtained from "applyCrudPredicate" to the designated bucket
	 * @param bucket_id
	 * @param nodes_future
	 * @param status_store
	 * @return
	 */
	static public CompletableFuture<Boolean> applyNodeAffinity(final @NonNull String bucket_id,
			final @NonNull ICrudService<DataBucketStatusBean> status_store,
			final @NonNull CompletableFuture<Set<String>> nodes_future)
	{
		return nodes_future.thenCompose(nodes -> {
			return status_store.updateObjectById(bucket_id, CrudUtils.update(DataBucketStatusBean.class).set(DataBucketStatusBean::node_affinity, nodes));
		});
	}
	
	/** Quick utility function to extract a set of sources (hostnames) that returned with success==true
	 * @param mgmt_results - management future including non-trivial side channel
	 * @return
	 */
	static public <X> CompletableFuture<Set<String>> getSuccessfulNodes(final @NonNull CompletableFuture<Collection<BasicMessageBean>> mgmt_results) {
		return mgmt_results.thenApply(list -> {
			return list.stream().filter(msg -> msg.success()).map(msg -> msg.source()).collect(Collectors.toSet());
		});
	}
	
	/** Applies a "CRUD predicate" (ie returning a ManagementFuture<Boolean>) to a cursor of results
	 *  and collects the main results into a list of successes, and collects the management side channels
	 * @param application_cursor
	 * @param crud_predicate
	 * @return - firstly a management future with the side channel, secondly a candidate list of nodes that say yes
	 */
	static public <T> Tuple2<ManagementFuture<Long>, CompletableFuture<Set<String>>> applyCrudPredicate(
			CompletableFuture<Cursor<T>> application_cursor_future,
			Function<T, ManagementFuture<Boolean>> crud_predicate)
	{
		final ManagementFuture<Long> part1 = 
			FutureUtils.denestManagementFuture(application_cursor_future.thenApply(cursor -> {
				return applyCrudPredicate(cursor, crud_predicate);
		}));
		
		final CompletableFuture<Set<String>> part2 = getSuccessfulNodes(part1.getManagementResults());
		return Tuples._2T(part1, part2);
	}
	
	
	/** Applies a "CRUD predicate" (ie returning a ManagementFuture<Boolean>) to a cursor of results
	 *  and collects the main results into a list of successes, and collects the management side channels
	 * @param application_cursor
	 * @param crud_predicate
	 * @return
	 */
	static public <T> ManagementFuture<Long> applyCrudPredicate(
			Cursor<T> application_cursor,
			Function<T, ManagementFuture<Boolean>> crud_predicate)
	{
		final List<Tuple2<Boolean, CompletableFuture<Collection<BasicMessageBean>>>> collected_results =
				StreamSupport.stream(application_cursor.spliterator(), false)
				.<Tuple2<Boolean, CompletableFuture<Collection<BasicMessageBean>>>>map(bucket -> {
					final ManagementFuture<Boolean> single_delete = crud_predicate.apply(bucket);
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

		final long deleted = collected_results.stream().collect(Collectors.summingLong(reply -> reply._1() ? 1 : 0));

		final List<CompletableFuture<Collection<BasicMessageBean>>> replies = 
				collected_results.stream()
				.<CompletableFuture<Collection<BasicMessageBean>>>map(reply -> reply._2())
				.collect(Collectors.toList());

		final CompletableFuture<Void> all_done_future = CompletableFuture.allOf(replies.toArray(new CompletableFuture[replies.size()]));										

		return (ManagementFuture<Long>) FutureUtils.createManagementFuture(
				CompletableFuture.completedFuture(deleted), 
				all_done_future.thenApply(__ -> 
				replies.stream().flatMap(reply -> reply.join().stream()).collect(Collectors.toList())));
		//(note: join shouldn't be able to throw here since we've already called .get() without incurring an exception if we're here)		
	}
}
