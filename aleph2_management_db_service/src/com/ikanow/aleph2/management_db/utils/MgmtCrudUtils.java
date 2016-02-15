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
package com.ikanow.aleph2.management_db.utils;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import scala.Tuple2;

import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.Cursor;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.FutureUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Optionals;
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
	public static BasicMessageBean createValidationError(final String error) {
		return new BasicMessageBean(
				new Date(), // date
				false, // success
				IManagementDbService.CORE_MANAGEMENT_DB.get(),
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
			final DataBucketBean bucket, 
			final ManagementDbActorContext actor_context,
			final ICrudService<BucketActionRetryMessage> retry_store,
			final T mgmt_operation,
			final Function<String, T> clone_lambda_with_source
			)
	{		
		final boolean multi_node_enabled = Optional.ofNullable(bucket.multi_node_enabled()).orElse(false);
				
		final CompletableFuture<BucketActionCollectedRepliesMessage> f =
				BucketActionSupervisor.askBucketActionActor(
						Optional.of(multi_node_enabled || !Optional.ofNullable(mgmt_operation.handling_clients()).orElse(Collections.emptySet()).isEmpty())
						,
						actor_context.getBucketActionSupervisor(), actor_context.getActorSystem(),
						(BucketActionMessage)mgmt_operation, 
						Optional.empty());
		
		// Handle retry 
		return f.thenAccept(replies -> {
			// (enough has gone wrong already - just fire and forget this)
			replies.timed_out().stream().forEach(source -> {
				retry_store.storeObject(
						new BucketActionRetryMessage(source, clone_lambda_with_source.apply(source)
								));
			});
		})
		// Build user error message
		.thenCompose(__ -> buildBucketReplyForUser(bucket, f))
		;
		
	}
	
	/** Utility to append some useful diagnostic information to the replies returned from the DIM 
	 * @param bucket
	 * @param f
	 * @return
	 */
	static public <T extends BucketActionMessage> CompletableFuture<Collection<BasicMessageBean>> buildBucketReplyForUser(
			final DataBucketBean bucket,
			final CompletableFuture<BucketActionCollectedRepliesMessage> f)
	{
		final CompletableFuture<Collection<BasicMessageBean>> management_results =
				f.<Collection<BasicMessageBean>>thenApply(replies -> {
					
					final int rejected = Optionals.ofNullable(replies.rejected()).size();
					final int timed_out = Optionals.ofNullable(replies.timed_out()).size();
					return Optional.of(Lambdas.get(() -> {
						if (replies.replies().isEmpty()) {
							boolean some_timed_out = timed_out > 0;
							boolean some_rejected = rejected > 0;
							
							return Arrays.asList(
									ErrorUtils.buildErrorMessage(MgmtCrudUtils.class.getSimpleName(), "applyRetriableManagementOperation",
											Lambdas.get(() -> {
												if (some_rejected && some_timed_out) {
													return ManagementDbErrorUtils.ALL_NODES_TIMED_OUT_OR_REJECTED;
												}
												else if (some_rejected) {
													return ManagementDbErrorUtils.ALL_NODES_REJECTED_BUCKET;
												}
												else if (some_timed_out) {
													return ManagementDbErrorUtils.ALL_NODES_TIMED_OUT;													
												}
												else {
													return ManagementDbErrorUtils.NO_NODES_AVAILABLE;
												}
											}),
											bucket.full_name(), timed_out, rejected));							
						}
						else return replies.replies();
					}))
					.map(l -> {
						if (rejected > 0) { // (add an INFO message)
							return Stream.concat(l.stream(),
									Arrays.asList(
											ErrorUtils.buildSuccessMessage(MgmtCrudUtils.class.getSimpleName(), "applyRetriableManagementOperation", 
														ManagementDbErrorUtils.SOME_NODES_REJECTED_BUCKET, 
															bucket.full_name(),
															rejected,
															replies.rejected().stream().limit(3).sorted().collect(Collectors.joining(";"))
															)
											).stream()
									).collect(Collectors.toList());
						}
						else {
							return l;
						}
					})
					.map(l -> {
						if (timed_out > 0) { // (add an INFO message)
							return Stream.concat(l.stream(),
									Arrays.asList(
											ErrorUtils.buildSuccessMessage(MgmtCrudUtils.class.getSimpleName(), "applyRetriableManagementOperation", 
														ManagementDbErrorUtils.SOME_NODES_TIMED_OUT, 
															bucket.full_name(),
															timed_out,
															replies.timed_out().stream().limit(3).sorted().collect(Collectors.joining(";"))
															)
											).stream()
									).collect(Collectors.toList());
						}
						else {
							return l;
						}
					})
					.get()
					;
				})
				;
		
		return management_results;		
	}
	
	/** Handles the case where no nodes reply - still perform the operation but then suspend the bucket (user will have to unsuspend once nodes are available)
	 * @param bucket
	 * @param is_suspended
	 * @param return_from_handlers
	 * @param status_store
	 * @return
	 */
	static public CompletableFuture<Collection<BasicMessageBean>> handleUpdatingStatus(
			final DataBucketBean bucket,
			final DataBucketStatusBean status,
			final boolean is_suspended,
			final CompletableFuture<Collection<BasicMessageBean>> return_from_handlers,
			final ICrudService<DataBucketStatusBean> status_store
			)
	{
		return return_from_handlers.thenApply(results -> {
			if (results.isEmpty()) { // uh oh, nobody answered, so we're going to generate an error after all and suspend it
				if (!is_suspended) { // suspend it
					try {
						status_store.updateObjectById(bucket._id(),
								CrudUtils.update(DataBucketStatusBean.class)
									.set(DataBucketStatusBean::suspended, true)
									.set(DataBucketStatusBean::confirmed_suspended, true)
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
			else if (results.stream().allMatch(m -> m.success())) { // A couple of other checks when no errors occur:
				Optional.of(Tuples._2T(false, CrudUtils.update(DataBucketStatusBean.class)))
						// If we weren't confirmed suspended before, then change that
						.map(change_update -> {
							return (Boolean.valueOf(is_suspended) != status.confirmed_suspended()) 
									? Tuples._2T(true, change_update._2().set(DataBucketStatusBean::confirmed_suspended, is_suspended))
									: Tuples._2T(change_update._1(), change_update._2());
						})
						// If we weren't confirmed multi-node before, then change that
						.map(change_update -> {
							return (bucket.multi_node_enabled() != status.confirmed_multi_node_enabled()) 
									? Tuples._2T(true,change_update._2().set(DataBucketStatusBean::confirmed_multi_node_enabled, bucket.multi_node_enabled()))
									: Tuples._2T(change_update._1(), change_update._2());
						})
						// Confirm master enrichment type, if changed
						.map(change_update -> {
							return (bucket.master_enrichment_type() != status.confirmed_master_enrichment_type()) 
									? Tuples._2T(true,change_update._2().set(DataBucketStatusBean::confirmed_master_enrichment_type, bucket.master_enrichment_type()))
									: Tuples._2T(change_update._1(), change_update._2());
						})
						.ifPresent(change_update -> {
							if (change_update._1()) {
								try {
									status_store.updateObjectById(bucket._id(), change_update._2()).get();
								}
								catch (Exception e) {
									LinkedList<BasicMessageBean> new_results = new LinkedList<>();
									new_results.addAll(results);
									new_results.add(createValidationError(ErrorUtils.getLongForm("{1}: {0}", e, bucket.full_name())));
								} 
								// (wait until complete)								
							}
						})
						;
				return results;
			}
			else {
				return results;				
			}
		});
	}
	
	/** Decides what the node affinity application strategy is based on lock_to_node, bucket type
	 * @param bucket
	 * @param status_store
	 * @param management_results
	 * @return
	 */
	public static CompletableFuture<Boolean> applyNodeAffinityWrapper(
			final DataBucketBean bucket, 
			final ICrudService<DataBucketStatusBean> status_store, 
			final CompletableFuture<Collection<BasicMessageBean>> management_results)
	{
		// Currently a bucket must either be:
		// - pure analytic thread (possibly with lock_to_nodes enabled)
		// - normal bucket with no lock_to_node analytic jobs
		// (In the former case - By construction, all the jobs have the same setting, so:)
		// THIS WILL NEED TO GET A LOT MORE COMPLICATED ONCE WE DECOUPLE THE JOBS/BUCKET RESTRICTIONS (if ever!)
		final boolean is_pure_analytic_thread = (null == bucket.harvest_technology_name_or_id());
		final boolean lock_to_nodes = 
				!is_pure_analytic_thread
				? Optional.ofNullable(bucket.lock_to_nodes()).orElse(true)
				: Optionals.of(() -> bucket.analytic_thread().jobs().stream().findAny().map(j -> j.lock_to_nodes()).get()).orElse(false);				
		
		final CompletableFuture<Boolean> update_future = 
				lock_to_nodes
				? 					
				MgmtCrudUtils.applyNodeAffinity(bucket._id(), status_store, 
						MgmtCrudUtils.getSuccessfulNodes(management_results, 
								is_pure_analytic_thread 
								?
								SuccessfulNodeType.all_technologies // pure analytic_thead if lock_to_nodes is set then do lock from analytic technologies
								:
								SuccessfulNodeType.harvest_only // harvest thread - don't lock analytic technologies
								))
				:
				CompletableFuture.completedFuture(true)
				;
		return update_future;
	}
	
	/** Applies the node affinity obtained from "applyCrudPredicate" to the designated bucket
	 * @param bucket_id
	 * @param nodes_future
	 * @param status_store
	 * @return
	 */
	static public CompletableFuture<Boolean> applyNodeAffinity(final String bucket_id,
			final ICrudService<DataBucketStatusBean> status_store,
			final CompletableFuture<Set<String>> nodes_future)
	{
		return nodes_future.thenCompose(nodes -> {
			return status_store.updateObjectById(bucket_id, CrudUtils.update(DataBucketStatusBean.class).set(DataBucketStatusBean::node_affinity, nodes));
		});
	}
	
	public enum SuccessfulNodeType { harvest_only, all_technologies };
	
	/** Quick utility function to extract a set of sources (hostnames) that returned with success==true
	 * @param mgmt_results - management future including non-trivial side channel
	 * @param include_analytics - for setting node affinity,
	 * @return
	 */
	static public <X> CompletableFuture<Set<String>> getSuccessfulNodes(final CompletableFuture<Collection<BasicMessageBean>> mgmt_results, final SuccessfulNodeType which_nodes) {
		return mgmt_results.thenApply(list -> {
			return list.stream()
					.filter(msg -> msg.success())
					.filter(msg -> (SuccessfulNodeType.all_technologies == which_nodes) ||
								(null == msg.command()) || !msg.command().equals(ActorUtils.BUCKET_ANALYTICS_ZOOKEEPER)) // (these are streaming enrichment messages, ignore them for node affinity purposes)
					.map(msg -> msg.source())
					.collect(Collectors.toSet());
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
		
		final CompletableFuture<Set<String>> part2 = getSuccessfulNodes(part1.getManagementResults(), SuccessfulNodeType.harvest_only);
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
