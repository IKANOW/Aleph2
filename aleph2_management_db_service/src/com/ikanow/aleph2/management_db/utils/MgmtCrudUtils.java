package com.ikanow.aleph2.management_db.utils;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.checkerframework.checker.nullness.qual.NonNull;

import scala.Tuple2;

import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.Cursor;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
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
	
	/** Applies a "CRUD predicate" (ie returning a ManagementFuture<Boolean>) to a cursor of results
	 *  and collects the main results into a list of successes, and collects the management side channels
	 * @param application_cursor
	 * @param crud_predicate
	 * @return
	 */
	static public <T> ManagementFuture<Long> applyCrudPredicate(
			CompletableFuture<Cursor<T>> application_cursor_future,
			Function<T, ManagementFuture<Boolean>> crud_predicate)
	{
		return FutureUtils.denestManagementFuture(application_cursor_future.thenApply(cursor -> {
			return applyCrudPredicate(cursor, crud_predicate);
		}));
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
