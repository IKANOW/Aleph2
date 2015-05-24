package com.ikanow.aleph2.management_db.utils;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import scala.Tuple2;

import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.Cursor;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.FutureUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.data_model.utils.FutureUtils.ManagementFuture;

/** A collection of useful functional shortcuts applicable across a number of different CRUD services etc 
 * @author acp
 */
public class MgmtCrudUtils {

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
