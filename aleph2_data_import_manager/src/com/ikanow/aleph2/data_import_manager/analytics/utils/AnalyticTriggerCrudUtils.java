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
package com.ikanow.aleph2.data_import_manager.analytics.utils;

import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import scala.Tuple2;

import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean.AnalyticThreadComplexTriggerBean.TriggerType;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils.MethodNamingHelper;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerStateBean;

/** A set of utilities for retrieving and updating the trigger state
 * @author Alex
 */
public class AnalyticTriggerCrudUtils {

	public final static long ACTIVE_CHECK_FREQ_SECS = 10L;
	
	///////////////////////////////////////////////////////////////////////
	
	// CREATE UTILTIES
	
	/** Creates a special record that just exists to indicate that a job is active
	 * @param trigger_crud
	 * @param bucket
	 * @param job
	 * @param locked_to_host
	 */
	public static CompletableFuture<?> createActiveJobRecord(final ICrudService<AnalyticTriggerStateBean> trigger_crud, final DataBucketBean bucket, final AnalyticThreadJobBean job, final Optional<String> locked_to_host) {
		final AnalyticTriggerStateBean new_entry_pre =
				BeanTemplateUtils.build(AnalyticTriggerStateBean.class)
					.with(AnalyticTriggerStateBean::is_bucket_active, true)
					.with(AnalyticTriggerStateBean::is_job_active, true)
					.with(AnalyticTriggerStateBean::bucket_id, bucket._id())
					.with(AnalyticTriggerStateBean::bucket_name, bucket.full_name())
					.with(AnalyticTriggerStateBean::job_name, job.name())
					.with(AnalyticTriggerStateBean::trigger_type, TriggerType.none)
					.with(AnalyticTriggerStateBean::last_checked, Date.from(Instant.now()))
					.with(AnalyticTriggerStateBean::next_check, Date.from(Instant.now().plusSeconds(ACTIVE_CHECK_FREQ_SECS)))
					.with(AnalyticTriggerStateBean::locked_to_host, locked_to_host.orElse(null))
				.done().get();
		
		final AnalyticTriggerStateBean new_entry = BeanTemplateUtils.clone(new_entry_pre)
													.with(AnalyticTriggerStateBean::_id, AnalyticTriggerStateBean.buildId(new_entry_pre, false))
													.done();
		
		return trigger_crud.storeObject(new_entry, true); //(fire and forget - don't use a list because storeObjects with replace can be problematic for some DBs)
		
		// Also: update the is_job_active for all other triggers
	}
	
	/** Stores/updates the set of triggers derived from the bucket (replacing any previous ones ... with the exception that if the job to be replaced
	 *  is active then the new set are stored as active  
	 * @param trigger_crud
	 * @param triggers
	 */
	public static CompletableFuture<?> storeOrUpdateTriggerStage(final ICrudService<AnalyticTriggerStateBean> trigger_crud, final Map<Tuple2<String, String>, List<AnalyticTriggerStateBean>> triggers) {
		
		final Date now = Date.from(Instant.now());
		
		final Stream<CompletableFuture<?>> ret = triggers.values().stream()
			.flatMap(l -> l.stream())
			.collect(Collectors.groupingBy(v -> Tuples._3T(v.bucket_name(), v.job_name(), v.locked_to_host())))
			.entrySet()
			.stream()
			.flatMap(kv -> {
				final String bucket_name = kv.getKey()._1();
				final String job_name = kv.getKey()._2();
				final Optional<String> locked_to_host = Optional.ofNullable(kv.getKey()._3());
				
				// Step 1: is the bucket/job active?
				final boolean is_active = isAnalyticBucketOrJobActive(trigger_crud, bucket_name, Optional.ofNullable(job_name), locked_to_host).join();
				
				// Step 2: write out the transformed job, with instructions to check in <5s and last_checked == now
				final CompletableFuture<?> f1 = trigger_crud.storeObjects(
						kv.getValue().stream()
							.map(trigger ->
								BeanTemplateUtils.clone(trigger)
									.with(AnalyticTriggerStateBean::_id, AnalyticTriggerStateBean.buildId(trigger, !is_active))
									.with(AnalyticTriggerStateBean::is_pending, !is_active)
									.with(AnalyticTriggerStateBean::last_checked, now)
									.with(AnalyticTriggerStateBean::next_check, now)
								.done()
							)
							.collect(Collectors.toList())
						, 
						true);
				
				// Step 3: then remove any existing entries with lesser last_checked (for that job)
				
				final CompletableFuture<?> f2 = deleteOldTriggers(trigger_crud, bucket_name, Optional.ofNullable(job_name), locked_to_host, now);
				
				return Stream.of(f1, f2);
			});
			;		
			
		final CompletableFuture<?> combined[] = ret.toArray(size -> new CompletableFuture[size]);
		
		return CompletableFuture.allOf(combined).exceptionally(__ -> null);			
	}
	
	///////////////////////////////////////////////////////////////////////
	
	// READ UTILTIES
	
	/** Get a list of triggers indexed by bucket (plus host for host-locked jobs) to examine for completion/triggering
	 *  May at some point need to update the next_check time on the underlying database records (currently doesn't, we'll let the interprocess mutex sort that out)
	 *  Here's the set of triggers that we want to get
	 *  1) External triggers for inactive buckets
	 *  2) Internal triggers for active buckets 
	 *  3) Active jobs
	 * @param trigger_crud
	 * @return
	 */
	public static CompletableFuture<Map<Tuple2<String, String>, List<AnalyticTriggerStateBean>>> getTriggersToCheck(final ICrudService<AnalyticTriggerStateBean> trigger_crud) {		
		
		final Date now = Date.from(Instant.now());
		
		// These queries want the following optimizations:
		// (next_check)
		
		final QueryComponent<AnalyticTriggerStateBean> active_job_query =
				CrudUtils.allOf(AnalyticTriggerStateBean.class)
					.rangeBelow(AnalyticTriggerStateBean::next_check, now, true)
					.when(AnalyticTriggerStateBean::is_job_active, true)
					.when(AnalyticTriggerStateBean::is_pending, false)
					.when(AnalyticTriggerStateBean::trigger_type, TriggerType.none); //(ie only returns special "is job active" records, not the input triggers)
		
		final QueryComponent<AnalyticTriggerStateBean> external_query =
				CrudUtils.allOf(AnalyticTriggerStateBean.class)
					.rangeBelow(AnalyticTriggerStateBean::next_check, now, true)
					.when(AnalyticTriggerStateBean::is_bucket_active, false)
					.when(AnalyticTriggerStateBean::is_bucket_suspended, false)
					.when(AnalyticTriggerStateBean::is_pending, false)
					.withNotPresent(AnalyticTriggerStateBean::job_name);
				
		final QueryComponent<AnalyticTriggerStateBean> internal_query =
				CrudUtils.allOf(AnalyticTriggerStateBean.class)
					.rangeBelow(AnalyticTriggerStateBean::next_check, now, true)
					.when(AnalyticTriggerStateBean::is_bucket_active, true)
					.when(AnalyticTriggerStateBean::is_pending, false)
					.withPresent(AnalyticTriggerStateBean::job_name);
						
		final QueryComponent<AnalyticTriggerStateBean> query = CrudUtils.anyOf(Arrays.asList(active_job_query, external_query, internal_query));
		
		return trigger_crud.getObjectsBySpec(query).thenApply(cursor -> 
				StreamSupport.stream(cursor.spliterator(), false)
							.collect(Collectors.<AnalyticTriggerStateBean, Tuple2<String, String>>
								groupingBy(trigger -> Tuples._2T(trigger.bucket_name(), trigger.locked_to_host()))));		
	}
	
	/** Checks the DB to see whether a bucket job is currently active
	 * @param analytic_bucket
	 * @param job
	 * @return
	 */
	public static CompletableFuture<Boolean> isAnalyticBucketOrJobActive(final ICrudService<AnalyticTriggerStateBean> trigger_crud, final String bucket_name, final Optional<String> job_name, final Optional<String> locked_to_host) {
		
		// These queries want the following optimizations:
		// (bucket_name, job_name, trigger_type, is_job_active)
		
		final QueryComponent<AnalyticTriggerStateBean> query = 
				Optional.of(CrudUtils.allOf(AnalyticTriggerStateBean.class)
							.when(AnalyticTriggerStateBean::bucket_name, bucket_name)
							.when(AnalyticTriggerStateBean::trigger_type, TriggerType.none)
							.when(AnalyticTriggerStateBean::is_job_active, true)) 				
				.map(q -> locked_to_host.map(host -> q.when(AnalyticTriggerStateBean::locked_to_host, host)).orElse(q))
				.map(q -> job_name.map(j -> q.when(AnalyticTriggerStateBean::job_name, j)).orElse(q))
				.get()
				.limit(1)
				;
					
		return trigger_crud.countObjectsBySpec(query).thenApply(c -> c.intValue() > 0);
	}
	
	///////////////////////////////////////////////////////////////////////
	
	// UPDATE UTILTIES
	
	// See also: storeOrUpdateTriggerStage (which is create/update depending on use)
	
	/** Once a job is complete, if necessary replaces it with the pending version from the most recent update
	 * @param trigger_crud
	 * @param bucket_name
	 * @param job_name
	 * @param locked_to_host
	 */
	public static CompletableFuture<?> updateCompletedJob(final ICrudService<AnalyticTriggerStateBean> trigger_crud, final String bucket_name, final String job_name, Optional<String> locked_to_host) {
		
		// These queries want the following optimizations:
		// (bucket_name, job_name, is_pending)
		
		final QueryComponent<AnalyticTriggerStateBean> is_pending_count_query = 
				Optional.of(CrudUtils.allOf(AnalyticTriggerStateBean.class)
							.when(AnalyticTriggerStateBean::bucket_name, bucket_name)
							.when(AnalyticTriggerStateBean::job_name, job_name)
							.when(AnalyticTriggerStateBean::is_pending, true))							
				.map(q -> locked_to_host.map(host -> q.when(AnalyticTriggerStateBean::locked_to_host, host)).orElse(q))
				.get()
				.limit(1)
				;
		
		return trigger_crud.countObjectsBySpec(is_pending_count_query).thenAccept(count -> {
			if (count > 0) {
				final QueryComponent<AnalyticTriggerStateBean> is_not_pending_delete_query = 
						Optional.of(CrudUtils.allOf(AnalyticTriggerStateBean.class)
									.when(AnalyticTriggerStateBean::bucket_name, bucket_name)
									.when(AnalyticTriggerStateBean::job_name, job_name)
									.when(AnalyticTriggerStateBean::is_pending, true))							
						.map(q -> locked_to_host.map(host -> q.when(AnalyticTriggerStateBean::locked_to_host, host)).orElse(q))
						.get()
						;
				
				trigger_crud.deleteObjectsBySpec(is_not_pending_delete_query).thenCompose(__ -> {
					
					final QueryComponent<AnalyticTriggerStateBean> is_pending_update_query = 
							Optional.of(CrudUtils.allOf(AnalyticTriggerStateBean.class)
										.when(AnalyticTriggerStateBean::bucket_name, bucket_name)
										.when(AnalyticTriggerStateBean::job_name, job_name)
										.when(AnalyticTriggerStateBean::is_pending, true))							
							.map(q -> locked_to_host.map(host -> q.when(AnalyticTriggerStateBean::locked_to_host, host)).orElse(q))
							.get()
							;
					
					final UpdateComponent<AnalyticTriggerStateBean> update =
							CrudUtils.update(AnalyticTriggerStateBean.class)
								.set(AnalyticTriggerStateBean::is_pending, false);
					
					return trigger_crud.updateObjectsBySpec(is_pending_update_query, Optional.of(false), update);
				});
			}
		});
		
	}
	
	/**  Updates the statuses of a bunch of existing triggers (does it by _id on each trigger, because it's updating a set of 
	 *   resources also)
	 * @param trigger_crud
	 * @param trigger_stream
	 * @param next_check
	 * @param change_activation - if present then the activation status of this also changes
	 */
	public static CompletableFuture<?> updateTriggerStatuses(final ICrudService<AnalyticTriggerStateBean> trigger_crud,
			final Stream<AnalyticTriggerStateBean> trigger_stream,
			final Date next_check,
			final Optional<Boolean> change_activation			
			)
	{
		// These queries want the following optimizations:
		// (_id)
		
		final Stream<CompletableFuture<?>> ret = trigger_stream.parallel().map(t -> {
			final UpdateComponent<AnalyticTriggerStateBean> trigger_update =
					Optional.of(CrudUtils.update(AnalyticTriggerStateBean.class)
						.set(AnalyticTriggerStateBean::curr_resource_size, t.curr_resource_size())
						.set(AnalyticTriggerStateBean::last_checked, Date.from(Instant.now()))
						.set(AnalyticTriggerStateBean::next_check, next_check))
					.map(q -> change_activation.map(change -> {
						if (change) {
							// (note: don't set the status to active until we get back a message from the technology)
							return q.set(AnalyticTriggerStateBean::last_resource_size, t.curr_resource_size())
									.set(AnalyticTriggerStateBean::is_job_active, true) // (this hasn't been confirmed by the tech yet but if it fails we'll find out in 10s time when we poll it)
									;							
						}
						else {
							return q.set(AnalyticTriggerStateBean::is_job_active, false) 
									;							
						}
					}).orElse(q))
					.get()
				;
			return trigger_crud.updateObjectById(t._id(), trigger_update);
		});		
		
		final CompletableFuture<?> combined[] = ret.toArray(size -> new CompletableFuture[size]);
		return CompletableFuture.allOf(combined).exceptionally(__ -> null);

	}	
	
	/** When a bucket.job completes, this may trigger inputs that depend on that bucket/job 
	 * @param trigger_crud
	 * @param bucket
	 */
	public static CompletableFuture<?> updateTriggerInputsWhenJobOrBucketCompletes(
			final ICrudService<AnalyticTriggerStateBean> trigger_crud, final DataBucketBean bucket,
			final Optional<AnalyticThreadJobBean> job,
			final Optional<String> locked_to_host
			)
	{
		// These queries want the following optimizations:
		// (input_resource_combined, input_data_service)
		
		final QueryComponent<AnalyticTriggerStateBean> update_trigger_query =
				Optional.of(CrudUtils.allOf(AnalyticTriggerStateBean.class)
					.when(AnalyticTriggerStateBean::input_resource_combined, bucket.full_name() + job.map(j -> ":" + j.name()).orElse("")) 
					.withNotPresent(AnalyticTriggerStateBean::input_data_service)
					.when(AnalyticTriggerStateBean::is_bucket_active, false)
					.when(AnalyticTriggerStateBean::is_job_active, false)
					.when(AnalyticTriggerStateBean::is_pending, false)
					)
					.map(q -> locked_to_host.map(host -> q.when(AnalyticTriggerStateBean::locked_to_host, host)).orElse(q))
					.get();
				;
				
		final UpdateComponent<AnalyticTriggerStateBean> update =
				CrudUtils.update(AnalyticTriggerStateBean.class)
					.increment(AnalyticTriggerStateBean::curr_resource_size, 1L)
				;
		
		return trigger_crud.updateObjectsBySpec(update_trigger_query, Optional.of(false), update);
	}
	
	/** Updates relevant triggers to indicate that their bucket is now active
	 * @param trigger_crud
	 * @param bucket
	 */
	public static CompletableFuture<?> updateTriggersWithBucketOrJobActivation(final ICrudService<AnalyticTriggerStateBean> trigger_crud, 
			final DataBucketBean bucket, final Optional<List<AnalyticThreadJobBean>> jobs, final Optional<String> locked_to_host)
	{
		// These queries want the following optimizations:
		// (bucket_name, job_name)
				
		final QueryComponent<AnalyticTriggerStateBean> update_query = 
				Optional.of(CrudUtils.allOf(AnalyticTriggerStateBean.class)
						.when(AnalyticTriggerStateBean::bucket_name, bucket.full_name()))
						.map(q -> jobs.map(js -> q.withAny(AnalyticTriggerStateBean::job_name, js.stream().map(j -> j.name()).collect(Collectors.toList()))).orElse(q))
						.map(q -> locked_to_host.map(host -> q.when(AnalyticTriggerStateBean::locked_to_host, host)).orElse(q))
						.get()
						;
		
		final UpdateComponent<AnalyticTriggerStateBean> update = 
				Optional.of(CrudUtils.update(AnalyticTriggerStateBean.class)
						.set(AnalyticTriggerStateBean::is_bucket_active, true))
						.map(u -> jobs.map(__ -> u.set(AnalyticTriggerStateBean::is_bucket_active, true)).orElse(u))
						.get()
						;
		
		return trigger_crud.updateObjectsBySpec(update_query, Optional.of(false), update);
	}
	
	/** Updates active job records' next check times
	 * @param trigger_crud
	 */
	public static CompletableFuture<?> updateActiveJobTriggerStatus(final ICrudService<AnalyticTriggerStateBean> trigger_crud, final DataBucketBean bucket) {
		// These queries want the following optimizations:
		// (bucket_name, trigger_type)
		
		final QueryComponent<AnalyticTriggerStateBean> update_query = 
				CrudUtils.allOf(AnalyticTriggerStateBean.class)
						.when(AnalyticTriggerStateBean::bucket_name, bucket.full_name())
						.when(AnalyticTriggerStateBean::trigger_type, TriggerType.none)
						;
				
		final Instant now = Instant.now();
		
		final UpdateComponent<AnalyticTriggerStateBean> update = 
				CrudUtils.update(AnalyticTriggerStateBean.class)
						.set(AnalyticTriggerStateBean::last_checked, Date.from(now))
						.set(AnalyticTriggerStateBean::next_check, Date.from(now.plusSeconds(AnalyticTriggerCrudUtils.ACTIVE_CHECK_FREQ_SECS)))
						;												
		
		return trigger_crud.updateObjectsBySpec(update_query, Optional.of(false), update);		
	}
	
	///////////////////////////////////////////////////////////////////////
	
	// DELETE UTILTIES
	
	/** Just deletes everything for a deleted bucket
	 *  May need to make this cleverer at some point
	 * @param trigger_crud
	 * @param bucket
	 */
	public static CompletableFuture<?> deleteTriggers(final ICrudService<AnalyticTriggerStateBean> trigger_crud, final DataBucketBean bucket) {
		// These queries want the following optimizations:
		// (bucket_name)
		
		return trigger_crud.deleteObjectsBySpec(CrudUtils.allOf(AnalyticTriggerStateBean.class).when(AnalyticTriggerStateBean::bucket_name, bucket.full_name()));
	}

	/** Removes any special "active job records" associated with a set of jobs that are now known to have completed
	 * @param trigger_crud
	 * @param bucket
	 * @param jobs
	 * @param locked_to_host
	 */
	public static CompletableFuture<?> deleteActiveJobEntries(final ICrudService<AnalyticTriggerStateBean> trigger_crud, final DataBucketBean bucket, List<AnalyticThreadJobBean> jobs, final Optional<String> locked_to_host)
	{
		// These queries want the following optimizations:
		// (bucket_name, job_name, trigger_type)
		
		final QueryComponent<AnalyticTriggerStateBean> delete_query =
				Optional.of(CrudUtils.allOf(AnalyticTriggerStateBean.class)
						.when(AnalyticTriggerStateBean::bucket_name, bucket.full_name())
						.withAny(AnalyticTriggerStateBean::job_name, jobs.stream().map(j -> j.name()).collect(Collectors.toList()))
						.when(AnalyticTriggerStateBean::trigger_type, TriggerType.none))
						.map(q -> locked_to_host.map(host -> q.when(AnalyticTriggerStateBean::locked_to_host, host)).orElse(q))
						.get();
			
		return trigger_crud.deleteObjectsBySpec(delete_query);
	}	

	/** Deletes old triggers (either that have been removed from the bucket, or that have been replaced by new ones)
	 * @param trigger_crud
	 * @param bucket_name
	 * @param job_name
	 * @param locked_to_host
	 * @param now
	 * @return
	 */
	public static CompletableFuture<?> deleteOldTriggers(final ICrudService<AnalyticTriggerStateBean> trigger_crud, final String bucket_name, final Optional<String> job_name, Optional<String> locked_to_host, final Date now) {
		// These queries want the following optimizations:
		// (bucket_name, job_name, is_job_active, last_checked)
		
		final QueryComponent<AnalyticTriggerStateBean> query = 
				Optional.of(CrudUtils.allOf(AnalyticTriggerStateBean.class)
							.when(AnalyticTriggerStateBean::bucket_name, bucket_name)
							.whenNot(AnalyticTriggerStateBean::is_job_active, true)
							.rangeBelow(AnalyticTriggerStateBean::last_checked, now, true))
				.map(q -> job_name.map(j -> q.when(AnalyticTriggerStateBean::job_name, j)).orElse(q))
				.map(q -> locked_to_host.map(host -> q.when(AnalyticTriggerStateBean::locked_to_host, host)).orElse(q))
				.get();
							
		final CompletableFuture<?> f2 = trigger_crud.deleteObjectsBySpec(query);
		
		return f2;
	}
	
	///////////////////////////////////////////////////////////////////////
	
	// OPTIMIZATION UTILTIES
	
	/** Optimize queries
	 * @param trigger_crud
	 */
	public static void optimizeQueries(final ICrudService<AnalyticTriggerStateBean> trigger_crud) {
		final MethodNamingHelper<AnalyticTriggerStateBean> state_clazz = BeanTemplateUtils.from(AnalyticTriggerStateBean.class);
		
		//1) getTriggersToCheck: (next_check)
		//2) isAnalyticBucketOrJobActive: (bucket_name, job_name, trigger_type, is_job_active)
		//3) updateCompletedJob: (bucket_name, job_name, is_pending)
		//4) updateTriggerStatuses: (_id) [none]
		//5) updateTriggerInputsWhenJobOrBucketCompletes:  (input_resource_combined, input_data_service)
		//6) updateTriggersWithBucketOrJobActivation: (bucket_name, job_name) [ie subset of 2]
		//7) updateActiveJobTriggerStatus: (bucket_name, trigger_type)
		//8) deleteTriggers: (bucket_name) [ie subset of 2]
		//9) deleteActiveJobEntries: (bucket_name, job_name, trigger_type) [ie subset of 2]
		//10) deleteOldTriggers: (bucket_name, job_name, is_job_active, last_checked)			
		
		//1)
		trigger_crud.optimizeQuery(Arrays.asList(state_clazz.field(AnalyticTriggerStateBean::next_check)));
		//2)
		trigger_crud.optimizeQuery(Arrays.asList(
				state_clazz.field(AnalyticTriggerStateBean::bucket_name),
				state_clazz.field(AnalyticTriggerStateBean::job_name),
				state_clazz.field(AnalyticTriggerStateBean::trigger_type),
				state_clazz.field(AnalyticTriggerStateBean::is_job_active)
			));
		//3)
		trigger_crud.optimizeQuery(Arrays.asList(
				state_clazz.field(AnalyticTriggerStateBean::bucket_name),
				state_clazz.field(AnalyticTriggerStateBean::job_name),
				state_clazz.field(AnalyticTriggerStateBean::is_pending)
			));
		//4) (none)
		//5)
		trigger_crud.optimizeQuery(Arrays.asList(
				state_clazz.field(AnalyticTriggerStateBean::input_resource_combined),
				state_clazz.field(AnalyticTriggerStateBean::input_data_service)
			));
		//6) (none)
		//7)
		trigger_crud.optimizeQuery(Arrays.asList(
				state_clazz.field(AnalyticTriggerStateBean::bucket_name),
				state_clazz.field(AnalyticTriggerStateBean::trigger_type)
			));
		//8) (none)
		//9) (none)
		//10) 
		trigger_crud.optimizeQuery(Arrays.asList(
				state_clazz.field(AnalyticTriggerStateBean::bucket_name),
				state_clazz.field(AnalyticTriggerStateBean::job_name),
				state_clazz.field(AnalyticTriggerStateBean::is_job_active),
				state_clazz.field(AnalyticTriggerStateBean::last_checked)
			));
		
		
	}		
}
