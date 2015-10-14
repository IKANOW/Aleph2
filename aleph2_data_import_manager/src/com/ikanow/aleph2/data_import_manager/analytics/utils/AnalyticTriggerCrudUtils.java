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

import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean.AnalyticThreadComplexTriggerBean.TriggerType;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerStateBean;

//TODO (ALEPH-12) - add these to the optimized list in singleton

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
	public static void createActiveJobRecord(final ICrudService<AnalyticTriggerStateBean> trigger_crud, final DataBucketBean bucket, final AnalyticThreadJobBean job, final Optional<String> locked_to_host) {
		final AnalyticTriggerStateBean new_entry =
				BeanTemplateUtils.build(AnalyticTriggerStateBean.class)
					.with(AnalyticTriggerStateBean::_id,
							AnalyticTriggerStateBean.buildId(bucket.full_name(), job.name(), locked_to_host, Optional.empty())
					)
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
		
		trigger_crud.storeObject(new_entry, true); //(fire and forget - don't use a list because storeObjects with replace can be problematic for some DBs)		
	}
	
	//TODO (ALEPH-12): what happens if an updated bucket has no jobs left? need to make sure the existing jobs get erased?!
	// ... can i do a big delete on all the buckets' jobs? 
	// Ahhhhhhhhhhh complication ... i receive these messages grouped not by bucket(:locked_host) _but_ by bucket(:locked_host)[implicitly:analytic_tech]
	// so i'm going to need to update these groupings....
	// ugh it gets worse ... i don't even know when i've received them all, so if i have a bucket with a job served via analytic tech X
	// and then the bucket gets updated so X doesn't appear any more, then i never see the update message (YIKES which also won't update DIM)
	// OK solution: use diff bean to see that analytic thread has changed, write logic to send an empty list of jobs, make sure DIM treats that as
	// "remove everything"
	// SO IN SUMMARY
	// 1) add code to CMDB to check create "empty" buckets
	// 2) ensure that DIM stops empty buckets (and sends messages to the analytic engine)
	// 3) ensure that we handle empty jobs (ie they get to this point)
	
	/** Stores/updates the set of triggers derived from the bucket (replacing any previous ones ... with the exception that if the job to be replaced
	 *  is active then the new set are stored as active  
	 * @param trigger_crud
	 * @param triggers
	 */
	public static void storeOrUpdateTriggerStage(final ICrudService<AnalyticTriggerStateBean> trigger_crud, final Map<String, List<AnalyticTriggerStateBean>> triggers) {
		
		final Date now = Date.from(Instant.now());
		
		triggers.values().stream()
			.flatMap(l -> l.stream())
			.collect(Collectors.groupingBy(v -> Tuples._3T(v.bucket_name(), v.job_name(), v.locked_to_host())))
			.entrySet()
			.stream()
			.forEach(kv -> {
				final String bucket_name = kv.getKey()._1();
				final String job_name = kv.getKey()._2();
				final Optional<String> locked_to_host = Optional.ofNullable(kv.getKey()._3());
				
				// Step 1: is the bucket/job active?
				final boolean is_active = isAnalyticBucketOrJobActive(trigger_crud, bucket_name, Optional.of(job_name), locked_to_host).join();
				
				// Step 2: write out the transformed job, with instructions to check in <5s and last_checked == now
				trigger_crud.storeObjects(
						kv.getValue().stream()
							.map(trigger ->
								BeanTemplateUtils.clone(trigger)
									.with(AnalyticTriggerStateBean::is_pending, !is_active)
									.with(AnalyticTriggerStateBean::last_checked, now)
									.with(AnalyticTriggerStateBean::next_check, now)
								.done()
							)
							.collect(Collectors.toList())
						, 
						true);
				
				// Step 3: then remove any existing entries with lesser last_checked
				final QueryComponent<AnalyticTriggerStateBean> query = 
						Optional.of(CrudUtils.allOf(AnalyticTriggerStateBean.class)
									.when(AnalyticTriggerStateBean::bucket_name, bucket_name)
									.whenNot(AnalyticTriggerStateBean::is_job_active, true)
									.rangeBelow(AnalyticTriggerStateBean::last_checked, now, true))
						.map(q -> locked_to_host.map(host -> q.when(AnalyticTriggerStateBean::locked_to_host, host)).orElse(q))
						.get();
									
				trigger_crud.deleteObjectsBySpec(query);
			});
			;
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
	public static CompletableFuture<Map<String, List<AnalyticTriggerStateBean>>> getTriggersToCheck(final ICrudService<AnalyticTriggerStateBean> trigger_crud) {		
		
		final Date now = Date.from(Instant.now());
		
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
							.collect(Collectors.<AnalyticTriggerStateBean, String>
								groupingBy(trigger -> trigger.bucket_name() + ":" + trigger.locked_to_host())));		
	}
	
	/** Checks the DB to see whether a bucket job is currently active
	 * @param analytic_bucket
	 * @param job
	 * @return
	 */
	public static CompletableFuture<Boolean> isAnalyticBucketOrJobActive(final ICrudService<AnalyticTriggerStateBean> trigger_crud, final String bucket_name, final Optional<String> job_name, final Optional<String> locked_to_host) {
		final QueryComponent<AnalyticTriggerStateBean> query = 
				Optional.of(CrudUtils.allOf(AnalyticTriggerStateBean.class)
							.when(AnalyticTriggerStateBean::bucket_name, bucket_name)
							.when(AnalyticTriggerStateBean::job_name, job_name)
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
	public static void updateCompletedJob(final ICrudService<AnalyticTriggerStateBean> trigger_crud, final String bucket_name, final String job_name, Optional<String> locked_to_host) {
		
		final QueryComponent<AnalyticTriggerStateBean> is_pending_count_query = 
				Optional.of(CrudUtils.allOf(AnalyticTriggerStateBean.class)
							.when(AnalyticTriggerStateBean::bucket_name, bucket_name)
							.when(AnalyticTriggerStateBean::job_name, job_name)
							.when(AnalyticTriggerStateBean::is_pending, true))							
				.map(q -> locked_to_host.map(host -> q.when(AnalyticTriggerStateBean::locked_to_host, host)).orElse(q))
				.get()
				.limit(1)
				;
		
		trigger_crud.countObjectsBySpec(is_pending_count_query).thenAccept(count -> {
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
		})
		.join(); //(because the inter process mutex forces synchronous operations)
		
	}
	
	/**  Updates the statuses of a bunch of existing triggers (does it by _id on each trigger, because it's updating a set of 
	 *   resources also)
	 * @param trigger_crud
	 * @param trigger_stream
	 * @param next_check
	 * @param change_activation - if present then the activation status of this also changes
	 */
	public static void updateTriggerStatuses(final ICrudService<AnalyticTriggerStateBean> trigger_crud,
			final Stream<AnalyticTriggerStateBean> trigger_stream,
			final Date next_check,
			final Optional<Boolean> change_activation			
			)
	{
		trigger_stream.parallel().forEach(t -> {
			final UpdateComponent<AnalyticTriggerStateBean> trigger_update =
					Optional.of(CrudUtils.update(AnalyticTriggerStateBean.class)
						.set(AnalyticTriggerStateBean::curr_resource_size, t.curr_resource_size())
						.set(AnalyticTriggerStateBean::last_checked, Date.from(Instant.now()))
						.set(AnalyticTriggerStateBean::next_check, next_check))
					.map(q -> change_activation.map(change -> {
						if (change) {
							return q.set(AnalyticTriggerStateBean::is_job_active, true)
									.set(AnalyticTriggerStateBean::last_resource_size, t.curr_resource_size())
									;							
						}
						else {
							return q.set(AnalyticTriggerStateBean::is_job_active, false)
									;							
						}
					}).orElse(q))
					.get()
				;
			trigger_crud.updateObjectById(t._id(), trigger_update);
		});		
	}	
	
	/** When a bucket.job completes, this may trigger inputs that depend on that bucket/job 
	 * @param trigger_crud
	 * @param bucket
	 */
	public static void updateTriggerInputsWhenJobOrBucketCompletes(
			final ICrudService<AnalyticTriggerStateBean> trigger_crud, final DataBucketBean bucket,
			final Optional<AnalyticThreadJobBean> job,
			final Optional<String> locked_to_host
			)
	{
		final QueryComponent<AnalyticTriggerStateBean> update_trigger_query =
				Optional.of(CrudUtils.allOf(AnalyticTriggerStateBean.class)
					.when(AnalyticTriggerStateBean::is_bucket_active, false)
					.when(AnalyticTriggerStateBean::is_job_active, false)
					.when(AnalyticTriggerStateBean::is_pending, false)
					.withNotPresent(AnalyticTriggerStateBean::input_data_service)
					.when(AnalyticTriggerStateBean::input_resource_combined, bucket.full_name() + job.map(j -> ":" + j.name()).orElse("") 
					))
					.map(q -> locked_to_host.map(host -> q.when(AnalyticTriggerStateBean::locked_to_host, host)).orElse(q))
					.get();
				;
				
		final UpdateComponent<AnalyticTriggerStateBean> update =
				CrudUtils.update(AnalyticTriggerStateBean.class)
					.increment(AnalyticTriggerStateBean::curr_resource_size, 1L)
				;
		
		trigger_crud.updateObjectsBySpec(update_trigger_query, Optional.of(false), update);
	}
	
	/** Updates relevant triggers to indicate that their bucket is now active
	 * @param trigger_crud
	 * @param bucket
	 */
	public static void updateTriggersWithBucketActivation(final ICrudService<AnalyticTriggerStateBean> trigger_crud, final DataBucketBean bucket) {
		final QueryComponent<AnalyticTriggerStateBean> update_query = 
				CrudUtils.allOf(AnalyticTriggerStateBean.class)
						.when(AnalyticTriggerStateBean::bucket_name, bucket.full_name());
		
		final UpdateComponent<AnalyticTriggerStateBean> update = 
				CrudUtils.update(AnalyticTriggerStateBean.class)
						.set(AnalyticTriggerStateBean::is_bucket_active, true)
						;
		
		trigger_crud.updateObjectsBySpec(update_query, Optional.of(false), update);
	}
	
	/** Updates active job records' next check times
	 * @param trigger_crud
	 */
	public static void updateActiveJobTriggerStatus(final ICrudService<AnalyticTriggerStateBean> trigger_crud, final DataBucketBean bucket) {
		final QueryComponent<AnalyticTriggerStateBean> update_query = 
				CrudUtils.allOf(AnalyticTriggerStateBean.class)
						.when(AnalyticTriggerStateBean::trigger_type, TriggerType.none)
						.when(AnalyticTriggerStateBean::bucket_name, bucket.full_name())
						;
				
		final Instant now = Instant.now();
		
		final UpdateComponent<AnalyticTriggerStateBean> update = 
				CrudUtils.update(AnalyticTriggerStateBean.class)
						.set(AnalyticTriggerStateBean::last_checked, Date.from(now))
						.set(AnalyticTriggerStateBean::next_check, Date.from(now.plusSeconds(AnalyticTriggerCrudUtils.ACTIVE_CHECK_FREQ_SECS)))
						;												
		
		trigger_crud.updateObjectsBySpec(update_query, Optional.of(false), update);		
	}
	
	///////////////////////////////////////////////////////////////////////
	
	// DELETE UTILTIES
	
	/** Just deletes everything for a deleted bucket
	 *  May need to make this cleverer at some point
	 * @param trigger_crud
	 * @param bucket
	 */
	public static void deleteTriggers(final ICrudService<AnalyticTriggerStateBean> trigger_crud, final DataBucketBean bucket) {
		trigger_crud.deleteObjectsBySpec(CrudUtils.allOf(AnalyticTriggerStateBean.class).when(AnalyticTriggerStateBean::bucket_name, bucket.full_name()));
	}

	/** Removes any special "active job records" associated with a set of jobs that are now known to have completed
	 * @param trigger_crud
	 * @param bucket
	 * @param jobs
	 * @param locked_to_host
	 */
	public static void deleteActiveJobEntries(final ICrudService<AnalyticTriggerStateBean> trigger_crud, final DataBucketBean bucket, List<AnalyticThreadJobBean> jobs, final Optional<String> locked_to_host)
	{
		final QueryComponent<AnalyticTriggerStateBean> delete_query =
				Optional.of(CrudUtils.allOf(AnalyticTriggerStateBean.class)
						.when(AnalyticTriggerStateBean::bucket_name, bucket.full_name())
						.withAny(AnalyticTriggerStateBean::job_name, jobs.stream().map(j -> j.name()).collect(Collectors.toList()))
						.when(AnalyticTriggerStateBean::trigger_type, TriggerType.none))
						.map(q -> locked_to_host.map(host -> q.when(AnalyticTriggerStateBean::locked_to_host, host)).orElse(q))
						.get();
			
			trigger_crud.deleteObjectsBySpec(delete_query);
	}	
}
