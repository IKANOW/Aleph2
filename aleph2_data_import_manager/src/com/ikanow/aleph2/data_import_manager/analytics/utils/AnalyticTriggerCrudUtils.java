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
package com.ikanow.aleph2.data_import_manager.analytics.utils;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
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
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerStateBean;

/** A set of utilities for retrieving and updating the trigger state
 * @author Alex
 */
public class AnalyticTriggerCrudUtils {

	///////////////////////////////////////////////////////////////////////
	
	// CREATE UTILTIES
	
	/** Creates a special record that just exists to indicate that a job is active
	 * @param trigger_crud
	 * @param bucket
	 * @param job
	 * @param locked_to_host
	 */
	public static CompletableFuture<?> createActiveBucketOrJobRecord(final ICrudService<AnalyticTriggerStateBean> trigger_crud, final DataBucketBean bucket, final Optional<AnalyticThreadJobBean> job, final Optional<String> locked_to_host) {
		final AnalyticTriggerStateBean new_entry_pre =
				BeanTemplateUtils.build(AnalyticTriggerStateBean.class)
					.with(AnalyticTriggerStateBean::is_job_active, true)
					.with(AnalyticTriggerStateBean::bucket_id, bucket._id())
					.with(AnalyticTriggerStateBean::bucket_name, bucket.full_name())
					.with(AnalyticTriggerStateBean::job_name, job.map(j -> j.name()).orElse(null))
					.with(AnalyticTriggerStateBean::trigger_type, TriggerType.none)
					.with(AnalyticTriggerStateBean::last_checked, Date.from(Instant.now()))
					.with(AnalyticTriggerStateBean::next_check, Date.from(Instant.now())) // (currently check active buckets every 10s)
					.with(AnalyticTriggerStateBean::locked_to_host, locked_to_host.orElse(null))
				.done().get();
		
		final CompletableFuture<?> f1 = job.map(j -> {
			final AnalyticTriggerStateBean new_entry = BeanTemplateUtils.clone(new_entry_pre)
														.with(AnalyticTriggerStateBean::_id, AnalyticTriggerStateBean.buildId(new_entry_pre, false))
														.done();
			
			final CompletableFuture<?> f1_int = trigger_crud.storeObject(new_entry, true); //(fire and forget)
			
			final QueryComponent<AnalyticTriggerStateBean> active_job_query =
					Optional.of(CrudUtils.allOf(AnalyticTriggerStateBean.class)
						.when(AnalyticTriggerStateBean::bucket_name, bucket.full_name())
						.when(AnalyticTriggerStateBean::job_name, j.name())
						.whenNot(AnalyticTriggerStateBean::is_job_active, true)
					)
					.map(q -> locked_to_host.map(host -> q.when(AnalyticTriggerStateBean::locked_to_host, host)).orElse(q))
					.get();
			
			final UpdateComponent<AnalyticTriggerStateBean> active_job_update =
					CrudUtils.update(AnalyticTriggerStateBean.class)
						.set(AnalyticTriggerStateBean::is_bucket_active, true)
						.set(AnalyticTriggerStateBean::is_job_active, true)
						;
			
			final CompletableFuture<?> f2_int = trigger_crud.updateObjectsBySpec(active_job_query, Optional.empty(), active_job_update);
			
			return CompletableFuture.allOf(f1_int, f2_int);
		})
		.orElseGet(() -> CompletableFuture.completedFuture(null))
		;	
		// Finally also create a bucket active notification record:
		
		final AnalyticTriggerStateBean new_bucket_entry_pre = 
				BeanTemplateUtils.clone(new_entry_pre)
					.with(AnalyticTriggerStateBean::job_name, null)
					.with(AnalyticTriggerStateBean::is_job_active, null)
					.with(AnalyticTriggerStateBean::last_resource_size, job.map(__ -> 1L).orElse(null))
					.done();

		final AnalyticTriggerStateBean new_bucket_entry = BeanTemplateUtils.clone(new_bucket_entry_pre)
				.with(AnalyticTriggerStateBean::_id, AnalyticTriggerStateBean.buildId(new_bucket_entry_pre, false))
				.done();

		final CompletableFuture<?> f3 = trigger_crud.storeObject(new_bucket_entry, true); //(fire and forget)		
		
		//(note any notification of active jobs also updates the bucket active status, so don't need to worry about that here)
		
		return CompletableFuture.allOf(f1, f3);
	}
	
	/** Stores/updates the set of triggers derived from the bucket (replacing any previous ones ... with the exception that if the job to be replaced
	 *  is active then the new set are stored as active  
	 * @param trigger_crud
	 * @param triggers
	 */
	public static CompletableFuture<?> storeOrUpdateTriggerStage(final DataBucketBean bucket, final ICrudService<AnalyticTriggerStateBean> trigger_crud, final Map<Tuple2<String, String>, List<AnalyticTriggerStateBean>> triggers) {
		
		final Date now = Date.from(Instant.now());
		final Date next_check_for_external_triggers_tmp = AnalyticTriggerBeanUtils.getNextCheckTime(now, bucket);
		final Date next_check_for_external_triggers = AnalyticTriggerBeanUtils.scheduleIsRelative(now, next_check_for_external_triggers_tmp, bucket)
														? now // eg for "hourly" check immediately
														: next_check_for_external_triggers_tmp; // eg for "3pm" don't check until then
							
		final Map<Tuple2<String, Optional<String>>, Boolean> mutable_bucket_names = new HashMap<>();
		
		final Stream<CompletableFuture<?>> ret = triggers.entrySet().stream()
			.flatMap(kv -> {
				final Tuple2<String, Optional<String>> bucket_name = Tuples._2T(kv.getKey()._1(), Optional.ofNullable(kv.getKey()._2()));
				mutable_bucket_names.put(bucket_name,
						isAnalyticBucketActive(trigger_crud, bucket_name._1(), bucket_name._2()).join()
						);
				return kv.getValue().stream();
			})
			.collect(Collectors.groupingBy(v -> Tuples._3T(v.bucket_name(), v.job_name(), v.locked_to_host())))
			.entrySet()
			.stream()
			.parallel() // (note no mutable state written to from here)
			.flatMap(kv -> {
				final String bucket_name = kv.getKey()._1();
				final Optional<String> job_name = Optional.ofNullable(kv.getKey()._2());
				final Optional<String> locked_to_host = Optional.ofNullable(kv.getKey()._3());
								
				// Step 1: is the bucket/job active?
				final boolean is_job_active = job_name.map(j -> areAnalyticJobsActive(trigger_crud, bucket_name, job_name, locked_to_host).join())
											.orElse(false); // (don't care if the bucket is active, only the designated job)

				// Step 1.5: is the bucket active?
				final boolean is_bucket_active = mutable_bucket_names.getOrDefault(Tuples._2T(bucket_name, locked_to_host), false);	//(must exist by construction?)	
				
				// Step 2: write out the transformed job, with instructions to check in <5s and last_checked == now
				final CompletableFuture<?> f1 = trigger_crud.storeObjects(
						kv.getValue().stream()
							.map(trigger ->
								BeanTemplateUtils.clone(trigger)
									.with(AnalyticTriggerStateBean::_id, AnalyticTriggerStateBean.buildId(trigger, is_job_active))
									.with(AnalyticTriggerStateBean::is_bucket_active, is_bucket_active)
									.with(AnalyticTriggerStateBean::is_pending, is_job_active)
									.with(AnalyticTriggerStateBean::last_checked, now)
									.with(AnalyticTriggerStateBean::next_check,
											AnalyticTriggerBeanUtils.isExternalTrigger(trigger) ? next_check_for_external_triggers : now)
								.done()
							)
							.collect(Collectors.toList())
						, 
						true);
				
				return Stream.of(f1);
			})
			;		
		
		// Step 3: then remove any existing entries with lesser last_checked (for all external depdendencies and non-active jobs)
		// (note that external depdendencies are typically updated hence won't be affected by this)
			
		final Stream<CompletableFuture<?>> f2 =
				mutable_bucket_names.keySet().stream().map(bucket_name -> {
					return deleteOldTriggers(trigger_crud, bucket_name._1(), Optional.empty(), bucket_name._2(), now);
				});
			
		final CompletableFuture<?> combined[] = Stream.concat(ret, f2).toArray(size -> new CompletableFuture[size]);
		
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
		
		final QueryComponent<AnalyticTriggerStateBean> active_bucket_job_query =
				CrudUtils.allOf(AnalyticTriggerStateBean.class)
					.rangeBelow(AnalyticTriggerStateBean::next_check, now, true)
					.when(AnalyticTriggerStateBean::trigger_type, TriggerType.none); 
			//(ie only returns special "is job active" and "is bucket active" records, not the input triggers - these are the records we use to check completion)
		
		final QueryComponent<AnalyticTriggerStateBean> external_query =
				CrudUtils.allOf(AnalyticTriggerStateBean.class)
					.rangeBelow(AnalyticTriggerStateBean::next_check, now, true)
					.when(AnalyticTriggerStateBean::is_bucket_active, false) // (only care about triggers for inactive jobs)
					.when(AnalyticTriggerStateBean::is_bucket_suspended, false)
					.when(AnalyticTriggerStateBean::is_pending, false)
					.withNotPresent(AnalyticTriggerStateBean::job_name);
				
		final QueryComponent<AnalyticTriggerStateBean> internal_query =
				CrudUtils.allOf(AnalyticTriggerStateBean.class)
					.rangeBelow(AnalyticTriggerStateBean::next_check, now, true)
					.when(AnalyticTriggerStateBean::is_bucket_active, true) // ie only check job dependencies for active buckets
					.when(AnalyticTriggerStateBean::is_pending, false)
					.withPresent(AnalyticTriggerStateBean::job_name);
						
		final QueryComponent<AnalyticTriggerStateBean> query = CrudUtils.anyOf(Arrays.asList(active_bucket_job_query, external_query, internal_query));
		
		return trigger_crud.getObjectsBySpec(query).thenApply(cursor -> 
				StreamSupport.stream(cursor.spliterator(), false)
							.collect(Collectors.<AnalyticTriggerStateBean, Tuple2<String, String>>
								groupingBy(trigger -> Tuples._2T(trigger.bucket_name(), trigger.locked_to_host()))));		
	}
	
	/** Checks the DB to see whether a bucket job (or any if job is empty) is currently active
	 * @param analytic_bucket
	 * @param job
	 * @return
	 */
	public static CompletableFuture<Boolean> areAnalyticJobsActive(final ICrudService<AnalyticTriggerStateBean> trigger_crud, final String bucket_name, final Optional<String> job_name, final Optional<String> locked_to_host) {
		
		// These queries want the following optimizations:
		// (bucket_name, job_name, trigger_type, is_job_active)
		
		final QueryComponent<AnalyticTriggerStateBean> query = 
				Optional.of(CrudUtils.allOf(AnalyticTriggerStateBean.class)
							.when(AnalyticTriggerStateBean::bucket_name, bucket_name)
							.when(AnalyticTriggerStateBean::trigger_type, TriggerType.none)
							.when(AnalyticTriggerStateBean::is_job_active, true)) //(ie won't match on the active bucket record because of this term)	
				.map(q -> locked_to_host.map(host -> q.when(AnalyticTriggerStateBean::locked_to_host, host)).orElse(q))
				.map(q -> job_name.map(j -> q.when(AnalyticTriggerStateBean::job_name, j)).orElse(q))
				.get()
				.limit(1)
				;
					
		return trigger_crud.countObjectsBySpec(query).thenApply(c -> c.intValue() > 0);
	}
	
	/** Checks the DB to see whether a bucket is currently active
	 * @param analytic_bucket
	 * @param job
	 * @return
	 */
	public static CompletableFuture<Boolean> isAnalyticBucketActive(final ICrudService<AnalyticTriggerStateBean> trigger_crud, final String bucket_name, final Optional<String> locked_to_host) {
		
		// These queries want the following optimizations:
		// (bucket_name, job_name, trigger_type, is_bucket_active)
		
		final QueryComponent<AnalyticTriggerStateBean> query = 
				Optional.of(CrudUtils.allOf(AnalyticTriggerStateBean.class)
							.when(AnalyticTriggerStateBean::bucket_name, bucket_name)
							.withNotPresent(AnalyticTriggerStateBean::job_name) // (ie will only match on the bucket active record)
							.when(AnalyticTriggerStateBean::trigger_type, TriggerType.none)
							.when(AnalyticTriggerStateBean::is_bucket_active, true)) 	
				.map(q -> locked_to_host.map(host -> q.when(AnalyticTriggerStateBean::locked_to_host, host)).orElse(q))
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
		
		final QueryComponent<AnalyticTriggerStateBean> is_pending_query = 
				Optional.of(CrudUtils.allOf(AnalyticTriggerStateBean.class)
							.when(AnalyticTriggerStateBean::bucket_name, bucket_name)
							.when(AnalyticTriggerStateBean::job_name, job_name)
							.when(AnalyticTriggerStateBean::is_pending, true))							
				.map(q -> locked_to_host.map(host -> q.when(AnalyticTriggerStateBean::locked_to_host, host)).orElse(q))
				.get()
				;
		
		return trigger_crud.getObjectsBySpec(is_pending_query).thenCompose(cursor -> {
			final Stream<CompletableFuture<?>> cfs = 
					StreamSupport.stream(cursor.spliterator(), true)
						.map(trigger -> {
							return trigger_crud.storeObject( // overwrite the old object with the new one (except with pending set)
									BeanTemplateUtils.clone(trigger)
										.with(AnalyticTriggerStateBean::is_pending, false)
										.with(AnalyticTriggerStateBean::_id, AnalyticTriggerStateBean.buildId(trigger, false))
										.done()
										,
										true
									)
									// then delete the "pending" new object
									.thenCompose(__ -> trigger_crud.deleteObjectById(trigger._id()))
									.exceptionally(__ -> trigger_crud.deleteObjectById(trigger._id()).join())
									;
						})
						;
			
			return CompletableFuture.allOf(cfs.toArray(CompletableFuture[]::new));
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
						.set(AnalyticTriggerStateBean::last_checked, Date.from(Instant.now()))
						.set(AnalyticTriggerStateBean::next_check, next_check))
					.map(q -> Optional.ofNullable(t.curr_resource_size())
								.map(size -> q.set(AnalyticTriggerStateBean::curr_resource_size, t.curr_resource_size()))
								.orElse(q))
					.map(q -> change_activation.map(change -> {
						if (change) {
							// (note: don't set the status to active until we get back a message from the technology)
							return Optional.ofNullable(t.curr_resource_size())
											.map(size -> q.set(AnalyticTriggerStateBean::last_resource_size, size))
											.orElse(q)
									.set(AnalyticTriggerStateBean::is_job_active, true) // (this hasn't been confirmed by the tech yet but if it fails we'll find out in 10s time when we poll it)
									.set(AnalyticTriggerStateBean::is_bucket_active, true)
									;							
						}
						else {
							return q.set(AnalyticTriggerStateBean::is_job_active, false)
									//(don't unset the bucket status, leave that alone)
									;							
						}
					}).orElse(q))
					.get()
				;
				final CompletableFuture<Boolean> cf_b = trigger_crud.updateObjectById(t._id(), trigger_update);
				
				return cf_b;
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
		// (input_resource_name_or_id)
		
		
		// 1) external inputs, ie from other buckets
		// since these are external inputs, only care if the bucket is _not_ active
		final QueryComponent<AnalyticTriggerStateBean> update_trigger_query_external =
				Optional.of(CrudUtils.allOf(AnalyticTriggerStateBean.class)
					.when(AnalyticTriggerStateBean::input_resource_combined, bucket.full_name() + job.map(j -> ":" + j.name()).orElse("")) 
					.withNotPresent(AnalyticTriggerStateBean::input_data_service)
					.when(AnalyticTriggerStateBean::is_bucket_active, false)
					.when(AnalyticTriggerStateBean::is_bucket_suspended, false)
					.when(AnalyticTriggerStateBean::is_pending, false)
					)
					.map(q -> locked_to_host.map(host -> q.when(AnalyticTriggerStateBean::locked_to_host, host)).orElse(q))
					.get();
				;
				
		// 2) internal inputs, ie within this bucket
		// since these are internal ... here only care if the bucket is _active_ (and if it's not already running)
		final QueryComponent<AnalyticTriggerStateBean> update_trigger_query =
				job.map(j -> {
					final QueryComponent<AnalyticTriggerStateBean> update_trigger_query_2 =
							Optional.of(CrudUtils.allOf(AnalyticTriggerStateBean.class)
								.when(AnalyticTriggerStateBean::input_resource_name_or_id, j.name()) 
								.withNotPresent(AnalyticTriggerStateBean::input_data_service)
								.when(AnalyticTriggerStateBean::is_bucket_active, true)
								.when(AnalyticTriggerStateBean::is_job_active, false)
								.when(AnalyticTriggerStateBean::is_pending, false)
								)
								.map(q -> locked_to_host.map(host -> q.when(AnalyticTriggerStateBean::locked_to_host, host)).orElse(q))
								.get();
					return (QueryComponent<AnalyticTriggerStateBean>)CrudUtils.anyOf(update_trigger_query_external, update_trigger_query_2);
				})
				.orElse(update_trigger_query_external);
				
		final Date next_trigger_check = Date.from(Instant.now().minusSeconds(1L));
		
		final UpdateComponent<AnalyticTriggerStateBean> update =
				CrudUtils.update(AnalyticTriggerStateBean.class)
					.increment(AnalyticTriggerStateBean::curr_resource_size, 1L)
					.set(AnalyticTriggerStateBean::next_check, next_trigger_check) // (Ensure it gets checked immediately)
				;
		
		// Update the values
		
		final CompletableFuture<?> cf1 = trigger_crud.updateObjectsBySpec(update_trigger_query, Optional.of(false), update);
		
		// Also update the "next_check" time for all buckets with an affected _external trigger_
		// (Note can fire and forget here since just updating the times)
		
		trigger_crud.getObjectsBySpec(update_trigger_query_external, //(only care about the bucket_name)
							Arrays.asList(BeanTemplateUtils.from(AnalyticTriggerStateBean.class).field(AnalyticTriggerStateBean::bucket_name)), true)
						.thenAccept(cursor -> {
							
							final List<String> update_names =
									Optionals.streamOf(cursor.iterator(), false)
										.map(t -> t.bucket_name())
										.collect(Collectors.toList());								

							trigger_crud.updateObjectsBySpec(
									CrudUtils.allOf(AnalyticTriggerStateBean.class)
										.withAny(AnalyticTriggerStateBean::bucket_name, update_names)
									, 
									Optional.of(false), 
									CrudUtils.update(AnalyticTriggerStateBean.class)
										.set(AnalyticTriggerStateBean::next_check, next_trigger_check)
									)
									;
						});
				;
		
		return cf1;
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
						.map(u -> jobs.map(__ -> u.set(AnalyticTriggerStateBean::is_job_active, true)).orElse(u))
						.get()
						;
		
		return trigger_crud.updateObjectsBySpec(update_query, Optional.of(false), update);
	}
	
	/** Updates active job records' next check times
	 *  CURRENTLY - JUST SETS TO "NOW" AGAIN, IE ACTIVE JOB STATUSES GET CHECKED EVERY TRIGGER (LIKE ACTIVE JOB DEPENDENCIES)
	 * @param trigger_crud
	 */
	public static CompletableFuture<?> updateActiveJobTriggerStatus(final ICrudService<AnalyticTriggerStateBean> trigger_crud, final DataBucketBean bucket, final Date next_check) {
		// These queries want the following optimizations:
		// (bucket_name, trigger_type)
		
		final QueryComponent<AnalyticTriggerStateBean> update_query = 
				CrudUtils.allOf(AnalyticTriggerStateBean.class)
						.when(AnalyticTriggerStateBean::bucket_name, bucket.full_name())
						.when(AnalyticTriggerStateBean::trigger_type, TriggerType.none)
						;
				
		final UpdateComponent<AnalyticTriggerStateBean> update = 
				CrudUtils.update(AnalyticTriggerStateBean.class)
						// (leave the "now" check on active records so that i can do timeouts)
						.set(AnalyticTriggerStateBean::next_check, next_check)
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
			
		final CompletableFuture<?> cf1 = trigger_crud.deleteObjectsBySpec(delete_query);
		
		// Also remove the is_job_active from eg external triggers
		
		final CompletableFuture<?> cf2 = trigger_crud.updateObjectsBySpec(
				Optional.of(CrudUtils.allOf(AnalyticTriggerStateBean.class)
						.withAny(AnalyticTriggerStateBean::job_name, jobs.stream().map(j -> j.name()).collect(Collectors.toList()))
						.when(AnalyticTriggerStateBean::bucket_name, bucket.full_name()))
							.map(q -> locked_to_host.map(host -> q.when(AnalyticTriggerStateBean::locked_to_host, host)).orElse(q))
							.get()
						,
						Optional.of(false)
						,
						CrudUtils.update(AnalyticTriggerStateBean.class)
							.set(AnalyticTriggerStateBean::is_job_active, false));
				
		return CompletableFuture.allOf(cf1, cf2);		
	}	

	/** Deletes old triggers (either that have been removed from the bucket, or that have been replaced by new ones)
	 * @param trigger_crud
	 * @param bucket_name
	 * @param job_name - 3 cases: 1) _only_ delete the specified triggers, 2) delete no triggers, delete only external deps
	 * @param locked_to_host
	 * @param now
	 * @return
	 */
	public static CompletableFuture<?> deleteOldTriggers(final ICrudService<AnalyticTriggerStateBean> trigger_crud, final String bucket_name, final Optional<Collection<String>> job_names, Optional<String> locked_to_host, final Date now) {
		// These queries want the following optimizations:
		// (bucket_name, job_name, is_job_active, last_checked)
		
		final QueryComponent<AnalyticTriggerStateBean> query = 
				Optional.of(CrudUtils.allOf(AnalyticTriggerStateBean.class)
							.when(AnalyticTriggerStateBean::bucket_name, bucket_name)
							.whenNot(AnalyticTriggerStateBean::is_job_active, true)
							.whenNot(AnalyticTriggerStateBean::trigger_type, TriggerType.none) //(important because active records don't get their last_checked updated)
							.rangeBelow(AnalyticTriggerStateBean::last_checked, now, true)
							)
				.map(q -> job_names.map(j -> q.withAny(AnalyticTriggerStateBean::job_name, j)).orElse(q))
				.map(q -> locked_to_host.map(host -> q.when(AnalyticTriggerStateBean::locked_to_host, host)).orElse(q))
				.get();
							
		final CompletableFuture<?> f2 = trigger_crud.deleteObjectsBySpec(query);
		
		return f2;
	}
	
	/** Deletes the active bucket record
	 * @param trigger_crud
	 * @param bucket_name
	 * @param locked_to_host
	 * @return
	 */
	public static CompletableFuture<?> deleteActiveBucketRecord(final ICrudService<AnalyticTriggerStateBean> trigger_crud, final String bucket_name, Optional<String> locked_to_host) {
		final CompletableFuture<?> cf1 = trigger_crud.deleteObjectsBySpec(
				Optional.of(CrudUtils.allOf(AnalyticTriggerStateBean.class)
						.when(AnalyticTriggerStateBean::bucket_name, bucket_name)
						.when(AnalyticTriggerStateBean::trigger_type, TriggerType.none))
						.map(q -> locked_to_host.map(host -> q.when(AnalyticTriggerStateBean::locked_to_host, host)).orElse(q))
						.get()
				);
		
		// Also remove the is_bucket_active from eg external triggers
		
		final CompletableFuture<?> cf2 = trigger_crud.updateObjectsBySpec(
				Optional.of(CrudUtils.allOf(AnalyticTriggerStateBean.class)
						.when(AnalyticTriggerStateBean::bucket_name, bucket_name))
							.map(q -> locked_to_host.map(host -> q.when(AnalyticTriggerStateBean::locked_to_host, host)).orElse(q))
							.get()
						,
						Optional.of(false)
						,
						CrudUtils.update(AnalyticTriggerStateBean.class)
							.set(AnalyticTriggerStateBean::is_bucket_active, false));
				
		return CompletableFuture.allOf(cf1, cf2);
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
		//5) updateTriggerInputsWhenJobOrBucketCompletes:  (input_resource_combined, input_data_service), (input_resource_name_or_id)
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
		//5a)
		trigger_crud.optimizeQuery(Arrays.asList(
				state_clazz.field(AnalyticTriggerStateBean::input_resource_combined),
				state_clazz.field(AnalyticTriggerStateBean::input_data_service)
			));
		//5b)
		trigger_crud.optimizeQuery(Arrays.asList(
				state_clazz.field(AnalyticTriggerStateBean::input_resource_name_or_id)
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
