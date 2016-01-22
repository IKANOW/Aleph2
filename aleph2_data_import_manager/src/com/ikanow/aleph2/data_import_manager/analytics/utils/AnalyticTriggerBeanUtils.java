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

import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.Tuple2;

import com.google.common.collect.ImmutableSet;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean.AnalyticThreadJobInputBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean.AnalyticThreadComplexTriggerBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean.AnalyticThreadComplexTriggerBean.TriggerOperator;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean.AnalyticThreadComplexTriggerBean.TriggerType;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.BucketUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.TimeUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.management_db.controllers.actors.BucketActionSupervisor;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerStateBean;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage.BucketActionAnalyticJobMessage.JobMessageType;
import com.ikanow.aleph2.management_db.data_model.BucketMgmtMessage.BucketTimeoutMessage;
import com.ikanow.aleph2.management_db.services.ManagementDbActorContext;

/** Utilities for converting analytic jobs into trigger state beans
 * @author Alex
 */
public class AnalyticTriggerBeanUtils {
	
	/** Generates a list of analytic trigger states from a job
	 * @param bucket
	 * @param job
	 * @return
	 */
	public static Stream<AnalyticTriggerStateBean> generateTriggerStateStream(final DataBucketBean bucket, final boolean is_suspended, Optional<String> locked_to_host)
	{				
		final Optional<AnalyticThreadTriggerBean> trigger_info = Optionals.of(() -> bucket.analytic_thread().trigger_config());
				
		// There are 2 types of dependency:
		
		// 1) Trigger dependencies to mark a bucket as active
		
		// 1.1) Get the external dependencies
		
		final Optional<List<AnalyticThreadComplexTriggerBean>> bucket_dependencies = Lambdas.get(() -> {
			
			if (!trigger_info.map(info -> Optional.ofNullable(info.enabled()).orElse(true)).orElse(true)) {
				// Case 1: no trigger specified
				return Optional.<List<AnalyticThreadComplexTriggerBean>>empty();
			}
			//(note beyond here trigger_info must be populated)
			else if (trigger_info.map(info -> Optional.ofNullable(info.auto_calculate()).orElse(null == info.trigger())).orElse(false)) {
				// (auto_calculate defaults to true if no trigger is specified, false otherwise)
				return Optional.of(getFullyAutomaticTriggerList(bucket));
			}
			else if (trigger_info.map(info -> null != info.trigger()).orElse(false)) { // manually specified trigger
				return Optional.of(getSemiAutomaticTriggerStream(trigger_info.get().trigger()).collect(Collectors.toList()));				
			}
			else if (trigger_info.map(info -> null != info.schedule()).orElse(false)) { // just run on a schedule, trivial trigger
				return Optional.of(Arrays.asList(
							BeanTemplateUtils.build(AnalyticThreadComplexTriggerBean.class)
								.with(AnalyticThreadComplexTriggerBean::type, TriggerType.time)
							.done().get()
						));
			}
			else { // manual trigger again
				return Optional.<List<AnalyticThreadComplexTriggerBean>>empty();				
			}			
		});
		
		// 1.2) Convert them to state beans
		
		final Stream<AnalyticTriggerStateBean> external_state_beans =
				bucket_dependencies.map(List::stream).orElseGet(Stream::empty)
					.map(trigger -> {
						final String[] resource_subchannel = Optional.ofNullable(trigger.resource_name_or_id()).filter(s -> !s.isEmpty()).orElse(bucket.full_name()).split(":");						
						
						return BeanTemplateUtils.build(AnalyticTriggerStateBean.class)
									//(add the transient params later)
									.with(AnalyticTriggerStateBean::bucket_id, bucket._id())
									.with(AnalyticTriggerStateBean::bucket_name, bucket.full_name())
									.with(AnalyticTriggerStateBean::is_bucket_active, false)
									.with(AnalyticTriggerStateBean::is_job_active, false)
									.with(AnalyticTriggerStateBean::is_bucket_suspended, is_suspended)
									//(no job for these global params)
									.with(AnalyticTriggerStateBean::trigger_type, trigger.type())
									.with(AnalyticTriggerStateBean::input_data_service, trigger.data_service())
									.with(AnalyticTriggerStateBean::input_resource_name_or_id, resource_subchannel[0])
									.with(AnalyticTriggerStateBean::input_resource_combined, trigger.resource_name_or_id())
									//(more transient counts)
									.with(AnalyticTriggerStateBean::last_resource_size, 0L)
									.with(AnalyticTriggerStateBean::resource_limit, trigger.resource_trigger_limit())
									.with(AnalyticTriggerStateBean::locked_to_host, locked_to_host.orElse(null))
								.done().get();
					})
					;
		
		// 2) Internal dependencies between jobs in active buckets
		
		// (OK I think at some point soon what i'll want to do is enable complex triggers for jobs
		//  but for now all you can do is specify jobs ... each time a job updates it will increment a count)

		final Stream<AnalyticTriggerStateBean> internal_state_beans =
				Optionals.of(() -> bucket.analytic_thread().jobs()).map(List::stream).orElseGet(Stream::empty)
					.filter(job -> Optional.ofNullable(job.enabled()).orElse(true))
					.map(job -> {
						return Optionals.ofNullable(job.dependencies()).stream()
								.<AnalyticTriggerStateBean>map(dep -> {
									return BeanTemplateUtils.build(AnalyticTriggerStateBean.class)
											.with(AnalyticTriggerStateBean::bucket_id, bucket._id())
											.with(AnalyticTriggerStateBean::bucket_name, bucket.full_name())
											.with(AnalyticTriggerStateBean::job_name, job.name())
											.with(AnalyticTriggerStateBean::input_resource_name_or_id, dep)
											//(no combined since it's internal)
											.with(AnalyticTriggerStateBean::is_bucket_active, false)
											.with(AnalyticTriggerStateBean::is_job_active, false)
											.with(AnalyticTriggerStateBean::is_bucket_suspended, is_suspended)
											.with(AnalyticTriggerStateBean::trigger_type, TriggerType.bucket)
											// (no resources)
											.with(AnalyticTriggerStateBean::curr_resource_size, 0L)
											.with(AnalyticTriggerStateBean::last_resource_size, 0L)
											.with(AnalyticTriggerStateBean::locked_to_host, locked_to_host.orElse(null))												
											.done().get();
								})
						;
					})
					.flatMap(s -> s)
					;
		
		return Stream.concat(external_state_beans, internal_state_beans);
	}
	
	///////////////////////////////////////////////////////////////////////////
	
	// UTILS FOR DECIDING WHETHER TO TRIGGER 
	
	/** Builds a set of trigger beans from the inputs
	 * @return
	 */
	public static List<AnalyticThreadComplexTriggerBean> getFullyAutomaticTriggerList(final DataBucketBean bucket) {
		
		return Optionals.of(() -> bucket.analytic_thread().jobs()).orElse(Collections.emptyList())
			.stream()
			.filter(job -> Optional.ofNullable(job.enabled()).orElse(true))
			.flatMap(job -> Optionals.ofNullable(job.inputs()).stream())
			.filter(input -> Optional.ofNullable(input.enabled()).orElse(true))
			.map(input -> convertExternalInputToComplexTrigger(bucket, input))
			.filter(opt_input -> opt_input.isPresent())
			.map(opt_input -> opt_input.get())
			.collect(Collectors.toList())
			;
	}

	
	/** Converts an analytic input to an optional trigger bean
	 * @param input
	 * @return
	 */
	public static Optional<AnalyticThreadComplexTriggerBean> convertExternalInputToComplexTrigger(final DataBucketBean bucket, final AnalyticThreadJobInputBean input) {
		
		// Only care about external sources:		
		final String resource_name_or_id = Optional.ofNullable(input.resource_name_or_id()).orElse("");
		final Optional<String> maybe_ds = Optional.ofNullable(input.data_service());
		if (resource_name_or_id.isEmpty() && maybe_ds.orElse("").equals("batch")) { //special case: my own input
			return Optional.of(
					BeanTemplateUtils.build(AnalyticThreadComplexTriggerBean.class)
						.with(AnalyticThreadComplexTriggerBean::type, TriggerType.file)
						.with(AnalyticThreadComplexTriggerBean::resource_name_or_id, bucket.full_name())
						.with(AnalyticThreadComplexTriggerBean::data_service, maybe_ds.get()) 
					.done().get()
					);			
		}
		else if (!resource_name_or_id.startsWith("/")) { // (ie it's an internal job)
			return Optional.empty();
		}
		else {
			return Optional.of(
					BeanTemplateUtils.build(AnalyticThreadComplexTriggerBean.class)
						.with(AnalyticThreadComplexTriggerBean::type, TriggerType.bucket)
						.with(AnalyticThreadComplexTriggerBean::resource_name_or_id, resource_name_or_id)
						.with(AnalyticThreadComplexTriggerBean::data_service, 
								maybe_ds.filter(ds -> !"streaming".equals(input.data_service()))
										.orElse(null)
								)
					.done().get()
					);
		}
	}
	
	/** Builds a set of trigger means from the recursive manually specified list
	 * @return
	 */
	public static Stream<AnalyticThreadComplexTriggerBean> getSemiAutomaticTriggerStream(final AnalyticThreadComplexTriggerBean trigger) {				
		// Basically we're just flattening the list here:		
		if (!Optional.ofNullable(trigger.enabled()).orElse(true)) {
			return Stream.empty();
		}
		else if (null == trigger.op()) { // end leaf node
			return Stream.of(trigger);
		}
		else { // flatten and combine			
			return trigger.dependency_list().stream().flatMap(nested_trigger -> getSemiAutomaticTriggerStream(nested_trigger));			
		}
	}
	
	///////////////////////////////////////////////////////////////////////////
	
	// UTILS FOR DECIDING WHETHER TO TRIGGER 
	
	/** Checks a set of triggered resources against the complex trigger "equation" in the bucket
	 * @param trigger
	 * @param resources_dataservices
	 * @return
	 */
	public static boolean checkTrigger(final AnalyticThreadComplexTriggerBean trigger, final Set<Tuple2<String, String>> resources_dataservices, boolean pass_if_disabled) {
				
		if (!Optional.ofNullable(trigger.enabled()).orElse(true)) {
			return pass_if_disabled;
		}
		else if (null == trigger.op()) { // end leaf node
			return resources_dataservices.contains(Tuples._2T(trigger.resource_name_or_id(), trigger.data_service()));
		}
		else { // flatten and combine			
			if (TriggerOperator.and == trigger.op()) {
				return Optionals.ofNullable(trigger.dependency_list()).stream()
						.allMatch(sub_trigger -> checkTrigger(sub_trigger, resources_dataservices, true));
			}
			else if (TriggerOperator.or == trigger.op()) {
				return Optionals.ofNullable(trigger.dependency_list()).stream()
						.anyMatch(sub_trigger -> checkTrigger(sub_trigger, resources_dataservices, true));				
			}
			else { // not
				return Optionals.ofNullable(trigger.dependency_list()).stream()
						.noneMatch(sub_trigger -> checkTrigger(sub_trigger, resources_dataservices, false));								
			}
		}
	}
	
	/** Just gets the manual trigger if present else builds an automatic one
	 * @return the trigger to be used (or empty if the bucket doesn't have one)
	 */
	public static Optional<AnalyticThreadComplexTriggerBean> getManualOrAutomatedTrigger(final DataBucketBean bucket) {
		
		final Optional<AnalyticThreadComplexTriggerBean> trigger_info = 
				Optionals.of(() -> bucket.analytic_thread().trigger_config())
				.map(info -> {
					if (Optional.ofNullable(info.auto_calculate()).orElse(null == info.trigger())) {
						return new AnalyticThreadComplexTriggerBean(TriggerOperator.and, true, getFullyAutomaticTriggerList(bucket));
					}
					else {
						return info.trigger();
					}					
				});
				;
				
		return trigger_info;
	}
	
	/** Simple utility to compare a few longs to decide if an updated trigger has activated 
	 * @param trigger
	 * @return
	 */
	public static boolean checkTriggerLimits(final AnalyticTriggerStateBean trigger) {
		if ((null != trigger.curr_resource_size())
				&&
			(null != trigger.last_resource_size()))
		{
			return (trigger.curr_resource_size() - trigger.last_resource_size())
					> Optional.ofNullable(trigger.resource_limit()).orElse(0L);
		}
		else return false;																
	}

	/** Is the trigger external?
	 * @param trigger
	 * @return
	 */
	public static boolean isExternalTrigger(final AnalyticTriggerStateBean trigger) {
		return (TriggerType.none != trigger.trigger_type()) && (null == trigger.job_name());
	}
	/** Is the trigger internal?
	 * @param trigger
	 * @return
	 */
	public static boolean isInternalTrigger(final AnalyticTriggerStateBean trigger) {
		return (TriggerType.none != trigger.trigger_type()) && (null != trigger.job_name());
	}
	/** Is the "trigger" an active job record
	 * @param trigger
	 * @return
	 */
	public static boolean isActiveJobRecord(final AnalyticTriggerStateBean trigger) {
		return (TriggerType.none == trigger.trigger_type()) && (null != trigger.job_name());
	}
	/** Is the "trigger" an active bucket record
	 * @param trigger
	 * @return
	 */
	public static boolean isActiveBucketRecord(final AnalyticTriggerStateBean trigger) {
		return (TriggerType.none == trigger.trigger_type()) && (null == trigger.job_name());
	}
	/** Is the "trigger" an active bucket or job record
	 * @param trigger
	 * @return
	 */
	public static boolean isActiveBucketOrJobRecord(final AnalyticTriggerStateBean trigger) {
		return (TriggerType.none == trigger.trigger_type());
	}
	
	///////////////////////////////////////////////////////////////////////////
	
	// TIMING UTILS
	
	/** Uses the bucket params and the time utils to select a next check time for the bucket triggers
	 * @param from
	 * @param bucket
	 * @return
	 */
	public static Date getNextCheckTime(final Date from, DataBucketBean bucket) {
		final String check_time =
				Optionals.of(() -> bucket.analytic_thread().trigger_config()).map(cfg -> cfg.schedule()).map(Optional::of)
				.orElse(Optional.ofNullable(bucket.poll_frequency()))
				.orElse("")
				;
		
		return TimeUtils.getSchedule(check_time, Optional.of(from))
				.validation(fail -> Date.from(from.toInstant().plus(10L, ChronoUnit.MINUTES)), success -> success);
	}
	
	/** For analytic triggers, need to distinguish on update between the 2 cases:
	 *  - "every 10 minutes" => run now and then in 10 minutes
	 *  - "3am" => run at 3am every day
	 *  This is kinda horrid, but we'll do this by running twice with slightly different times and comparing the results
	 * @param from
	 * @param bucket
	 * @return
	 */
	public static boolean scheduleIsRelative(final Date now, final Date next_trigger, DataBucketBean bucket) {
		
		// Want to check we don't cross eg a day boundary and mess up when "3pm resolves to" - this is a safe way to do it because can't change hour
		// without changing minute...
		final boolean is_on_minute_boundary = 0 == (now.getTime() % 60L);
		final int offset = is_on_minute_boundary ? +1 : -1;
		
		final Date d = getNextCheckTime(Date.from(now.toInstant().plusMillis(offset*500L)), bucket);

		// Basic idea is that eg "every 10 minutes" will be relative to "now" at the second level, but eg "3pm" will be relative to "now" on a much higher level
		// (eg day in that case) so we adjust by 0.5s and see if that causes the generated times to change
		// (NOTE: might get this wrong if next trigger and now are very close, but who cares in that case?!)
		return d.getTime() != next_trigger.getTime();
	}
	
	///////////////////////////////////////////////////////////////////////////
	
	// MESSAGING UTILS
	
	//TODO: handle lock to nodes (!= locked_to_host ugh!)
	
	public static BucketActionMessage buildInternalEventMessage(final DataBucketBean bucket, final List<AnalyticThreadJobBean> jobs, final JobMessageType message_type, final Optional<String> locked_to_host) {
		final BucketActionMessage.BucketActionAnalyticJobMessage new_message =
				new BucketActionMessage.BucketActionAnalyticJobMessage(bucket, jobs, message_type);
		
		final BucketActionMessage.BucketActionAnalyticJobMessage message_with_node_affinity =
				Lambdas.get(() -> {
					if (!locked_to_host.isPresent()) return new_message;
					else return BeanTemplateUtils.clone(new_message)
									.with(BucketActionMessage::handling_clients, ImmutableSet.builder().add(locked_to_host.get()).build())
								.done();
				});
		
		return message_with_node_affinity;
	}
	
	/** Sends a message (eg one build from buildInternalEventMessage)
	 * @param new_message
	 * @param status_crud - workaround while we handle a restricted set of lock_to_nodes case but not the general locked_to_host case... 
	 * @return the reply future (currently unused)
	 */
	public static CompletableFuture<?> sendInternalEventMessage(final BucketActionMessage new_message, 
			final ICrudService<DataBucketStatusBean> status_crud,
			final ICrudService<BucketTimeoutMessage> test_status_crud)
	{
		return sendInternalEventMessage_internal(new_message, status_crud, test_status_crud).thenCompose(node_affinity -> {
			
			return BucketActionSupervisor.askBucketActionActor(Optional.of(false), // (single node only) 
					ManagementDbActorContext.get().getBucketActionSupervisor(), 
					ManagementDbActorContext.get().getActorSystem(), 
					BeanTemplateUtils.clone(new_message)
						.with(BucketActionMessage::handling_clients, ImmutableSet.builder().addAll(node_affinity).build())
					.done()
					, 
					Optional.empty());
		})
		;
	}

	/** Interim Workaround for locked_to_host not working but needing restricted support for lock_to_node
	 * @param new_message
	 * @param status_crud
	 * @return
	 */
	protected static CompletableFuture<Collection<String>> sendInternalEventMessage_internal(
			final BucketActionMessage new_message, 
			final ICrudService<DataBucketStatusBean> status_crud,
			final ICrudService<BucketTimeoutMessage> test_status_crud)
	{
		
		final boolean lock_to_single_node = Optionals.of(() -> 
			new_message.bucket()
						.analytic_thread()
						.jobs().stream()
						.anyMatch(j -> Optional.ofNullable(j.lock_to_nodes()).orElse(false)))
					.orElse(false)
					;

		return Lambdas.<CompletableFuture<Collection<String>>>get(() -> {
			if (lock_to_single_node) {
				if (BucketUtils.isTestBucket(new_message.bucket())) { // no status crud, but the test contains the information we need					
					return test_status_crud.getObjectById(new_message.bucket().full_name(), 
												Arrays.asList(BeanTemplateUtils.from(BucketTimeoutMessage.class).field(BucketTimeoutMessage::handling_clients)), true
												)
							.thenApply(maybe_status -> maybe_status.map(stat -> stat.handling_clients()).orElse(Collections.emptySet()));
				}
				else {
					return 	status_crud
							.getObjectById(new_message, Arrays.asList(BeanTemplateUtils.from(DataBucketStatusBean.class).field(DataBucketStatusBean::node_affinity)), true)
							.thenApply(maybe_status -> maybe_status.map(stat -> stat.node_affinity()).orElse(Collections.emptyList()));
				}
			}
			else return CompletableFuture.completedFuture(Collections.<String>emptyList());
		});
	}
		
}
