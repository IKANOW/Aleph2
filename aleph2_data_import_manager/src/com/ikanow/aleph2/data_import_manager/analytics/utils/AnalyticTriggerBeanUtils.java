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
package com.ikanow.aleph2.data_import_manager.analytics.utils;

import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.Tuple2;

import com.google.common.collect.ImmutableSet;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean.AnalyticThreadJobInputBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean.AnalyticThreadComplexTriggerBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean.AnalyticThreadComplexTriggerBean.TriggerOperator;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean.AnalyticThreadComplexTriggerBean.TriggerType;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.TimeUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerStateBean;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage.BucketActionAnalyticJobMessage.JobMessageType;

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
			else if (trigger_info.map(info -> null != info.trigger()).orElse(false)) {
				return Optional.of(getSemiAutomaticTriggerStream(trigger_info.get().trigger()).collect(Collectors.toList()));				
			}
			else { // manual trigger again
				return Optional.<List<AnalyticThreadComplexTriggerBean>>empty();				
			}			
		});
		
		// 1.2) Convert them to state beans
		
		final Stream<AnalyticTriggerStateBean> external_state_beans =
				bucket_dependencies.map(List::stream).orElseGet(Stream::empty)
					.map(trigger -> {
						final String[] resource_subchannel = Optional.ofNullable(trigger.resource_name_or_id()).orElse("").split(":");						
						
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
	
	///////////////////////////////////////////////////////////////////////////
	
	// MESSAGING UTILS
	
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
	

}
