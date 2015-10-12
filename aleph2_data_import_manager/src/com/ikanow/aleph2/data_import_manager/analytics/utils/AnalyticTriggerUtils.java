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

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.Tuple2;

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
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerStateBean;

/** Utilities for converting analytic jobs into trigger state beans
 * @author Alex
 */
public class AnalyticTriggerUtils {
	
	/** Generates a list of analytic trigger states from a job
	 * @param bucket
	 * @param job
	 * @return
	 */
	public static Stream<AnalyticTriggerStateBean> generateTriggerStateStream(final DataBucketBean bucket, Optional<String> locked_to_host) {		
		
		Optional<AnalyticThreadTriggerBean> trigger_info = Optionals.of(() -> bucket.analytic_thread().trigger_config());
				
		// There are 2 types of dependency:
		
		// 1) Trigger dependencies to mark a bucket as active
		
		// 1.1) Get the external dependencies
		
		final Optional<List<AnalyticThreadComplexTriggerBean>> bucket_dependencies = Lambdas.get(() -> {
			
			if (trigger_info.map(info -> Optional.ofNullable(info.enabled()).orElse(true)).orElse(true)) {
				// Case 1: no trigger specified
				return Optional.<List<AnalyticThreadComplexTriggerBean>>empty();
			}
			//(note beyond here trigger_info must be populated)
			else if (trigger_info.map(info -> Optional.ofNullable(info.auto_calculate()).orElse(null != info.trigger())).orElse(false)) {
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
						return BeanTemplateUtils.build(AnalyticTriggerStateBean.class)
									//(add the transient params later)
									.with(AnalyticTriggerStateBean::bucket_id, bucket._id())
									.with(AnalyticTriggerStateBean::bucket_name, bucket.full_name())
									//(no job for these global params)
									.with(AnalyticTriggerStateBean::input_data_service, trigger.data_service())
									.with(AnalyticTriggerStateBean::input_resource_name_or_id, trigger.resource_name_or_id())
									//(more transient counts)									
									.with(AnalyticTriggerStateBean::locked_to_host, locked_to_host.orElse(null))
								.done().get();
					})
					;
		
		// 2) Internal dependencies between jobs in active buckets
		
		final Set<String> deps_filter = 
				Optionals.of(() -> bucket.analytic_thread().jobs()).map(List::stream).orElseGet(Stream::empty)
				.filter(job -> job.enabled())
				.flatMap(job -> Optionals.ofNullable(job.dependencies()).stream())
				.collect(Collectors.toSet())
				;
		
		final Stream<AnalyticTriggerStateBean> internal_state_beans =
			Optionals.of(() -> bucket.analytic_thread().jobs()).map(List::stream).orElseGet(Stream::empty)
				.filter(job -> job.enabled())
				.<Tuple2<AnalyticThreadJobBean, AnalyticThreadJobInputBean>>
					flatMap(job -> Optionals.ofNullable(job.inputs()).stream().filter(i -> i.enabled()).map(i -> Tuples._2T(job, i)))
				.map(job_input -> Tuples._2T(job_input._1(), convertInternalInputToComplexTrigger(bucket, job_input._2())))
				.filter(job_trigger -> job_trigger._2().isPresent())
				.filter(job_trigger -> { // (note that have ensured the "rid" is in form external:internal by this point
					return deps_filter.contains(job_trigger._2().get().resource_name_or_id().split(":")[1]);
				})
				.map(job_trigger -> {
					return BeanTemplateUtils.build(AnalyticTriggerStateBean.class)
							//(add the transient params later)
							.with(AnalyticTriggerStateBean::bucket_id, bucket._id())
							.with(AnalyticTriggerStateBean::bucket_name, bucket.full_name())
							.with(AnalyticTriggerStateBean::job_name, job_trigger._1().name())
							.with(AnalyticTriggerStateBean::input_data_service, job_trigger._2().get().data_service())
							.with(AnalyticTriggerStateBean::input_resource_name_or_id, job_trigger._2().get().resource_name_or_id())
							//(more transient counts)									
							.with(AnalyticTriggerStateBean::locked_to_host, locked_to_host.orElse(null))
						.done().get();				
				})
				;
		
		return Stream.concat(external_state_beans, internal_state_beans);
	}
	
	/** Builds a set of trigger beans from the internal inputs
	 *  TODO (ALEPH-12): not sure if this is still needed?
	 * @return
	 */
	public static List<AnalyticThreadComplexTriggerBean> getInternalTriggerList(final DataBucketBean bucket) {
		
		return Optionals.of(() -> bucket.analytic_thread().jobs()).orElse(Collections.emptyList())
			.stream()
			.flatMap(job -> job.inputs().stream())
			.filter(input -> Optional.ofNullable(input.enabled()).orElse(true))
			.map(input -> convertInternalInputToComplexTrigger(bucket, input))
			.filter(opt_input -> opt_input.isPresent())
			.map(opt_input -> opt_input.get())
			.collect(Collectors.toList())
			;
	}
	
	/** Converts an analytic input to an optional trigger bean
	 * @param input
	 * @return
	 */
	protected static Optional<AnalyticThreadComplexTriggerBean> convertInternalInputToComplexTrigger(final DataBucketBean bucket, final AnalyticThreadJobInputBean input) {
		
		final String resource_name_or_id = Optional.ofNullable(input.resource_name_or_id()).orElse("");
		if (resource_name_or_id.startsWith("/") && !bucket.full_name().equals(resource_name_or_id.split(":")[0])) {
			return Optional.empty();
		}
		else {
			final String full_resource_name =
					Optional.of(resource_name_or_id)
							.filter(rid -> rid.startsWith("/"))
							.map(rid -> rid.startsWith(":") ? rid.substring(1) : rid)
							.map(rid -> bucket.full_name() + ":" + rid)
							.orElse(resource_name_or_id)
							;
			
			return Optional.of(
					BeanTemplateUtils.build(AnalyticThreadComplexTriggerBean.class)
						.with(AnalyticThreadComplexTriggerBean::type, TriggerType.bucket)
						.with(AnalyticThreadComplexTriggerBean::resource_name_or_id, full_resource_name)
						.with(AnalyticThreadComplexTriggerBean::data_service, 
								Optional.ofNullable(input.data_service())
										.filter(ds -> !"streaming".equals(input.data_service()))
										.orElse(null)
								)
					.done().get()
					);			
		}
	}
	
	/** Builds a set of trigger beans from the inputs
	 * @return
	 */
	public static List<AnalyticThreadComplexTriggerBean> getFullyAutomaticTriggerList(final DataBucketBean bucket) {
		
		return Optionals.of(() -> bucket.analytic_thread().jobs()).orElse(Collections.emptyList())
			.stream()
			.flatMap(job -> job.inputs().stream())
			.filter(input -> Optional.ofNullable(input.enabled()).orElse(true))
			.map(input -> convertExternalInputToComplexTrigger(input))
			.filter(opt_input -> opt_input.isPresent())
			.map(opt_input -> opt_input.get())
			.collect(Collectors.toList())
			;
	}

	
	/** Converts an analytic input to an optional trigger bean
	 * @param input
	 * @return
	 */
	public static Optional<AnalyticThreadComplexTriggerBean> convertExternalInputToComplexTrigger(final AnalyticThreadJobInputBean input) {
		
		// Only care about external sources:		
		final String resource_name_or_id = Optional.ofNullable(input.resource_name_or_id()).orElse("");
		if (!resource_name_or_id.startsWith("/")) {
			return Optional.empty();
		}
		else {
			return Optional.of(
					BeanTemplateUtils.build(AnalyticThreadComplexTriggerBean.class)
						.with(AnalyticThreadComplexTriggerBean::type, TriggerType.bucket)
						.with(AnalyticThreadComplexTriggerBean::resource_name_or_id, resource_name_or_id)
						.with(AnalyticThreadComplexTriggerBean::data_service, 
								Optional.ofNullable(input.data_service())
										.filter(ds -> !"streaming".equals(input.data_service()))
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
		if (Optional.ofNullable(trigger.enabled()).orElse(true)) {
			return Stream.empty();
		}
		else if (null == trigger.op()) { // end leaf node
			return Stream.of(trigger);
		}
		else { // flatten and combine			
			return trigger.dependency_list().stream().flatMap(nested_trigger -> getSemiAutomaticTriggerStream(nested_trigger));			
		}
	}
	
	
	/** Checks a set of triggered resources against the complex trigger "equation" in the bucket
	 * @param trigger
	 * @param resources_dataservices
	 * @return
	 */
	public static boolean checkTrigger(final AnalyticThreadComplexTriggerBean trigger, Set<Tuple2<String, String>> resources_dataservices) {
				
		if (Optional.ofNullable(trigger.enabled()).orElse(true)) {
			return true;
		}
		else if (null == trigger.op()) { // end leaf node
			return resources_dataservices.contains(Tuples._2T(trigger.resource_name_or_id(), trigger.data_service()));
		}
		else { // flatten and combine			
			if (TriggerOperator.and == trigger.op()) {
				return Optionals.ofNullable(trigger.dependency_list()).stream()
						.allMatch(sub_trigger -> checkTrigger(sub_trigger, resources_dataservices));
			}
			else if (TriggerOperator.or == trigger.op()) {
				return Optionals.ofNullable(trigger.dependency_list()).stream()
						.anyMatch(sub_trigger -> checkTrigger(sub_trigger, resources_dataservices));				
			}
			else { // not
				return Optionals.ofNullable(trigger.dependency_list()).stream()
						.noneMatch(sub_trigger -> checkTrigger(sub_trigger, resources_dataservices));								
			}
		}
	}
}
