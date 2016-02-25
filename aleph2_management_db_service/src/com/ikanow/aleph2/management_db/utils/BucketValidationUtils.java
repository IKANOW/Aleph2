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
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.Tuple2;
import scala.Tuple3;

import com.ikanow.aleph2.data_model.interfaces.data_services.IColumnarService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IDataWarehouseService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IDocumentService;
import com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.data_services.ITemporalService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean.AnalyticThreadComplexTriggerBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean.AnalyticThreadComplexTriggerBean.TriggerType;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.data_import.HarvestControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean.MasterEnrichmentType;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.TimeUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;

import fj.data.Validation;

/** A collection of utilities to validate bucket formats
 * @author Alex
 */
public class BucketValidationUtils {

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	// MISC
	
	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	
	/** Bucket validation rules:
	 *  in the format /path/to/<etc>/here where:
	 *  - leading /, no trailing /
	 *  - no /../, /./ or //s 
	 *  - no " "s or ","s or ":"s or ';'s
	 * @param bucket_path
	 * @return
	 */
	public static boolean bucketPathFormatValidationCheck(final String bucket_path) {
		return 0 != Stream.of(bucket_path)
				.filter(p -> p.startsWith("/")) // not allowed for/example
				.filter(p -> !p.endsWith("/")) // not allowed /for/example/
				.filter(p -> !Pattern.compile("/[.]+(?:/|$)").matcher(p).find()) // not allowed /./ or /../
				.filter(p -> !Pattern.compile("//").matcher(p).find())
				.filter(p -> !Pattern.compile("(?:\\s|[,:;])").matcher(p).find())
				.count();
	}
	
	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	// SCHEMA
	
	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	
	/** Validates that all enabled schema point to existing services and are well formed
	 * @param bucket
	 * @param service_context
	 * @return
	 */
	public static Tuple2<Map<String, String>, List<BasicMessageBean>> validateSchema(final DataBucketBean bucket, final IServiceContext service_context) {
		final List<BasicMessageBean> errors = new LinkedList<>();
		final Map<String, String> data_locations = new LinkedHashMap<>(); // icky MUTABLE code)
		
		// Generic data schema:
		errors.addAll(
				validateBucketTimes(bucket).stream()
					.map(s -> MgmtCrudUtils.createValidationError(s)).collect(Collectors.toList()));
		
		// Specific data schema
		if (null != bucket.data_schema()) {
			// Columnar
			if ((null != bucket.data_schema().columnar_schema()) && Optional.ofNullable(bucket.data_schema().columnar_schema().enabled()).orElse(true))
			{
				errors.addAll(service_context.getService(IColumnarService.class, Optional.ofNullable(bucket.data_schema().columnar_schema().service_name()))
								.map(s -> s.validateSchema(bucket.data_schema().columnar_schema(), bucket))
								.map(s -> { 
									if ((null != s._1()) && !s._1().isEmpty()) data_locations.put("columnar_schema", s._1());
									return s._2(); 
								})
								.orElse(Arrays.asList(MgmtCrudUtils.createValidationError(
										ErrorUtils.get(ManagementDbErrorUtils.SCHEMA_ENABLED_BUT_SERVICE_NOT_PRESENT, bucket.full_name(), "columnar_schema")))));
			}
			// Document
			if ((null != bucket.data_schema().document_schema()) && Optional.ofNullable(bucket.data_schema().document_schema().enabled()).orElse(true))
			{
				errors.addAll(service_context.getService(IDocumentService.class, Optional.ofNullable(bucket.data_schema().document_schema().service_name()))
								.map(s -> s.validateSchema(bucket.data_schema().document_schema(), bucket))
								.map(s -> { 
									if ((null != s._1()) && !s._1().isEmpty()) data_locations.put("document_schema", s._1());
									return s._2(); 
								})
								.orElse(Arrays.asList(MgmtCrudUtils.createValidationError(
										ErrorUtils.get(ManagementDbErrorUtils.SCHEMA_ENABLED_BUT_SERVICE_NOT_PRESENT, bucket.full_name(), "document_schema")))));
			}
			// Search Index
			if ((null != bucket.data_schema().search_index_schema()) && Optional.ofNullable(bucket.data_schema().search_index_schema().enabled()).orElse(true))
			{
				errors.addAll(service_context.getService(ISearchIndexService.class, Optional.ofNullable(bucket.data_schema().search_index_schema().service_name()))
								.map(s -> s.validateSchema(bucket.data_schema().search_index_schema(), bucket))
								.map(s -> { 
									if ((null != s._1()) && !s._1().isEmpty()) data_locations.put("search_index_schema", s._1());
									return s._2(); 
								})
								.orElse(Arrays.asList(MgmtCrudUtils.createValidationError(
										ErrorUtils.get(ManagementDbErrorUtils.SCHEMA_ENABLED_BUT_SERVICE_NOT_PRESENT, bucket.full_name(), "search_index_schema")))));
			}
			// Storage
			if ((null != bucket.data_schema().storage_schema()) && Optional.ofNullable(bucket.data_schema().storage_schema().enabled()).orElse(true))
			{
				errors.addAll(service_context.getService(IStorageService.class, Optional.ofNullable(bucket.data_schema().storage_schema().service_name()))
								.map(s -> s.validateSchema(bucket.data_schema().storage_schema(), bucket))
								.map(s -> { 
									if ((null != s._1()) && !s._1().isEmpty()) data_locations.put("storage_schema", s._1());
									return s._2(); 
								})
								.orElse(Arrays.asList(MgmtCrudUtils.createValidationError(
										ErrorUtils.get(ManagementDbErrorUtils.SCHEMA_ENABLED_BUT_SERVICE_NOT_PRESENT, bucket.full_name(), "storage_schema")))));
			}
			if ((null != bucket.data_schema().temporal_schema()) && Optional.ofNullable(bucket.data_schema().temporal_schema().enabled()).orElse(true))
			{
				errors.addAll(service_context.getService(ITemporalService.class, Optional.ofNullable(bucket.data_schema().temporal_schema().service_name()))
								.map(s -> s.validateSchema(bucket.data_schema().temporal_schema(), bucket))
								.map(s -> { 
									if ((null != s._1()) && !s._1().isEmpty()) data_locations.put("temporal_schema", s._1());
									return s._2(); 
								})
								.orElse(Arrays.asList(MgmtCrudUtils.createValidationError(
										ErrorUtils.get(ManagementDbErrorUtils.SCHEMA_ENABLED_BUT_SERVICE_NOT_PRESENT, bucket.full_name(), "temporal_schema")))));
			}
			if (null != bucket.data_schema().data_warehouse_schema())
			{
				errors.addAll(service_context.getService(IDataWarehouseService.class, Optional.ofNullable(bucket.data_schema().data_warehouse_schema().service_name()))
						.map(s -> s.validateSchema(bucket.data_schema().data_warehouse_schema(), bucket))
						.map(s -> { 
							if ((null != s._1()) && !s._1().isEmpty()) data_locations.put("data_warehouse_schema", s._1());
							return s._2(); 
						})
						.orElse(Arrays.asList(MgmtCrudUtils.createValidationError(
								ErrorUtils.get(ManagementDbErrorUtils.SCHEMA_ENABLED_BUT_SERVICE_NOT_PRESENT, bucket.full_name(), "data_warehouse_schema")))));
			}
			//TODO (ALEPH-19) graph schema, geospatial schema
			if (null != bucket.data_schema().geospatial_schema())
			{
				errors.add(MgmtCrudUtils.createValidationError(
						ErrorUtils.get(ManagementDbErrorUtils.SCHEMA_ENABLED_BUT_SERVICE_NOT_PRESENT, bucket.full_name(), "geospatial_schema")));
			}
			if (null != bucket.data_schema().graph_schema())
			{
				errors.add(MgmtCrudUtils.createValidationError(
						ErrorUtils.get(ManagementDbErrorUtils.SCHEMA_ENABLED_BUT_SERVICE_NOT_PRESENT, bucket.full_name(), "graph_schema")));
			}
		}
		return Tuples._2T(data_locations, errors);
	}
	
	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	// HARVEST
	
	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	
	/** Static / simple bucket validation utility method
	 * @param bucket - the bucket to test
	 * @return - a list of errors
	 */
	public static final LinkedList<BasicMessageBean> staticValidation(final DataBucketBean bucket, final boolean allow_system_names) {
		LinkedList<BasicMessageBean> errors = new LinkedList<>();
		
		// More full_name checks
		
		if (!allow_system_names) {
			if (bucket.full_name().startsWith("/aleph2_")) {
				errors.add(MgmtCrudUtils.createValidationError(
						ErrorUtils.get(ManagementDbErrorUtils.BUCKET_FULL_NAME_RESERVED_ERROR, Optional.ofNullable(bucket.full_name()).orElse("(unknown)"))));				
			}
		}
				
		// More complex missing field checks
		
		// - if has enrichment then must have harvest_technology_name_or_id (1) - REMOVED THIS .. eg can upload data/copy directly into Kafka/file system 
		// - if has harvest_technology_name_or_id then must have harvest_configs (2)
		// - if has enrichment then must have master_enrichment_type (3)
		// - if master_enrichment_type == batch/both then must have either batch_enrichment_configs or batch_enrichment_topology (4)
		// - if master_enrichment_type == streaming/both then must have either streaming_enrichment_configs or streaming_enrichment_topology (5)
		//(- for now ... don't support streaming_and_batch, the current enrichment logic doesn't support it (X1))

		//(1, 3, 4, 5)
		if (null == bucket.master_enrichment_type()) { // (3)
			if (((null != bucket.batch_enrichment_configs()) && !bucket.batch_enrichment_configs().isEmpty())
					|| ((null != bucket.streaming_enrichment_configs()) && !bucket.streaming_enrichment_configs().isEmpty())
					|| (null != bucket.batch_enrichment_topology())
					|| (null != bucket.streaming_enrichment_topology()))
			{
				errors.add(MgmtCrudUtils.createValidationError(ErrorUtils.get(ManagementDbErrorUtils.ENRICHMENT_BUT_NO_MASTER_ENRICHMENT_TYPE, bucket.full_name())));
			}
		}
		else if (DataBucketBean.MasterEnrichmentType.streaming_and_batch == bucket.master_enrichment_type()) { //(X1)
			errors.add(MgmtCrudUtils.createValidationError(ErrorUtils.get(ManagementDbErrorUtils.STREAMING_AND_BATCH_NOT_SUPPORTED, bucket.full_name())));
		}
		else {
			// (4)
			if ((DataBucketBean.MasterEnrichmentType.batch == bucket.master_enrichment_type())
				 || (DataBucketBean.MasterEnrichmentType.streaming_and_batch == bucket.master_enrichment_type()))
			{
				if ((null == bucket.batch_enrichment_topology()) && (null == bucket.batch_enrichment_configs()))
				{
					errors.add(MgmtCrudUtils.createValidationError(ErrorUtils.get(ManagementDbErrorUtils.BATCH_ENRICHMENT_NO_CONFIGS, bucket.full_name())));
				}
			}
			// (5)
			if ((DataBucketBean.MasterEnrichmentType.streaming == bucket.master_enrichment_type())
					 || (DataBucketBean.MasterEnrichmentType.streaming_and_batch == bucket.master_enrichment_type()))
			{
				if ((null == bucket.streaming_enrichment_topology()) && (null == bucket.streaming_enrichment_configs()))
				{
					errors.add(MgmtCrudUtils.createValidationError(ErrorUtils.get(ManagementDbErrorUtils.STREAMING_ENRICHMENT_NO_CONFIGS, bucket.full_name())));
				}
			}
		}
		// (2)
		if ((null != bucket.harvest_technology_name_or_id()) &&
				((null == bucket.harvest_configs()) || bucket.harvest_configs().isEmpty()))
		{			
			errors.add(MgmtCrudUtils.createValidationError(ErrorUtils.get(ManagementDbErrorUtils.HARVEST_BUT_NO_HARVEST_CONFIG, bucket.full_name())));
		}
		
		// Embedded object field rules
		
		final Consumer<Tuple2<String, List<String>>> list_test = list -> {
			if (null != list._2()) for (String s: list._2()) {
				if ((s == null) || s.isEmpty()) {
					errors.add(MgmtCrudUtils.createValidationError(ErrorUtils.get(ManagementDbErrorUtils.FIELD_MUST_NOT_HAVE_ZERO_LENGTH, bucket.full_name(), list._1())));
				}
			}
			
		};
		
		// Lists mustn't have zero-length elements
		
		// (NOTE: these strings are just for error messing so not critical, which is we we're not using MethodNamingHelper)
		
		// (6)
		if ((null != bucket.harvest_technology_name_or_id()) && (null != bucket.harvest_configs())) {
			for (int i = 0; i < bucket.harvest_configs().size(); ++i) {
				final HarvestControlMetadataBean hmeta = bucket.harvest_configs().get(i);
				if ((null == hmeta.entry_point()) && (null == hmeta.module_name_or_id())) { // if either of these are set, don't need library names
					list_test.accept(Tuples._2T("harvest_configs" + Integer.toString(i) + ".library_ids_or_names", hmeta.library_names_or_ids()));
				}
			}
		}

		// (7) 
		BiConsumer<Tuple2<String, EnrichmentControlMetadataBean>, Boolean> enrichment_test = (emeta, allowed_empty_list) -> {
			if (Optional.ofNullable(emeta._2().enabled()).orElse(true)) {
				if (!allowed_empty_list)
					if ((null == emeta._2().entry_point()) && (null == emeta._2().module_name_or_id()) //if either of these are set then dont' need library names
							&& 
						((null == emeta._2().library_names_or_ids()) || emeta._2().library_names_or_ids().isEmpty()))
					{
						errors.add(MgmtCrudUtils.createValidationError(ErrorUtils.get(ManagementDbErrorUtils.INVALID_ENRICHMENT_CONFIG_ELEMENTS_NO_LIBS, bucket.full_name(), emeta._1())));				
					}
			}
			list_test.accept(Tuples._2T(emeta._1() + ".library_ids_or_names", emeta._2().library_names_or_ids()));
			list_test.accept(Tuples._2T(emeta._1() + ".dependencies", emeta._2().dependencies()));
		};		
		if (null != bucket.batch_enrichment_topology()) {
			enrichment_test.accept(Tuples._2T("batch_enrichment_topology", bucket.batch_enrichment_topology()), true);
		}
		if (null != bucket.batch_enrichment_configs()) {
			for (int i = 0; i < bucket.batch_enrichment_configs().size(); ++i) {
				final EnrichmentControlMetadataBean emeta = bucket.batch_enrichment_configs().get(i);
				enrichment_test.accept(Tuples._2T("batch_enrichment_configs." + Integer.toString(i), emeta), false);
			}
		}
		if (null != bucket.streaming_enrichment_topology()) {
			enrichment_test.accept(Tuples._2T("streaming_enrichment_topology", bucket.streaming_enrichment_topology()), true);
		}
		if (null != bucket.streaming_enrichment_configs()) {
			for (int i = 0; i < bucket.streaming_enrichment_configs().size(); ++i) {
				final EnrichmentControlMetadataBean emeta = bucket.streaming_enrichment_configs().get(i);
				enrichment_test.accept(Tuples._2T("streaming_enrichment_configs." + Integer.toString(i), emeta), false);
			}
		}
		
		// Multi-buckets logic 
		
		// - if a multi bucket than cannot have any of: enrichment or harvest (8)
		// - multi-buckets cannot be nested (TODO: ALEPH-19, leave this one for later)		
		
		//(8)
		if ((null != bucket.multi_bucket_children()) && !bucket.multi_bucket_children().isEmpty()) {
			if (null != bucket.harvest_technology_name_or_id()) {
				errors.add(MgmtCrudUtils.createValidationError(ErrorUtils.get(ManagementDbErrorUtils.MULTI_BUCKET_CANNOT_HARVEST, bucket.full_name())));				
			}
		}
				
		/////////////////
		
		// PHASE 1b: ANALYIC VALIDATION
				
		errors.addAll(validateAnalyticBucket(bucket).stream().map(MgmtCrudUtils::createValidationError).collect(Collectors.toList()));
				
		return errors;
	}
	
	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	// ANALYTICS
	
	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	
	private static final Pattern VALID_ANALYTIC_JOB_NAME = Pattern.compile("[a-zA-Z0-9_]+");
	private static final Pattern VALID_DATA_SERVICES = Pattern.compile("(?:batch|stream)|(?:(?:search_index_service|storage_service|document_service)(?:[.][a-zA-Z0-9_-]+)?)");
	private static final Pattern VALID_RESOURCE_ID = Pattern.compile("(:?[a-zA-Z0-9_$]*|/[^:]+|/[^:]+:[a-zA-Z0-9_$]*)");
		//(first is internal job, second is external only, third is both) 
	
	/** Validate buckets for their analytic content (lots depends on the specific technology, so that is delegated to the technology)
	 * @param bean
	 * @return
	 */
	public static List<String> validateAnalyticBucket(final DataBucketBean bean) {
		final LinkedList<String> errs = new LinkedList<String>();
		if ((null != bean.analytic_thread()) && Optional.ofNullable(bean.analytic_thread().enabled()).orElse(true)) {
			
			// Check that no enrichment is specified
			if (MasterEnrichmentType.none != Optional.ofNullable(bean.master_enrichment_type()).orElse(MasterEnrichmentType.none)) {
				errs.add(ErrorUtils.get(ManagementDbErrorUtils.ANALYTIC_JOB_ENRICHMENT_SPECIFIED, bean.full_name()));
			}		
			
			final AnalyticThreadBean analytic_thread = bean.analytic_thread();
			final Collection<AnalyticThreadJobBean> jobs = Optionals.ofNullable(bean.analytic_thread().jobs());			

			// 0) Some current (pragmatic) restrictions on analytic bucket distribution settings:
			
			final boolean multi_node_enabled = Optionals.of(() -> jobs.stream().findFirst().map(j -> j.multi_node_enabled()).get()).orElse(false);
			if (multi_node_enabled) {
				errs.add(ErrorUtils.get(ManagementDbErrorUtils.CURRENT_ANALYTIC_DISTRIBUTION_RESTRICTIONS, bean.full_name()));
			}
			final boolean lock_to_nodes = Optionals.of(() -> jobs.stream().findFirst().map(j -> j.lock_to_nodes()).get()).orElse(false);
			if (lock_to_nodes && (null != bean.harvest_technology_name_or_id())) {
				errs.add(ErrorUtils.get(ManagementDbErrorUtils.CURRENT_ANALYTIC_DISTRIBUTION_RESTRICTIONS, bean.full_name()));
			}
			if (jobs.stream().anyMatch(j -> !Optional.ofNullable(j.lock_to_nodes()).orElse(false).equals(lock_to_nodes))) {
				errs.add(ErrorUtils.get(ManagementDbErrorUtils.CURRENT_ANALYTIC_DISTRIBUTION_RESTRICTIONS, bean.full_name()));				
			}
			
			// 1) Jobs can be empty
			
			jobs.stream().filter(job -> Optional.ofNullable(job.enabled()).orElse(true)).forEach(job -> {

				// 2) Some basic checks
				
				final String job_identifier = Optional.ofNullable(job.name()).orElse(BeanTemplateUtils.toJson(job).toString());
				if ((null == job.name()) || !VALID_ANALYTIC_JOB_NAME.matcher(job.name()).matches()) {
					errs.add(ErrorUtils.get(ManagementDbErrorUtils.ANALYTIC_JOB_MALFORMED_NAME, bean.full_name(), job_identifier));					
				}
				
				if (null == job.analytic_technology_name_or_id()) {
					errs.add(ErrorUtils.get(ManagementDbErrorUtils.ANALYTIC_BUT_NO_ANALYTIC_TECH, bean.full_name(), job_identifier));
				}
				
				if (null == job.analytic_type() || (MasterEnrichmentType.none == job.analytic_type())) {
					errs.add(ErrorUtils.get(ManagementDbErrorUtils.ANALYTIC_JOB_MUST_HAVE_TYPE, bean.full_name(), job_identifier));					
				}
				
				if (null != job.analytic_type() && (MasterEnrichmentType.streaming_and_batch == job.analytic_type())) {
					//(temporary restriction - jobs cannot be both batch and streaming, even though that option exists as a enum)
					errs.add(ErrorUtils.get(ManagementDbErrorUtils.STREAMING_AND_BATCH_NOT_SUPPORTED, bean.full_name()));					
				}				
				
				// 3) Inputs
				
				final List<AnalyticThreadJobBean.AnalyticThreadJobInputBean> inputs = 
						Optionals.ofNullable(job.inputs()).stream().filter(i -> Optional.ofNullable(i.enabled()).orElse(true))
						.collect(Collectors.toList());

				// we'll allow inputs to be empty (or at least leave it up to the technology) - perhaps a job is hardwired to get external inputs?
				
				// enabled inputs must not be non-empty
				
				inputs.forEach(input -> {

					// 3.1) basic checks
					
					if ((null == input.data_service()) || !VALID_DATA_SERVICES.matcher(input.data_service()).matches()) {
						errs.add(ErrorUtils.get(ManagementDbErrorUtils.ANALYTIC_INPUT_MALFORMED_DATA_SERVICE, bean.full_name(), job_identifier, 
								input.data_service(), VALID_DATA_SERVICES.toString()));											
					}
					if ((null == input.resource_name_or_id()) || !VALID_RESOURCE_ID.matcher(input.resource_name_or_id()).matches()) {
						errs.add(ErrorUtils.get(ManagementDbErrorUtils.ANALYTIC_INPUT_MALFORMED_RESOURCE_ID, bean.full_name(), job_identifier, 
								input.resource_name_or_id(), VALID_RESOURCE_ID.toString()));											
					}
					
					// 3.2) input config
					
					final Optional<String> o_tmin = Optionals.of(() -> job.global_input_config().time_min()).map(Optional::of).orElse(Optionals.of(() -> input.config().time_min()));
					final Optional<String> o_tmax = Optionals.of(() -> job.global_input_config().time_max()).map(Optional::of).orElse(Optionals.of(() -> input.config().time_max()));
					o_tmin.ifPresent(tmin -> {
						final Validation<String, Date> test = TimeUtils.getSchedule(tmin, Optional.empty());
						if (test.isFail()) {
							errs.add(ErrorUtils.get(ManagementDbErrorUtils.ANALYTIC_INPUT_MALFORMED_DATE, bean.full_name(), job_identifier, "tmin", tmin, test.fail())); 
						}
					});
					o_tmax.ifPresent(tmax -> {
						final Validation<String, Date> test = TimeUtils.getSchedule(tmax, Optional.empty());
						if (test.isFail()) {
							errs.add(ErrorUtils.get(ManagementDbErrorUtils.ANALYTIC_INPUT_MALFORMED_DATE, bean.full_name(), job_identifier, "tmax", tmax, test.fail())); 
						}						
					});
				});
				
				// 4) Outputs
				
				// (will allow null output here, since the technology might be fine with that, though it will normally result in downstream errors)

				if (null != job.output()) {
					
					final boolean is_transient = Optional.ofNullable(job.output().is_transient()).orElse(false);
					if (is_transient) {
						// If transient have to set a streaming/batch type
						if (MasterEnrichmentType.none == Optional.ofNullable(job.output().transient_type()).orElse(MasterEnrichmentType.none)) {
							errs.add(ErrorUtils.get(ManagementDbErrorUtils.ANALYTIC_OUTPUT_TRANSIENT_MISSING_FIELD, bean.full_name(), job_identifier, "transient_type")); 
						}
						// Must not have a sub-bucket path
						if (null != job.output().sub_bucket_path()) {
							errs.add(ErrorUtils.get(ManagementDbErrorUtils.ANALYTIC_OUTPUT_TRANSIENT_ILLEGAL_FIELD, bean.full_name(), job_identifier, "sub_bucket_path")); 
						}
						// TODO (ALEPH-12): Don't currently support "is_transient" with "preserve_data"
						if (Optional.ofNullable(job.output().preserve_existing_data()).orElse(false)) {
							errs.add(ErrorUtils.get("Due to temporary bug, don't currently support is_transient:true and preserve_existing_data:false - please contact the developers"));
						}
					}
				}				
			});
			
			// 5) Triggers
			
			if (null != analytic_thread.trigger_config()) {
				if (null != analytic_thread.trigger_config().schedule()) {
					final Validation<String, Date> test = TimeUtils.getSchedule(analytic_thread.trigger_config().schedule(), Optional.empty());
					if (test.isFail()) {
						errs.add(ErrorUtils.get(ManagementDbErrorUtils.ANALYTIC_TRIGGER_MALFORMED_DATE, bean.full_name(), analytic_thread.trigger_config().schedule(), test.fail()));
					}					
				}			
				Optional.ofNullable(analytic_thread.trigger_config().trigger()).ifPresent(trigger -> errs.addAll(validateAnalyticTrigger(bean, trigger)));
			}
			
			// 6) Check the slightly messy "lock_to_nodes" case:
			
			if ((null == bean.harvest_technology_name_or_id()) && Optional.ofNullable(bean.lock_to_nodes()).orElse(false)) {
				// pure analytic thread with lock_to_nodes set
				
				Map<Tuple3<String, String, MasterEnrichmentType>, DataBucketBean> splits = AnalyticActorUtils.splitAnalyticBuckets(bean, Optional.empty());
				
				if (splits.size() > 1) {
					errs.add(ErrorUtils.get(ManagementDbErrorUtils.ANALYTIC_INVALID_LOCK_TO_NODES, bean.full_name(), 
							splits.keySet().stream().map(s -> s.toString()).sorted().collect(Collectors.joining(";"))
							));					
				}
			}
		}
		return errs;
	}

	/** Recursive check for triggers in analytic beans
	 * @param trigger
	 * @return
	 */
	public static List<String> validateAnalyticTrigger(final DataBucketBean bucket, final AnalyticThreadComplexTriggerBean trigger) {
		final LinkedList<String> errs = new LinkedList<String>();

		// Either: specify resource id or boolean equation type...
		if ((null != trigger.resource_name_or_id()) && (null != trigger.op())
				||
			(null == trigger.resource_name_or_id()) && (null == trigger.op()))
		{
			errs.add(ErrorUtils.get(ManagementDbErrorUtils.ANALYTIC_TRIGGER_ILLEGAL_COMBO, bucket.full_name()));
		}
		// if resource id then must have corresponding type (unless custom - custom analytic tech will have to do its own error validation)
		if ((null != trigger.resource_name_or_id()) ^ (null != trigger.type())) {
			errs.add(ErrorUtils.get(ManagementDbErrorUtils.ANALYTIC_TRIGGER_ILLEGAL_COMBO, bucket.full_name()));
		}		
		// (If custom then must specifiy a custom technology)
		if ((TriggerType.custom == trigger.type()) && (null == trigger.custom_analytic_technology_name_or_id())) {
			errs.add(ErrorUtils.get(ManagementDbErrorUtils.ANALYTIC_CUSTOM_TRIGGER_NOT_COMPLETE, bucket.full_name()));
		}
		// If generating a boolean equation then must have equation terms
		if ((null != trigger.op()) && Optionals.ofNullable(trigger.dependency_list()).isEmpty()) {
			errs.add(ErrorUtils.get(ManagementDbErrorUtils.ANALYTIC_TRIGGER_ILLEGAL_COMBO, bucket.full_name()));
		}
		
		final Stream<AnalyticThreadComplexTriggerBean> other_trigger_configs = Optionals.ofNullable(trigger.dependency_list()).stream();
		
		// Recursive check
		other_trigger_configs.forEach(other_trigger -> errs.addAll(validateAnalyticTrigger(bucket, other_trigger)));		
		
		return errs;
	}
	
	/** Check times inside the bucket and some of its standard schemas
	 * @param bean
	 * @return
	 */
	public static List<String> validateBucketTimes(final DataBucketBean bean) {
		final LinkedList<String> errs = new LinkedList<String>();
		
		if (null != bean.poll_frequency()) {
			TimeUtils.getDuration(bean.poll_frequency())
				.f().forEach(s -> errs.add(ErrorUtils.get(ManagementDbErrorUtils.BUCKET_INVALID_TIME, bean.full_name(), "poll_frequency", s)));			
		}
		
		if (null != bean.data_schema()) {
			if (null != bean.data_schema().temporal_schema()) {
				if (Optional.ofNullable(bean.data_schema().temporal_schema().enabled()).orElse(true)) {
					// Grouping times
					if (null != bean.data_schema().temporal_schema().grouping_time_period()) {
						TimeUtils.getTimePeriod(bean.data_schema().temporal_schema().grouping_time_period())
							.f().forEach(s -> errs.add(ErrorUtils.get(ManagementDbErrorUtils.BUCKET_INVALID_TIME, bean.full_name(), "data_schema.temporal_schema.grouping_time_period", s)));						
					}
					// Max ages
					if (null != bean.data_schema().temporal_schema().cold_age_max()) {
						TimeUtils.getDuration(bean.data_schema().temporal_schema().cold_age_max())
							.f().forEach(s -> errs.add(ErrorUtils.get(ManagementDbErrorUtils.BUCKET_INVALID_TIME, bean.full_name(), "data_schema.temporal_schema.cold_age_max", s)));
					}
					if (null != bean.data_schema().temporal_schema().hot_age_max()) {
						TimeUtils.getDuration(bean.data_schema().temporal_schema().hot_age_max())
							.f().forEach(s -> errs.add(ErrorUtils.get(ManagementDbErrorUtils.BUCKET_INVALID_TIME, bean.full_name(), "data_schema.temporal_schema.hot_age_max", s)));
					}
					if (null != bean.data_schema().temporal_schema().exist_age_max()) {
						TimeUtils.getDuration(bean.data_schema().temporal_schema().exist_age_max())
							.f().forEach(s -> errs.add(ErrorUtils.get(ManagementDbErrorUtils.BUCKET_INVALID_TIME, bean.full_name(), "data_schema.temporal_schema.exist_age_max", s)));
					}
				}
			}
			if (null != bean.data_schema().storage_schema()) { 
				if (Optional.ofNullable(bean.data_schema().storage_schema().enabled()).orElse(true)) {
					// JSON
					if (null != bean.data_schema().storage_schema().json()) {
						if (null != bean.data_schema().storage_schema().json().grouping_time_period()) {
							TimeUtils.getTimePeriod(bean.data_schema().storage_schema().json().grouping_time_period())
								.f().forEach(s -> errs.add(ErrorUtils.get(ManagementDbErrorUtils.BUCKET_INVALID_TIME, bean.full_name(), "data_schema.storage_schema.json.grouping_time_period", s)));						
						}
						if (null != bean.data_schema().storage_schema().json().exist_age_max()) {
							TimeUtils.getDuration(bean.data_schema().storage_schema().json().exist_age_max())
								.f().forEach(s -> errs.add(ErrorUtils.get(ManagementDbErrorUtils.BUCKET_INVALID_TIME, bean.full_name(), "data_schema.storage_schema.json.exist_age_max", s)));						
						}
					}
					// RAW
					if (null != bean.data_schema().storage_schema().raw()) {
						if (null != bean.data_schema().storage_schema().raw().grouping_time_period()) {
							TimeUtils.getTimePeriod(bean.data_schema().storage_schema().raw().grouping_time_period())
								.f().forEach(s -> errs.add(ErrorUtils.get(ManagementDbErrorUtils.BUCKET_INVALID_TIME, bean.full_name(), "data_schema.storage_schema.raw.grouping_time_period", s)));						
						}
						if (null != bean.data_schema().storage_schema().raw().exist_age_max()) {
							TimeUtils.getDuration(bean.data_schema().storage_schema().raw().exist_age_max())
								.f().forEach(s -> errs.add(ErrorUtils.get(ManagementDbErrorUtils.BUCKET_INVALID_TIME, bean.full_name(), "data_schema.storage_schema.raw.exist_age_max", s)));						
						}
					}
					// PROCESSED
					if (null != bean.data_schema().storage_schema().processed()) {
						if (null != bean.data_schema().storage_schema().processed().grouping_time_period()) {
							TimeUtils.getTimePeriod(bean.data_schema().storage_schema().processed().grouping_time_period())
								.f().forEach(s -> errs.add(ErrorUtils.get(ManagementDbErrorUtils.BUCKET_INVALID_TIME, bean.full_name(), "data_schema.storage_schema.processed.grouping_time_period", s)));						
						}
						if (null != bean.data_schema().storage_schema().processed().exist_age_max()) {
							TimeUtils.getDuration(bean.data_schema().storage_schema().processed().exist_age_max())
								.f().forEach(s -> errs.add(ErrorUtils.get(ManagementDbErrorUtils.BUCKET_INVALID_TIME, bean.full_name(), "data_schema.storage_schema.processed.exist_age_max", s)));						
						}
					}
				}				
			}
		}		
		return errs;
	}
}
