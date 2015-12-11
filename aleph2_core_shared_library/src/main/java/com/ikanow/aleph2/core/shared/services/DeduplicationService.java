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
package com.ikanow.aleph2.core.shared.services;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.Tuple2;
import scala.Tuple3;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.interfaces.data_services.IDocumentService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.objects.data_import.AnnotationBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.DocumentSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.DocumentSchemaBean.DeduplicationPolicy;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils.MethodNamingHelper;
import com.ikanow.aleph2.data_model.utils.BucketUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.Patterns;
import com.ikanow.aleph2.data_model.utils.TimeUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.data_model.utils.JsonUtils;
import com.ikanow.aleph2.data_model.utils.SetOnce;

import fj.data.Either;

//TODO (ALEPH-20): need to ensure that object gets added with overwrite existing: true (in the contexts)

/** An enrichment module that will perform deduplication using the provided document_schema
 * @author Alex
 */
public class DeduplicationService implements IEnrichmentBatchModule {
	protected final static ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());
	protected final static MethodNamingHelper<AnnotationBean> _annot = BeanTemplateUtils.from(AnnotationBean.class);
	
	protected final SetOnce<ICrudService<JsonNode>> _dedup_context = new SetOnce<>();
	protected final SetOnce<IEnrichmentModuleContext> _context = new SetOnce<>();
	protected final SetOnce<DocumentSchemaBean> _doc_schema = new SetOnce<>();
	protected final SetOnce<String> _timestamp_field = new SetOnce<>();
	
	protected final SetOnce<IEnrichmentBatchModule> _custom_handler = new SetOnce<>();
	
	/** Implementation of IBatchRecord
	 * @author Alex
	 */
	public static class MyBatchRecord implements IBatchRecord {
		protected final JsonNode _json;
		public MyBatchRecord(JsonNode json) {
			_json = json;
		}		
		@Override
		public JsonNode getJson() {
			return _json;
		}

		@Override
		public Optional<ByteArrayOutputStream> getContent() {
			return Optional.empty();
		}		
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule#onStageInitialize(com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext, com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean, scala.Tuple2, java.util.Optional)
	 */
	@Override
	public void onStageInitialize(final IEnrichmentModuleContext context,
			final DataBucketBean bucket, 
			final EnrichmentControlMetadataBean dedup_control,
			final Tuple2<ProcessingStage, ProcessingStage> previous_next,
			final Optional<List<String>> next_grouping_fields)
	{
		_context.set(context);

		final DocumentSchemaBean doc_schema = bucket.data_schema().document_schema(); //(exists by construction)
		_doc_schema.set(doc_schema);
		
		final String timestamp_field = Optionals.of(() -> bucket.data_schema().temporal_schema().time_field())
											.orElseGet(() -> AnnotationBean.ROOT_PATH + ".tp");
		_timestamp_field.set(timestamp_field);

		final DataBucketBean context_holder =
				Optional.ofNullable(doc_schema.deduplication_contexts())
					.filter(l -> !l.isEmpty()) // (if empty or null then fall back to...)				
					.map(contexts -> 
						BeanTemplateUtils.build(DataBucketBean.class)
								.with(DataBucketBean::multi_bucket_children, ImmutableSet.<String>builder().addAll(contexts).build())
						.done().get()
					)
					.orElse(bucket);
		
		final Optional<ICrudService<JsonNode>> maybe_read_crud = 
			context.getServiceContext().getService(IDocumentService.class, Optional.ofNullable(doc_schema.service_name()))
					.flatMap(ds -> ds.getDataService())
					.flatMap(ds -> ds.getReadableCrudService(JsonNode.class, Arrays.asList(context_holder), Optional.empty()))
			;
		
		maybe_read_crud.ifPresent(read_crud -> _dedup_context.set(read_crud));
		
		final DeduplicationPolicy policy = Optional.ofNullable( _doc_schema.get().deduplication_policy()).orElse(DeduplicationPolicy.leave);
		if ((DeduplicationPolicy.custom == policy) || (DeduplicationPolicy.custom_update == policy)) {
			//TODO (ALEPH-20): for now only support one dedup enrichment config (really the "multi-enrichment" pipeline code should be in core shared vs the technology I think?)
			
			Optional<EnrichmentControlMetadataBean> custom_config =  doc_schema.custom_deduplication_configs().stream().findFirst();
			custom_config.ifPresent(cfg -> {
				final Optional<String> entry_point = Optional.ofNullable(cfg.entry_point())
						.map(Optional::of)
						.orElseGet(() -> {
							// Get the shared library bean:
							
							return BucketUtils.getBatchEntryPoint(							
								context.getServiceContext().getCoreManagementDbService().readOnlyVersion().getSharedLibraryStore()
									.getObjectBySpec(CrudUtils.anyOf(SharedLibraryBean.class)
												.when(SharedLibraryBean::_id, cfg.module_name_or_id())
												.when(SharedLibraryBean::path_name, cfg.module_name_or_id())
											)
									.join()
									.map(bean -> (Map<String, SharedLibraryBean>)ImmutableMap.of(cfg.module_name_or_id(), bean))
									.orElse(Collections.<String, SharedLibraryBean>emptyMap())
									,
									cfg);
						});
				
				entry_point.ifPresent(Lambdas.wrap_consumer_u(ep -> 
					_custom_handler.set((IEnrichmentBatchModule) Class.forName(ep, true, Thread.currentThread().getContextClassLoader()).newInstance())));

				_custom_handler.optional().ifPresent(base_module -> base_module.onStageInitialize(context, bucket, cfg, previous_next, next_grouping_fields));
			});
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule#onObjectBatch(java.util.stream.Stream, java.util.Optional, java.util.Optional)
	 */
	@Override
	public void onObjectBatch(final Stream<Tuple2<Long, IBatchRecord>> batch,
			final Optional<Integer> batch_size, 
			final Optional<JsonNode> grouping_key)
	{
		if ((null == _doc_schema.get().deduplication_policy())
				&& Optionals.ofNullable(_doc_schema.get().deduplication_fields()).isEmpty()
				&& Optionals.ofNullable(_doc_schema.get().deduplication_contexts()).isEmpty()
				)
		{ 
			// no deduplication, generally shouldn't be here...
			//.. but if we are, make do the best we can
			batch.forEach(t2 -> _context.get().emitImmutableObject(t2._1(), t2._2().getJson(), Optional.empty(), Optional.empty(), Optional.empty()));
			return;
		}		
		final List<String> dedup_fields = Optional.ofNullable(_doc_schema.get().deduplication_fields()).orElse(Arrays.asList(JsonUtils._ID));
		final DeduplicationPolicy policy = Optional.ofNullable( _doc_schema.get().deduplication_policy()).orElse(DeduplicationPolicy.leave);
		
		// Create big query
		
		final Tuple3<QueryComponent<JsonNode>, List<Tuple2<JsonNode, Tuple2<Long, IBatchRecord>>>, Either<String, List<String>>> fieldinfo_dedupquery_keyfields = 
				getDedupQuery(batch, dedup_fields);
		
		// Get duplicate results
		
		final Tuple2<List<String>, Boolean> fields_include = getIncludeFields(policy, dedup_fields, _timestamp_field.get());

		//TODO: go back to removing fields
		final CompletableFuture<Iterator<JsonNode>> dedup_res =
				fieldinfo_dedupquery_keyfields._2().isEmpty()
				? 
				CompletableFuture.completedFuture(Collections.<JsonNode>emptyList().iterator())
				: 
				_dedup_context.get().getObjectsBySpec(fieldinfo_dedupquery_keyfields._1(), fields_include._1(), fields_include._2()).thenApply(cursor -> cursor.iterator());

		// Wait for it to finsh
		
			//(create handy results structure if so)
			final LinkedHashMap<JsonNode, LinkedList<Tuple3<Long, IBatchRecord, ObjectNode>>> mutable_obj_map =				
					fieldinfo_dedupquery_keyfields._2().stream().collect(						
							Collector.of(
								() -> new LinkedHashMap<JsonNode, LinkedList<Tuple3<Long, IBatchRecord, ObjectNode>>>(), 
								(acc, t2) -> {
									// (ie only the first element is added, duplicate elements are removed)
									final Tuple3<Long, IBatchRecord, ObjectNode> t3 = Tuples._3T(t2._2()._1(), t2._2()._2(), _mapper.createObjectNode());
									acc.compute(t2._1(), (k, v) -> {
										final LinkedList<Tuple3<Long, IBatchRecord, ObjectNode>> new_list = 
												(null == v)
												? new LinkedList<>()
												: v;
										new_list.add(t3);
										return new_list;
									});
								},
								(map1, map2) -> {
									map1.putAll(map2);
									return map1;
								}));						
				
		//TODO (ALEPH-20): various options for inserting _ids
		//TODO (ALEPH-20): add timestamps to annotation
		//TODO (ALEPH-20): support different timestamp fields for the different buckets
		//TODO (ALEPH-20): really need to support >1 current job (Really really longer term you should be able to decide what objects you want and what you don't) <- don't remember what i meant here
				
		final Iterator<JsonNode> cursor = dedup_res.join();
		
		// Handle the results
		
		Optionals.streamOf(cursor, true)
					.forEach(ret_obj -> {
						final Optional<JsonNode> maybe_key = getKeyFieldsAgain(ret_obj, fieldinfo_dedupquery_keyfields._3());
						final Optional<LinkedList<Tuple3<Long, IBatchRecord, ObjectNode>>> matching_records = maybe_key.map(key -> mutable_obj_map.get(key)); 

						//DEBUG
						//System.out.println("?? " + ret_obj + " vs " + maybe_key + " vs " + matching_record.map(x -> x._2().getJson().toString()).orElse("(no match)"));
						
						matching_records.ifPresent(records -> 
							handleDuplicateRecord(policy, _context.get(), _custom_handler.optional(), _timestamp_field.get(), records, ret_obj, maybe_key.get(), mutable_obj_map));
					});
		
		mutable_obj_map.values().stream()
			.map(t -> t.peekLast())
			.forEach(t -> _context.get().emitImmutableObject(t._1(), t._2().getJson(), Optional.of(t._3()), Optional.empty(), Optional.empty()))
			;
	}

	/** The heart of the dedup logic
	 *  (everything gets ordered in the correct order except if policy is custom, in which case the ordering is a
	 *   bit arbitrary anyway)
	 * @param policy
	 * @param context
	 * @param timestamp_field
	 * @param new_record
	 * @param old_record
	 * @param key
	 * @param mutable_obj_map
	 */
	protected static void handleDuplicateRecord(final DeduplicationPolicy policy,
			final IEnrichmentModuleContext context, Optional<IEnrichmentBatchModule> custom_handler,
			final String timestamp_field,
			final LinkedList<Tuple3<Long, IBatchRecord, ObjectNode>> new_records, final JsonNode old_record,
			final JsonNode key,
			final Map<JsonNode, LinkedList<Tuple3<Long, IBatchRecord, ObjectNode>>> mutable_obj_map
			)
	{
		Patterns.match(policy).andAct()
				.when(p -> p == DeduplicationPolicy.leave, __ -> {
					mutable_obj_map.remove(key); //(drop new record)
				})
				.when(p -> p == DeduplicationPolicy.update, __ -> {
					final Tuple3<Long, IBatchRecord, ObjectNode> last_record = new_records.peekLast();
					if (newRecordUpdatesOld(timestamp_field, last_record._2().getJson(), old_record)) {
						last_record._3().put(JsonUtils._ID, old_record.get(JsonUtils._ID));
					}
					else {
						mutable_obj_map.remove(key); //(drop new record)				
					}
				})
				.when(p -> p == DeduplicationPolicy.overwrite, __ -> {
					final Tuple3<Long, IBatchRecord, ObjectNode> last_record = new_records.peekLast();
					// Just update the new record's "_id" field
					last_record._3().put(JsonUtils._ID, old_record.get(JsonUtils._ID));
				})
				.when(p -> p == DeduplicationPolicy.custom_update, __ -> {
					final Tuple3<Long, IBatchRecord, ObjectNode> last_record = new_records.peekLast();
					if (newRecordUpdatesOld(timestamp_field, last_record._2().getJson(), old_record)) {
						mutable_obj_map.remove(key); // (since the "final step" logic is responsible for calling the update code)
						handleCustomDeduplication(custom_handler, new_records, old_record, key);
					}
					else {
						mutable_obj_map.remove(key); //(drop new record)
					}
				})
				.otherwise(__ -> {
					mutable_obj_map.remove(key); // (since the "final step" logic is responsible for calling the update code)		
					handleCustomDeduplication(custom_handler, new_records, old_record, key);
				});
	}
	
	/** Returns the minimal set of includes to return from the dedup query
	 * @param policy
	 * @param dedup_fields
	 * @param timestamp_field
	 * @return
	 */
	protected static Tuple2<List<String>, Boolean> getIncludeFields(final DeduplicationPolicy policy, final List<String> dedup_fields, String timestamp_field) {
		final Tuple2<List<String>, Boolean> fields_include = Optional.of(Patterns.match(policy).<Tuple2<List<String>, Boolean>>andReturn()
				.when(p -> p == DeduplicationPolicy.leave, __ -> Tuples._2T(Arrays.asList(JsonUtils._ID), true))
				.when(p -> p == DeduplicationPolicy.update, __ -> Tuples._2T(Arrays.asList(JsonUtils._ID, timestamp_field), true))
				.when(p -> p == DeduplicationPolicy.overwrite, __ -> Tuples._2T(Arrays.asList(JsonUtils._ID), true))
				.otherwise(__ -> Tuples._2T(Arrays.asList(), false))
			)
			.map(t2 -> t2._2() 
					? Tuples._2T(Stream.concat(t2._1().stream(), dedup_fields.stream()).collect(Collectors.toList()), t2._2())
					: t2
				)
			.get()
			;

		return fields_include;
	}
	
	/** Logic to perform the custom deduplication with the current and new versions
	 * @param custom_handler
	 * @param new_record
	 * @param old_record
	 */
	protected static void handleCustomDeduplication(
			final Optional<IEnrichmentBatchModule> custom_handler,
			final List<Tuple3<Long, IBatchRecord, ObjectNode>> new_records, final JsonNode old_record, final JsonNode key)
	{
		custom_handler
			.map(base_module -> base_module.cloneForNewGrouping())
			.ifPresent(new_module -> {
				final Stream<Tuple2<Long, IBatchRecord>> dedup_stream =
						Stream.concat(
								new_records.stream().map(t3 -> Tuples._2T(t3._1(), t3._2())),
								Stream.of(Tuples._2T(-1L, new MyBatchRecord(old_record)))
								);
				
				new_module.onObjectBatch(dedup_stream, Optional.of(1 + new_records.size()), Optional.of(key));
				new_module.onStageComplete(false);
			});		
	}
	
	/** Compares the old and new records' timestamps (if either doesn't exist then assume we're leaving)
	 *  (so that if the time isn't present then doesn't hammer the DB)
	 * @param timestamp_field
	 * @param new_record
	 * @param old_record
	 * @return
	 */
	protected static boolean newRecordUpdatesOld(String timestamp_field, final JsonNode new_record, final JsonNode old_record) {
		final Optional<JsonNode> old_timestamp = JsonUtils.getProperty(timestamp_field, old_record);
		final Optional<JsonNode> new_timestamp = JsonUtils.getProperty(timestamp_field, new_record);
		final Optional<Tuple2<Long, Long>> maybe_old_new =
			old_timestamp.flatMap(old_ts -> getTimestampFromJsonNode(old_ts))
							.flatMap(old_ts -> 
										new_timestamp
											.flatMap(new_ts -> getTimestampFromJsonNode(new_ts))
											.map(new_ts -> Tuples._2T(old_ts, new_ts)));
		
		return maybe_old_new.filter(old_new -> old_new._2() > old_new._1()).isPresent();
	}
	
	/** Converts a JsonNode to a timestamp if possible
	 * @param in
	 * @return
	 */
	public static Optional<Long> getTimestampFromJsonNode(final JsonNode in) {
		if (null == in) {
			return Optional.empty();
		}
		else if (in.isNumber()) {
			return Optional.of(in.asLong());
		}
		else if (in.isTextual()) {
			return Optional.ofNullable(TimeUtils.parseIsoString(in.asText()).validation(fail -> null, success -> success.getTime()));
		}
		else {
			return Optional.empty();
		}
	}
	
	/** Creates the query and some associated metadata
	 * @param batch
	 * @param dedup_fields
	 * @return a 3-tuple containing: the query to apply, the list of records indexed by the key, the field-or-fields that form the key
	 */
	protected static Tuple3<QueryComponent<JsonNode>, List<Tuple2<JsonNode, Tuple2<Long, IBatchRecord>>>, Either<String, List<String>>> getDedupQuery(final Stream<Tuple2<Long, IBatchRecord>> batch, final List<String> dedup_fields) {
		
		if (1 == dedup_fields.size()) { // this is a simpler case
			final String key_field = dedup_fields.stream().findFirst().get();
			final List<Tuple2<JsonNode, Tuple2<Long, IBatchRecord>>> field_info = extractKeyField(batch, key_field);
			return Tuples._3T(
					CrudUtils.allOf().withAny(key_field, field_info.stream().map(t2 -> t2._1().toString()).collect(Collectors.toList())).limit(Integer.MAX_VALUE)
					,
					field_info
					,
					Either.left(key_field)
					);
		}
		else {
			final List<Tuple2<JsonNode, Tuple2<Long, IBatchRecord>>> field_info = extractKeyFields(batch, dedup_fields);
			
			final Stream<QueryComponent<JsonNode>> elements =
				field_info.stream()
					.map(t2 -> {
						return Optionals.streamOf(t2._1().fields(), false)
						 	.reduce(CrudUtils.allOf(),
						 			(acc, kv) -> acc.when(kv.getKey(), JsonUtils.jacksonToJava(kv.getValue())),
						 			(acc1, acc2) -> acc1 // (not possible because not parallel()
						 			)
						 	;
					})
					;
			
			final QueryComponent<JsonNode> query_dedup = CrudUtils.anyOf(elements).limit(Integer.MAX_VALUE);
			
			return Tuples._3T(query_dedup, field_info, Either.right(dedup_fields));
		}		
	}
	
	/** Utility to find fragments of a json object from single/multiple fields
	 * @param in
	 * @param key_fields
	 * @return
	 */
	protected static Optional<JsonNode> getKeyFieldsAgain(final JsonNode in, Either<String, List<String>> key_field_or_fields) {
		return  key_field_or_fields.either(
				key_field -> extractKeyField(in, key_field)
				,
				key_fields -> extractKeyFields(in, key_fields)
				)
				;
	}
	
	/** Utility to find a single field for dedup purposes 
	 * @param in - stream of JSON objects
	 * @param key_field
	 * @return
	 */
	protected static List<Tuple2<JsonNode, Tuple2<Long, IBatchRecord>>> extractKeyField(final Stream<Tuple2<Long, IBatchRecord>> in, final String key_field) {
		return in
				.map(x -> extractKeyField(x._2().getJson(), key_field).map(y -> Tuples._2T(y, x)).orElse(null))
				.filter(x -> null != x)
				.collect(Collectors.toList())
				;
	}

	/** Utility to find a single field for dedup purposes 
	 * @param in - single JSON object
	 * @param key_field
	 * @return
	 */
	protected static Optional<JsonNode> extractKeyField(final JsonNode in, final String key_field) {
		return JsonUtils.getProperty(key_field, in).filter(j -> j.isValueNode());
	}

	
	/** Utility to find a multiple-field set of values for dedup purposes 
	 * @param in - stream of JSON objects
	 * @param key_fields
	 * @return
	 */
	protected static List<Tuple2<JsonNode, Tuple2<Long, IBatchRecord>>> extractKeyFields(final Stream<Tuple2<Long, IBatchRecord>> in, final List<String> key_fields) {
		return in
				.map(x -> extractKeyFields(x._2().getJson(), key_fields).map(y -> Tuples._2T(y, x)).orElse(null))
				.filter(x -> null != x)
				.collect(Collectors.toList())
				;
	}
	
	/** Utility to find a multiple-field set of values for dedup purposes 
	 * @param in - single JSON object
	 * @param key_fields
	 * @return
	 */
	protected static Optional<JsonNode> extractKeyFields(final JsonNode in, final List<String> key_fields) {
		final ObjectNode on = key_fields.stream()
				.reduce(_mapper.createObjectNode(),
						(acc, v) -> JsonUtils.getProperty(v, in).<ObjectNode>map(val -> (ObjectNode) acc.put(v, val)).orElse(acc),
						(acc1, acc2) -> acc1 //(not possible because not parallel())
						);

		return Optional.of((JsonNode) on).filter(o -> 0 != o.size());
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule#onStageComplete(boolean)
	 */
	@Override
	public void onStageComplete(final boolean is_original) {
		_custom_handler.optional().ifPresent(handler -> handler.onStageComplete(true));
	}

}
