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
package com.ikanow.aleph2.analytics.services;

import static org.junit.Assert.*;

import java.io.File;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import scala.Tuple2;
import scala.Tuple3;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.analytics.services.DeduplicationService;
import com.ikanow.aleph2.core.shared.utils.BatchRecordUtils;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule.ProcessingStage;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.interfaces.data_services.IDocumentService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IBucketLogger;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.DocumentSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.DocumentSchemaBean.DeduplicationPolicy;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import fj.data.Either;
import fj.data.Validation;

public class TestDeduplicationService {
	protected static ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());
	
	@Inject 
	IServiceContext _service_context;
	
	@Before
	public void setup() throws Exception {
		
		final String temp_dir = System.getProperty("java.io.tmpdir") + File.separator;		

		Config config = ConfigFactory.parseReader(new InputStreamReader(this.getClass().getResourceAsStream("/context_local_test.properties")))
				.withValue("globals.local_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
				.withValue("globals.local_cached_jar_dir", ConfigValueFactory.fromAnyRef(temp_dir))
				.withValue("globals.distributed_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
				.withValue("globals.local_yarn_config_dir", ConfigValueFactory.fromAnyRef(temp_dir));

		Injector app_injector = ModuleUtils.createTestInjector(Arrays.asList(), Optional.of(config));	
		app_injector.injectMembers(this);
	}
	
	@Test
	public void test_onStageInitialize() {

		try {
			// Some globals
			final EnrichmentControlMetadataBean control = BeanTemplateUtils.build(EnrichmentControlMetadataBean.class)
						.with(EnrichmentControlMetadataBean::name, "test")
					.done().get();
			final IEnrichmentModuleContext enrich_context = Mockito.mock(IEnrichmentModuleContext.class);
			Mockito.when(enrich_context.getServiceContext()).thenReturn(_service_context);
			final IBucketLogger mock_logger = Mockito.mock(IBucketLogger.class);
			Mockito.when(enrich_context.getLogger(Mockito.any())).thenReturn(mock_logger);			
			
			// simple version (default temporal field)
			{
				final DataBucketBean test_bucket = getDocBucket("/test/simple",
						BeanTemplateUtils.build(DocumentSchemaBean.class)
							.with(DocumentSchemaBean::deduplication_policy, DeduplicationPolicy.custom_update)
						.done().get()
						);
				
				final DeduplicationService test_module = new DeduplicationService();
				
				test_module.onStageInitialize(enrich_context, test_bucket, control, Tuples._2T(ProcessingStage.input, ProcessingStage.output), Optional.empty());
				
				assertEquals(test_module._doc_schema.get().deduplication_policy(), test_bucket.data_schema().document_schema().deduplication_policy());
				assertEquals(test_module._doc_schema.get().custom_finalize_all_objects(), false);
				assertEquals(test_module._timestamp_field.get(), "__a.tp");
				assertTrue("Should have built dedup context", test_module._dedup_context.optional().isPresent());
				assertFalse("There shouldn't be a custom handler", test_module._custom_handler.optional().isPresent());
				
				//(test coverage!)
				test_module.onStageComplete(true);
			}
			// custom version - gets custom handler - entry point specified, specify time field
			{
				final DataBucketBean test_bucket = addTimestampField("@timestamp", getDocBucket("/test/custom/1",
						BeanTemplateUtils.build(DocumentSchemaBean.class)
							.with(DocumentSchemaBean::deduplication_policy, DeduplicationPolicy.custom)
							.with(DocumentSchemaBean::custom_deduplication_configs,
									Arrays.asList(
											BeanTemplateUtils.build(EnrichmentControlMetadataBean.class)
												.with(EnrichmentControlMetadataBean::name, "custom_test")
												.with(EnrichmentControlMetadataBean::entry_point, TestDedupEnrichmentModule.class.getName())
											.done().get()										
											)
									)
						.done().get()
						));
				
				final DeduplicationService test_module = new DeduplicationService();
				
				test_module.onStageInitialize(enrich_context, test_bucket, control, Tuples._2T(ProcessingStage.batch, ProcessingStage.output), Optional.empty());
				
				assertEquals(test_module._timestamp_field.get(), "@timestamp");
				assertEquals(TestDedupEnrichmentModule.class, test_module._custom_handler.get().getClass());
				
				//(test coverage!)
				test_module.onStageComplete(true);
			}
			// custom+update - insert shared library bean
			{
				final DataBucketBean test_bucket = getDocBucket("/test/custom/1",
						BeanTemplateUtils.build(DocumentSchemaBean.class)
							.with(DocumentSchemaBean::deduplication_policy, DeduplicationPolicy.custom_update)
							.with(DocumentSchemaBean::deduplication_contexts, Arrays.asList("/**"))
							.with(DocumentSchemaBean::custom_deduplication_configs,
									Arrays.asList(
											BeanTemplateUtils.build(EnrichmentControlMetadataBean.class)
												.with(EnrichmentControlMetadataBean::name, "custom_test")
												.with(EnrichmentControlMetadataBean::module_name_or_id, "/app/aleph2/library/test.jar")
											.done().get()										
											)
									)
						.done().get()
						);
				
				// OK now need to create a shared library bean and insert it
				final SharedLibraryBean bean = 
						BeanTemplateUtils.build(SharedLibraryBean.class)
							.with(SharedLibraryBean::path_name, "/app/aleph2/library/test.jar")
							.with(SharedLibraryBean::batch_enrichment_entry_point, TestDedupEnrichmentModule.class.getName())
						.done().get();
				
				_service_context.getService(IManagementDbService.class, Optional.empty()).get()
					.getSharedLibraryStore().storeObject(bean, true)
					.join()
					;			
				
				final DeduplicationService test_module = new DeduplicationService();
				
				test_module.onStageInitialize(enrich_context, test_bucket, control, Tuples._2T(ProcessingStage.input, ProcessingStage.batch), Optional.empty());
	
				assertEquals(TestDedupEnrichmentModule.class, test_module._custom_handler.get().getClass());
				
				//(test coverage!)
				test_module.onStageComplete(true);
			}
		}
		catch (Throwable t) {
			System.out.println(ErrorUtils.getLongForm("{0}", t));
			throw t;
		}		
	}

	@Test
	public void test_extractKeyOrKeys() {

		// extractKeyFields / extractKeyField, single element:
		{
			final ObjectNode test = _mapper.createObjectNode();
			final ObjectNode test_nest = _mapper.createObjectNode();
			test.put("field1", "test1");
			test.put("field2", "test2");
			test.put("nested1", test_nest);
			test_nest.put("nested_field", "nested1");
			
			// Empty things:
			
			assertEquals(Optional.empty(), DeduplicationService.extractKeyField(test, "test_notpresent"));
			assertEquals(Optional.empty(), DeduplicationService.extractKeyField(test, "nested1.test_notpresent"));
			assertEquals(Optional.empty(), DeduplicationService.extractKeyField(test, "nested2.nested1"));
			
			assertEquals(Optional.empty(), DeduplicationService.extractKeyFields(test, Arrays.asList("test_notpresent", "test_notpresent2")));
			assertEquals(Optional.empty(), DeduplicationService.extractKeyFields(test, Arrays.asList("nested1.test_notpresent")));
			assertEquals(Optional.empty(), DeduplicationService.extractKeyFields(test, Arrays.asList("nested2.nested1")));
			
			// values:
			
			// single field
			assertEquals("test1", DeduplicationService.extractKeyField(test, "field1").get().asText());
			assertEquals("nested1", DeduplicationService.extractKeyField(test, "nested1.nested_field").get().asText());
			
			// multi-field
			final String expected = "{\"field1\":\"test1\",\"nested1.nested_field\":\"nested1\"}";
			assertEquals(expected, DeduplicationService.extractKeyFields(test, Arrays.asList("field1", "nested1.nested_field")).get().toString());
			assertEquals(expected, DeduplicationService.extractKeyFields(test, Arrays.asList("field1", "nested1.nested_field", "field3")).get().toString());
		}
		// Similar but with a stream of objects
		{
			final ObjectNode test1 = _mapper.createObjectNode();
			test1.put("field1", "test1");
			final ObjectNode test2 = _mapper.createObjectNode();
			test2.put("field2", "test2");
			
			final List<Tuple2<Long, IBatchRecord>> batch =
					Arrays.<JsonNode>asList(
							test1,
							test2
							)
							.stream()
							.map(j -> Tuples._2T(0L, (IBatchRecord)new BatchRecordUtils.JsonBatchRecord(j)))
							.collect(Collectors.toList());

			assertEquals(Arrays.asList(new TextNode("test1")), DeduplicationService.extractKeyField(batch.stream(), "field1").stream().map(t2 -> t2._1()).collect(Collectors.toList()));
			assertEquals(Arrays.asList("{\"field1\":\"test1\"}"), DeduplicationService.extractKeyField(batch.stream(), "field1").stream().map(t2 -> t2._2()._2().getJson().toString()).collect(Collectors.toList()));
			assertEquals(Arrays.asList("{\"field1\":\"test1\"}"), DeduplicationService.extractKeyFields(batch.stream(), Arrays.asList("field1")).stream().map(t2 -> t2._1().toString()).collect(Collectors.toList()));
			assertEquals(Arrays.asList("{\"field1\":\"test1\"}"), DeduplicationService.extractKeyFields(batch.stream(), Arrays.asList("field1")).stream().map(t2 -> t2._2()._2().getJson().toString()).collect(Collectors.toList()));			
			
			//(keep the old results since i keep changing my mind!)
//			assertEquals(Arrays.asList(new TextNode("test1"), _mapper.createObjectNode()), DeduplicationService.extractKeyField(batch.stream(), "field1").stream().map(t2 -> t2._1()).collect(Collectors.toList()));
//			assertEquals(Arrays.asList("{\"field1\":\"test1\"}", "{\"field2\":\"test2\"}"), DeduplicationService.extractKeyField(batch.stream(), "field1").stream().map(t2 -> t2._2()._2().getJson().toString()).collect(Collectors.toList()));
//			assertEquals(Arrays.asList("{\"field1\":\"test1\"}", "{}"), DeduplicationService.extractKeyFields(batch.stream(), Arrays.asList("field1")).stream().map(t2 -> t2._1().toString()).collect(Collectors.toList()));
//			assertEquals(Arrays.asList("{\"field1\":\"test1\"}", "{\"field2\":\"test2\"}"), DeduplicationService.extractKeyFields(batch.stream(), Arrays.asList("field1")).stream().map(t2 -> t2._2()._2().getJson().toString()).collect(Collectors.toList()));			
		}
		// Another utility function related to these
		{
			final ObjectNode test1 = _mapper.createObjectNode();
			test1.put("field1", "test1");
			
			assertEquals(new TextNode("test1"), DeduplicationService.getKeyFieldsAgain(test1, Either.left("field1")).get());
			assertEquals("{\"field1\":\"test1\"}", DeduplicationService.getKeyFieldsAgain(test1, Either.right(Arrays.asList("field1"))).get().toString());
		}
		
	}
	
	@Test
	public void test_getDedupQuery() {
		final ObjectNode test1 = _mapper.createObjectNode();
		final ObjectNode test_nest1 = _mapper.createObjectNode();
		test1.put("field_1", "test1a");
		test1.put("field_2", "test1b");
		test_nest1.put("nested_1", "nested1");
		test1.put("nested", test_nest1);
		final ObjectNode test2 = _mapper.createObjectNode();
		final ObjectNode test_nest2 = _mapper.createObjectNode();
		test2.put("field_1", "test2a");
		test1.put("field_2", "test2b");
		test_nest2.put("nested_1", "nested2");
		test2.put("nested", test_nest2);
		
		final List<Tuple2<Long, IBatchRecord>> batch =
				Arrays.<JsonNode>asList(
						test1,
						test2
						)
						.stream()
						.map(j -> Tuples._2T(0L, (IBatchRecord)new BatchRecordUtils.JsonBatchRecord(j)))
						.collect(Collectors.toList());
		
		
		// single non-nested query
		{
			final Tuple3<QueryComponent<JsonNode>, List<Tuple2<JsonNode, Tuple2<Long, IBatchRecord>>>, Either<String, List<String>>> res =
					DeduplicationService.getDedupQuery(batch.stream(), Arrays.asList("field_1"), f -> f);
			
			assertEquals("(SingleQueryComponent: limit=2147483647 sort=(none) op=all_of element=(none) extra={field_1=[(any_of,([test1a, test2a],null))]})", res._1().toString());
			//_2 is adequately tested by test_extractKeyOrKeys 
			assertEquals(2, res._2().size());
			assertEquals(Either.left("field_1"), res._3());
		}
		// single nested query
		{
			final Tuple3<QueryComponent<JsonNode>, List<Tuple2<JsonNode, Tuple2<Long, IBatchRecord>>>, Either<String, List<String>>> res =
					DeduplicationService.getDedupQuery(batch.stream(), Arrays.asList("nested.nested_1"), f -> f);
			
			assertEquals("(SingleQueryComponent: limit=2147483647 sort=(none) op=all_of element=(none) extra={nested.nested_1=[(any_of,([nested1, nested2],null))]})", res._1().toString());
			//_2 is adequately tested by test_extractKeyOrKeys 
			assertEquals(2, res._2().size());
			assertEquals(Either.left("nested.nested_1"), res._3());
		}
		// single query, no matches
		{
			final Tuple3<QueryComponent<JsonNode>, List<Tuple2<JsonNode, Tuple2<Long, IBatchRecord>>>, Either<String, List<String>>> res =
					DeduplicationService.getDedupQuery(batch.stream(), Arrays.asList("field_3"), f -> f);
			
			assertEquals(0, res._2().size());
		}
		// multi-query
		{
			final Tuple3<QueryComponent<JsonNode>, List<Tuple2<JsonNode, Tuple2<Long, IBatchRecord>>>, Either<String, List<String>>> res =
					DeduplicationService.getDedupQuery(batch.stream(), Arrays.asList("field_1", "nested.nested_1"), f -> f);
			
			assertEquals("(MultiQueryComponent: limit=2147483647 sort=(none) op=any_of elements=(SingleQueryComponent: limit=(none) sort=(none) op=all_of element=(none) extra={field_1=[(equals,(test1a,null))], nested.nested_1=[(equals,(nested1,null))]});(SingleQueryComponent: limit=(none) sort=(none) op=all_of element=(none) extra={field_1=[(equals,(test2a,null))], nested.nested_1=[(equals,(nested2,null))]}))", res._1().toString());
			//_2 is adequately tested by test_extractKeyOrKeys 
			assertEquals(2, res._2().size());
			assertEquals(Either.right(Arrays.asList("field_1", "nested.nested_1")), res._3());
			
		}
		// multi-query, no matches
		{
			final Tuple3<QueryComponent<JsonNode>, List<Tuple2<JsonNode, Tuple2<Long, IBatchRecord>>>, Either<String, List<String>>> res =
					DeduplicationService.getDedupQuery(batch.stream(), Arrays.asList("field_3", "nested.nested_2"), f -> f);
			
			assertEquals(0, res._2().size());
		}
		
	}
	
	public static class TimeTestBean {
		TimeTestBean(Date d_, Long l_, String s_, String err_) {
			d = d_; l = l_; s = s_; err = err_;
		}
		Date d;
		Long l;
		String s;
		String err;
	}
	
	@Test
	public void test_timestamps() {
		
		final Date d = new Date(0L);
		final Date d2 = new Date();
		final TimeTestBean bean1 = new TimeTestBean(d, d.getTime(), null, "banana"); //(old)
		final JsonNode json1 = BeanTemplateUtils.toJson(bean1);
		final TimeTestBean bean2 = new TimeTestBean(d2, null, d.toInstant().toString(), null); //(new)
		final JsonNode json2 = BeanTemplateUtils.toJson(bean2);
		
		// Check some very basic time utils
		{
			assertEquals(0L, DeduplicationService.getTimestampFromJsonNode(json1.get("d")).get().longValue());
			assertEquals(0L, DeduplicationService.getTimestampFromJsonNode(json1.get("l")).get().longValue());
			assertEquals(0L, DeduplicationService.getTimestampFromJsonNode(json2.get("s")).get().longValue());
			assertEquals(Optional.empty(), DeduplicationService.getTimestampFromJsonNode(json1.get("err")));;
			assertEquals(Optional.empty(), DeduplicationService.getTimestampFromJsonNode(json2.get("err")));;
			assertEquals(Optional.empty(), DeduplicationService.getTimestampFromJsonNode(_mapper.createObjectNode())); //(code coverage!)
		}
		// Compare sets of 2 json objects
		{
			assertTrue(DeduplicationService.newRecordUpdatesOld("d", json2, json1));
			assertFalse(DeduplicationService.newRecordUpdatesOld("d", json1, json2));
			assertFalse(DeduplicationService.newRecordUpdatesOld("d", json1, json1));
			
			// (new doesn't have field so always false)
			assertFalse(DeduplicationService.newRecordUpdatesOld("l", json2, json1));
			assertFalse(DeduplicationService.newRecordUpdatesOld("l", json1, json2));
			assertFalse(DeduplicationService.newRecordUpdatesOld("l", json1, json1));
			
			// (old doesn't have field so always false)
			assertFalse(DeduplicationService.newRecordUpdatesOld("s", json2, json1));
			assertFalse(DeduplicationService.newRecordUpdatesOld("s", json1, json2));
			assertFalse(DeduplicationService.newRecordUpdatesOld("s", json1, json1));
		}
		
	}
	
	@Test
	public void test_handleCustomDeduplication() {
	
		TestDedupEnrichmentModule test_module = new TestDedupEnrichmentModule();
		
		// dummy context
		DeduplicationEnrichmentContext test_context = Mockito.mock(DeduplicationEnrichmentContext.class);
		Mockito.doNothing().when(test_context).resetMutableState(Mockito.any(), Mockito.any());
		Mockito.when(test_context.getObjectIdsToDelete()).thenReturn(Arrays.asList((JsonNode)new TextNode("a")).stream());
		
		_called_batch.set(0);
		assertEquals(0L, _called_batch.get());

		final ObjectNode test1 = _mapper.createObjectNode();
		test1.put("field1", "test1");
		final ObjectNode test2 = _mapper.createObjectNode();
		test2.put("field2", "test2");
		
		final List<Tuple3<Long, IBatchRecord, ObjectNode>> batch =
				Arrays.<JsonNode>asList(
						test1,
						test2
						)
						.stream()
						.map(j -> Tuples._3T(0L, (IBatchRecord)new BatchRecordUtils.JsonBatchRecord(j), _mapper.createObjectNode()))
						.collect(Collectors.toList());
		
		
		DeduplicationService.handleCustomDeduplication(Optional.empty(), batch.stream().collect(Collectors.toList()), Arrays.asList(test2), _mapper.createObjectNode());
		
		assertEquals(0L, _called_batch.get());

		final Stream<JsonNode> ret_val = DeduplicationService.handleCustomDeduplication(Optional.of(Tuples._2T(test_module, test_context)), batch.stream().collect(Collectors.toList()), Arrays.asList(test2), _mapper.createObjectNode());
		
		assertEquals(3L, _called_batch.get());
		
		assertEquals(1L, ret_val.count());
	}
	
	@Test
	public void test_getIncludeFields() {
		
		final List<String> fields = Arrays.asList("test1", "test2");
		final String ts_field = "@timestamp";
		
		assertEquals(Arrays.asList("_id", "test1", "test2"), DeduplicationService.getIncludeFields(DeduplicationPolicy.leave, fields, ts_field)._1());
		assertEquals(true, DeduplicationService.getIncludeFields(DeduplicationPolicy.leave, fields, ts_field)._2());
		assertEquals(Arrays.asList("_id", "@timestamp", "test1", "test2"), DeduplicationService.getIncludeFields(DeduplicationPolicy.update, fields, ts_field)._1());
		assertEquals(true, DeduplicationService.getIncludeFields(DeduplicationPolicy.update, fields, ts_field)._2());
		assertEquals(Arrays.asList("_id", "test1", "test2"), DeduplicationService.getIncludeFields(DeduplicationPolicy.overwrite, fields, ts_field)._1());
		assertEquals(true, DeduplicationService.getIncludeFields(DeduplicationPolicy.overwrite, fields, ts_field)._2());
		assertEquals(Arrays.asList(), DeduplicationService.getIncludeFields(DeduplicationPolicy.custom, fields, ts_field)._1());
		assertEquals(false, DeduplicationService.getIncludeFields(DeduplicationPolicy.custom, fields, ts_field)._2());
		assertEquals(Arrays.asList(), DeduplicationService.getIncludeFields(DeduplicationPolicy.custom_update, fields, ts_field)._1());
		assertEquals(false, DeduplicationService.getIncludeFields(DeduplicationPolicy.custom_update, fields, ts_field)._2());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void test_handleDuplicateRecord() {
		
		final IEnrichmentModuleContext enrich_context = Mockito.mock(IEnrichmentModuleContext.class);
		
		Mockito.when(enrich_context.emitImmutableObject(Mockito.any(Long.class), Mockito.any(JsonNode.class), Mockito.any(Optional.class), Mockito.any(Optional.class), Mockito.any(Optional.class)))
					.thenReturn(Validation.success(_mapper.createObjectNode()));
				
		TestDedupEnrichmentModule test_module = new TestDedupEnrichmentModule();
		
		final String ts_field = "@timestamp";
		
		final ObjectNode old_json = _mapper.createObjectNode();
		old_json.put("_id", "old_record");
		old_json.put("@timestamp", 0L);
		old_json.put("url", "test");
		
		final ObjectNode new_json = _mapper.createObjectNode();
		new_json.put("@timestamp", 1L);
		new_json.put("url", "test");
		
		final ObjectNode new_json_but_same_time = _mapper.createObjectNode();
		new_json_but_same_time.put("@timestamp", 0L);
		new_json_but_same_time.put("url", "test");
		
		Tuple3<Long, IBatchRecord, ObjectNode> new_record = Tuples._3T(0L, new BatchRecordUtils.JsonBatchRecord(new_json), _mapper.createObjectNode());
		Tuple3<Long, IBatchRecord, ObjectNode> new_record_but_same_time = Tuples._3T(0L, new BatchRecordUtils.JsonBatchRecord(new_json_but_same_time), _mapper.createObjectNode());
		
		new_record._2().getContent(); //(code coverage!)
		
		final TextNode key = new TextNode("url");
		
		LinkedHashMap<JsonNode, LinkedList<Tuple3<Long, IBatchRecord, ObjectNode>>> mutable_obj_map = new LinkedHashMap<>();
		
		final LinkedList<Tuple3<Long, IBatchRecord, ObjectNode>> new_records = Stream.of(new_record).collect(Collectors.toCollection(LinkedList::new));
		final LinkedList<Tuple3<Long, IBatchRecord, ObjectNode>> new_records_but_same_time = Stream.of(new_record_but_same_time).collect(Collectors.toCollection(LinkedList::new));
		
		// Simple case Leave policy
		{
			//(reset)
			mutable_obj_map.clear();
			mutable_obj_map.put(new TextNode("never_changed"), new_records);
			mutable_obj_map.put(new TextNode("url"), new_records);
			assertEquals(2, mutable_obj_map.size());
			new_record._3().removeAll();
			new_record_but_same_time._3().removeAll();
			_called_batch.set(0);
			
			DocumentSchemaBean config = 
					BeanTemplateUtils.build(DocumentSchemaBean.class)
						.with(DocumentSchemaBean::deduplication_policy, DeduplicationPolicy.leave)
					.done().get();
			DeduplicationEnrichmentContext test_context = new DeduplicationEnrichmentContext(enrich_context, config, j -> Optional.empty());
			
			final Stream<JsonNode> ret_val = DeduplicationService.handleDuplicateRecord(config,
					Optional.of(Tuples._2T(test_module, test_context)), ts_field, new_records, Arrays.asList(old_json), key, mutable_obj_map
					);			
			assertEquals(0L, ret_val.count());
			
			// Nothing emitted
			Mockito.verify(enrich_context, Mockito.times(0)).emitImmutableObject(Mockito.any(Long.class), Mockito.any(JsonNode.class), Mockito.any(Optional.class), Mockito.any(Optional.class), Mockito.any(Optional.class));
			// No custom processing performed
			assertEquals(0, _called_batch.get());
			// No annotations/mutations
			assertEquals("{}", new_record._3().toString());
			// Object removed from mutable map
			assertEquals(1, mutable_obj_map.size());
		}
		// Simple case update policy - time updates
		final Consumer<Boolean> test_time_updates = delete_unhandled -> {
			//(reset)
			mutable_obj_map.clear();
			mutable_obj_map.put(new TextNode("never_changed"), new_records);
			mutable_obj_map.put(new TextNode("url"), new_records);
			assertEquals(2, mutable_obj_map.size());
			new_record._3().removeAll();
			new_record_but_same_time._3().removeAll();
			_called_batch.set(0);
			
			DocumentSchemaBean config = 
					BeanTemplateUtils.build(DocumentSchemaBean.class)
						.with(DocumentSchemaBean::deduplication_policy, DeduplicationPolicy.update)
						.with(DocumentSchemaBean::delete_unhandled_duplicates, delete_unhandled)
					.done().get();
			DeduplicationEnrichmentContext test_context = new DeduplicationEnrichmentContext(enrich_context, config, j -> Optional.empty());
			
			// (add the same object twice to test the "return ids to delete" functionality)
			final Stream<JsonNode> ret_val = DeduplicationService.handleDuplicateRecord(config,
					Optional.of(Tuples._2T(test_module, test_context)), ts_field, new_records, Arrays.asList(old_json, old_json), key, mutable_obj_map
					);
			if (delete_unhandled) {
				assertEquals(Arrays.asList("old_record"), ret_val.sorted().map(j -> DeduplicationService.jsonToObject(j)).collect(Collectors.toList()));				
			}
			else {
				assertEquals(0L, ret_val.count());
			}
			
			// Nothing emitted
			Mockito.verify(enrich_context, Mockito.times(0)).emitImmutableObject(Mockito.any(Long.class), Mockito.any(JsonNode.class), Mockito.any(Optional.class), Mockito.any(Optional.class), Mockito.any(Optional.class));
			// No custom processing performed
			assertEquals(0, _called_batch.get());
			// _id
			assertEquals("{\"_id\":\"old_record\"}", new_record._3().toString());
			// Object removed from mutable map
			assertEquals(2, mutable_obj_map.size());
		};
		test_time_updates.accept(true);
		test_time_updates.accept(false);
		
		// Simple case update policy - times the same
		{
			//(reset)
			mutable_obj_map.clear();
			mutable_obj_map.put(new TextNode("never_changed"), new_records);
			mutable_obj_map.put(new TextNode("url"), new_records);
			new_record._3().removeAll();
			new_record_but_same_time._3().removeAll();
			_called_batch.set(0);
			
			DocumentSchemaBean config = 
					BeanTemplateUtils.build(DocumentSchemaBean.class)
						.with(DocumentSchemaBean::deduplication_policy, DeduplicationPolicy.update)
						.with(DocumentSchemaBean::delete_unhandled_duplicates, false)
					.done().get();
			DeduplicationEnrichmentContext test_context = new DeduplicationEnrichmentContext(enrich_context, config, j -> Optional.empty());
			
			final Stream<JsonNode> ret_val = DeduplicationService.handleDuplicateRecord(config,
					Optional.of(Tuples._2T(test_module, test_context)), ts_field, new_records_but_same_time, Arrays.asList(old_json), key, mutable_obj_map
					);
			assertEquals(0L, ret_val.count());
			
			// Nothing emitted
			Mockito.verify(enrich_context, Mockito.times(0)).emitImmutableObject(Mockito.any(Long.class), Mockito.any(JsonNode.class), Mockito.any(Optional.class), Mockito.any(Optional.class), Mockito.any(Optional.class));
			// No custom processing performed
			assertEquals(0, _called_batch.get());
			// No annotations/mutations
			assertEquals("{}", new_record_but_same_time._3().toString());
			// Object removed from mutable map
			assertEquals(1, mutable_obj_map.size());
		}
		// overwrite
		final Consumer<Boolean> test_overwrites = delete_unhandled -> {
			//(reset)
			mutable_obj_map.clear();
			mutable_obj_map.put(new TextNode("never_changed"), new_records);
			mutable_obj_map.put(new TextNode("url"), new_records);
			assertEquals(2, mutable_obj_map.size());
			new_record._3().removeAll();
			new_record_but_same_time._3().removeAll();
			_called_batch.set(0);
			
			DocumentSchemaBean config = 
					BeanTemplateUtils.build(DocumentSchemaBean.class)
						.with(DocumentSchemaBean::deduplication_policy, DeduplicationPolicy.overwrite)
						.with(DocumentSchemaBean::delete_unhandled_duplicates, delete_unhandled)
					.done().get();
			DeduplicationEnrichmentContext test_context = new DeduplicationEnrichmentContext(enrich_context, config, j -> Optional.empty());
			
			final Stream<JsonNode> ret_val = DeduplicationService.handleDuplicateRecord(config,
					Optional.of(Tuples._2T(test_module, test_context)), ts_field, new_records, Arrays.asList(old_json, old_json), key, mutable_obj_map
					);
			if (delete_unhandled) {
				assertEquals(Arrays.asList("old_record"), ret_val.sorted().map(j -> DeduplicationService.jsonToObject(j)).collect(Collectors.toList()));				
			}
			else {
				assertEquals(0L, ret_val.count());
			}
			
			// Nothing emitted
			Mockito.verify(enrich_context, Mockito.times(0)).emitImmutableObject(Mockito.any(Long.class), Mockito.any(JsonNode.class), Mockito.any(Optional.class), Mockito.any(Optional.class), Mockito.any(Optional.class));
			// No custom processing performed
			assertEquals(0, _called_batch.get());
			// _id
			assertEquals("{\"_id\":\"old_record\"}", new_record._3().toString());
			// Object removed from mutable map
			assertEquals(2, mutable_obj_map.size());			
		};
		test_overwrites.accept(true);
		test_overwrites.accept(false);
		
		//(check ignores times)
		{
			//(reset)
			mutable_obj_map.clear();
			mutable_obj_map.put(new TextNode("never_changed"), new_records);
			mutable_obj_map.put(new TextNode("url"), new_records);
			assertEquals(2, mutable_obj_map.size());
			new_record._3().removeAll();
			new_record_but_same_time._3().removeAll();
			_called_batch.set(0);
			
			DocumentSchemaBean config = 
					BeanTemplateUtils.build(DocumentSchemaBean.class)
						.with(DocumentSchemaBean::deduplication_policy, DeduplicationPolicy.overwrite)
						.with(DocumentSchemaBean::delete_unhandled_duplicates, false)
					.done().get();
			DeduplicationEnrichmentContext test_context = new DeduplicationEnrichmentContext(enrich_context, config, j -> Optional.empty());
			
			final Stream<JsonNode> ret_val = DeduplicationService.handleDuplicateRecord(config,
					Optional.of(Tuples._2T(test_module, test_context)), ts_field, new_records_but_same_time, Arrays.asList(old_json), key, mutable_obj_map
					);
			assertEquals(0L, ret_val.count());

			// Nothing emitted
			Mockito.verify(enrich_context, Mockito.times(0)).emitImmutableObject(Mockito.any(Long.class), Mockito.any(JsonNode.class), Mockito.any(Optional.class), Mockito.any(Optional.class), Mockito.any(Optional.class));
			// No custom processing performed
			assertEquals(0, _called_batch.get());
			// _id
			assertEquals("{\"_id\":\"old_record\"}", new_record_but_same_time._3().toString());
			// Object removed from mutable map
			assertEquals(2, mutable_obj_map.size());			
		}
		// custom
		{
			//(reset)
			mutable_obj_map.clear();
			mutable_obj_map.put(new TextNode("never_changed"), new_records);
			mutable_obj_map.put(new TextNode("url"), new_records);
			assertEquals(2, mutable_obj_map.size());
			new_record._3().removeAll();
			new_record_but_same_time._3().removeAll();
			_called_batch.set(0);
			
			DocumentSchemaBean config = 
					BeanTemplateUtils.build(DocumentSchemaBean.class)
						.with(DocumentSchemaBean::deduplication_policy, DeduplicationPolicy.custom)
						.with(DocumentSchemaBean::delete_unhandled_duplicates, false)
					.done().get();
			DeduplicationEnrichmentContext test_context = new DeduplicationEnrichmentContext(enrich_context, config, j -> Optional.empty());
			
			final Stream<JsonNode> ret_val = DeduplicationService.handleDuplicateRecord(config,
					Optional.of(Tuples._2T(test_module, test_context)), ts_field, new_records, Arrays.asList(old_json), key, mutable_obj_map
					);
			assertEquals(0L, ret_val.count());
			
			// Nothing emitted
			Mockito.verify(enrich_context, Mockito.times(0)).emitImmutableObject(Mockito.any(Long.class), Mockito.any(JsonNode.class), Mockito.any(Optional.class), Mockito.any(Optional.class), Mockito.any(Optional.class));
			// No custom processing performed
			assertEquals(2, _called_batch.get()); //(old + new)
			// _id
			assertEquals("{}", new_record._3().toString()); // up to the custom code to do this
			// Object removed from mutable map
			assertEquals(1, mutable_obj_map.size()); //(remove since it's the responsibility of the custom code to emit)
		}
		//(check ignores times)
		{
			//(reset)
			mutable_obj_map.clear();
			mutable_obj_map.put(new TextNode("never_changed"), new_records);
			mutable_obj_map.put(new TextNode("url"), new_records);
			assertEquals(2, mutable_obj_map.size());
			new_record._3().removeAll();
			new_record_but_same_time._3().removeAll();
			_called_batch.set(0);
			
			DocumentSchemaBean config = 
					BeanTemplateUtils.build(DocumentSchemaBean.class)
						.with(DocumentSchemaBean::deduplication_policy, DeduplicationPolicy.custom)
						.with(DocumentSchemaBean::delete_unhandled_duplicates, false)
					.done().get();
			DeduplicationEnrichmentContext test_context = new DeduplicationEnrichmentContext(enrich_context, config, j -> Optional.empty());
			
			final Stream<JsonNode> ret_val = DeduplicationService.handleDuplicateRecord(config,
					Optional.of(Tuples._2T(test_module, test_context)), ts_field, new_records_but_same_time, Arrays.asList(old_json), key, mutable_obj_map
					);
			assertEquals(0L, ret_val.count());
			
			// Nothing emitted
			Mockito.verify(enrich_context, Mockito.times(0)).emitImmutableObject(Mockito.any(Long.class), Mockito.any(JsonNode.class), Mockito.any(Optional.class), Mockito.any(Optional.class), Mockito.any(Optional.class));
			// No custom processing performed
			assertEquals(2, _called_batch.get()); //(old + new)
			// _id
			assertEquals("{}", new_record_but_same_time._3().toString()); // up to the custom code to do this
			// Object removed from mutable map
			assertEquals(1, mutable_obj_map.size()); //(remove since it's the responsibility of the custom code to emit)
		}
		// Simple case *custom* update policy - time updates
		{
			//(reset)
			mutable_obj_map.clear();
			mutable_obj_map.put(new TextNode("never_changed"), new_records);
			mutable_obj_map.put(new TextNode("url"), new_records);
			assertEquals(2, mutable_obj_map.size());
			new_record._3().removeAll();
			new_record_but_same_time._3().removeAll();
			_called_batch.set(0);
			
			DocumentSchemaBean config = 
					BeanTemplateUtils.build(DocumentSchemaBean.class)
						.with(DocumentSchemaBean::deduplication_policy, DeduplicationPolicy.custom_update)
						.with(DocumentSchemaBean::delete_unhandled_duplicates, false)
					.done().get();
			DeduplicationEnrichmentContext test_context = new DeduplicationEnrichmentContext(enrich_context, config, j -> Optional.empty());
			
			final Stream<JsonNode> ret_val = DeduplicationService.handleDuplicateRecord(config,
					Optional.of(Tuples._2T(test_module, test_context)), ts_field, new_records, Arrays.asList(old_json), key, mutable_obj_map
					);
			assertEquals(0L, ret_val.count());
			
			// Nothing emitted
			Mockito.verify(enrich_context, Mockito.times(0)).emitImmutableObject(Mockito.any(Long.class), Mockito.any(JsonNode.class), Mockito.any(Optional.class), Mockito.any(Optional.class), Mockito.any(Optional.class));
			// No custom processing performed
			assertEquals(2, _called_batch.get()); //(old + new)
			// _id
			assertEquals("{}", new_record._3().toString()); // up to the custom code to do this
			// Object removed from mutable map
			assertEquals(1, mutable_obj_map.size()); //(remove since it's the responsibility of the custom code to emit)
		}
		// Simple case *custom* update policy - times the same
		{
			//(reset)
			mutable_obj_map.clear();
			mutable_obj_map.put(new TextNode("never_changed"), new_records);
			mutable_obj_map.put(new TextNode("url"), new_records);
			assertEquals(2, mutable_obj_map.size());
			new_record._3().removeAll();
			new_record_but_same_time._3().removeAll();
			_called_batch.set(0);
			
			DocumentSchemaBean config = 
					BeanTemplateUtils.build(DocumentSchemaBean.class)
						.with(DocumentSchemaBean::deduplication_policy, DeduplicationPolicy.custom_update)
						.with(DocumentSchemaBean::delete_unhandled_duplicates, false)
					.done().get();
			DeduplicationEnrichmentContext test_context = new DeduplicationEnrichmentContext(enrich_context, config, j -> Optional.empty());
			
			final Stream<JsonNode> ret_val = DeduplicationService.handleDuplicateRecord(config,
					Optional.of(Tuples._2T(test_module, test_context)), ts_field, new_records_but_same_time, Arrays.asList(old_json), key, mutable_obj_map
					);
			assertEquals(0L, ret_val.count());
			
			// Nothing emitted
			Mockito.verify(enrich_context, Mockito.times(0)).emitImmutableObject(Mockito.any(Long.class), Mockito.any(JsonNode.class), Mockito.any(Optional.class), Mockito.any(Optional.class), Mockito.any(Optional.class));
			// No custom processing performed
			assertEquals(0, _called_batch.get());
			// No annotations/mutations
			assertEquals("{}", new_record_but_same_time._3().toString());
			// Object removed from mutable map
			assertEquals(1, mutable_obj_map.size());
		}
		
	}

	////////////////////////////////////////////////////
	
	protected final int num_write_records = 500;
	
	@SuppressWarnings("unchecked")
	@Test
	public void test_puttingItAllTogether() throws InterruptedException {
		
		test_puttingItAllTogether_genericPhase();
		
		// Generic intialization:
		
		final String ts_field = "@timestamp";
		
		System.out.println("STARTING DEDUP TEST NOW: " + new Date());

		// Test 0: NO DEDUP
		{
			final IEnrichmentModuleContext enrich_context = getMockEnrichmentContext();
			
			final DataBucketBean write_bucket = addTimestampField(ts_field, getDocBucket("/test/dedup/write/a",
					BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
					.done().get()
					));
			
			// Test
			
			test_puttingItAllTogether_runTest(write_bucket, enrich_context);			
			
			// Things to check:
			
			// Should have called emit 2*"num_write_records" times (no dedup), + 10% dups
			Mockito.verify(enrich_context, Mockito.times(22*num_write_records/10)).emitImmutableObject(Mockito.any(Long.class), Mockito.any(JsonNode.class), Mockito.any(Optional.class), Mockito.any(Optional.class), Mockito.any(Optional.class));
		}
		
		// Test 1: LEAVE
		
		// TEST 1a: LEAVE, MULTI-FIELD, MULTI-BUCKET CONTEXT
		{
			final IEnrichmentModuleContext enrich_context = getMockEnrichmentContext();
			
			final DataBucketBean write_bucket = addTimestampField(ts_field, getDocBucket("/test/dedup/write/a",
					BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_policy, DeduplicationPolicy.leave)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_contexts, Arrays.asList("/dedup/*"))
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_fields, Arrays.asList("dup_field", "dup"))
					.done().get()
					));
			
			// Test
			
			test_puttingItAllTogether_runTest(write_bucket, enrich_context);			
			
			// Things to check:
			
			// Should have called emit "num_write_records" times (50% of them are duplicates)
			Mockito.verify(enrich_context, Mockito.times(num_write_records)).emitImmutableObject(Mockito.any(Long.class), Mockito.any(JsonNode.class), Mockito.any(Optional.class), Mockito.any(Optional.class), Mockito.any(Optional.class));
		}		
		// TEST 1b: LEAVE, MULTI-FIELD, SINGLE-BUCKET CONTEXT
		{
			final IEnrichmentModuleContext enrich_context = getMockEnrichmentContext();
			
			final DataBucketBean write_bucket = addTimestampField(ts_field, getDocBucket("/dedup/context1",
					BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_policy, DeduplicationPolicy.leave)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_fields, Arrays.asList("dup_field", "dup"))
					.done().get()
					));
			
			// Test
			
			test_puttingItAllTogether_runTest(write_bucket, enrich_context);			
			
			// Things to check:
			
			// Should have called emit "num_write_records" times (50% of them are duplicates)
			Mockito.verify(enrich_context, Mockito.times(num_write_records)).emitImmutableObject(Mockito.any(Long.class), Mockito.any(JsonNode.class), Mockito.any(Optional.class), Mockito.any(Optional.class), Mockito.any(Optional.class));
		}
		// TEST 1c: LEAVE, SINGLE-FIELD, MULTI-BUCKET CONTEXT
		{
			final IEnrichmentModuleContext enrich_context = getMockEnrichmentContext();
			
			final DataBucketBean write_bucket = addTimestampField(ts_field, getDocBucket("/test/dedup/write/c",
					BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_policy, DeduplicationPolicy.leave)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_contexts, Arrays.asList("/dedup/*"))
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_fields, Arrays.asList("alt_dup_field"))
					.done().get()
					));
			
			// Test
			
			test_puttingItAllTogether_runTest(write_bucket, enrich_context);			
			
			// Things to check:
			
			// Should have called emit "num_write_records" times (50% of them are duplicates)
			Mockito.verify(enrich_context, Mockito.times(num_write_records)).emitImmutableObject(Mockito.any(Long.class), Mockito.any(JsonNode.class), Mockito.any(Optional.class), Mockito.any(Optional.class), Mockito.any(Optional.class));
		}		
		// TEST 1d: LEAVE, MULTI-FIELD, SINGLE-BUCKET CONTEXT
		{
			final IEnrichmentModuleContext enrich_context = getMockEnrichmentContext();
			
			final DataBucketBean write_bucket = addTimestampField(ts_field, getDocBucket("/dedup/context1",
					BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_policy, DeduplicationPolicy.leave)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_fields, Arrays.asList("alt_dup_field"))
					.done().get()
					));
			
			// Test
			
			test_puttingItAllTogether_runTest(write_bucket, enrich_context);			
			
			// Things to check:
			
			// Should have called emit "num_write_records" times (50% of them are duplicates)
			Mockito.verify(enrich_context, Mockito.times(num_write_records)).emitImmutableObject(Mockito.any(Long.class), Mockito.any(JsonNode.class), Mockito.any(Optional.class), Mockito.any(Optional.class), Mockito.any(Optional.class));
		}
		
		// Test 2: OVERWRITE
		
		// TEST 2a: OVERWRITE, MULTI-FIELD, MULTI-BUCKET CONTEXT
		{
			final IEnrichmentModuleContext enrich_context = getMockEnrichmentContext();
			
			final DataBucketBean write_bucket = addTimestampField(ts_field, getDocBucket("/test/dedup/write/a",
					BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_policy, DeduplicationPolicy.overwrite)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_contexts, Arrays.asList("/dedup/*"))
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_fields, Arrays.asList("dup_field", "dup"))
					.done().get()
					));
			
			// Test
			
			test_puttingItAllTogether_runTest(write_bucket, enrich_context);			
			
			// Things to check:
			
			// Should have called emit 2*"num_write_records" times (50% of them are duplicates but they get overwritten)
			Mockito.verify(enrich_context, Mockito.times(2*num_write_records)).emitImmutableObject(Mockito.any(Long.class), Mockito.any(JsonNode.class), Mockito.any(Optional.class), Mockito.any(Optional.class), Mockito.any(Optional.class));
		}		
		// TEST 2b: OVERWRITE, MULTI-FIELD, SINGLE-BUCKET CONTEXT
		{
			final IEnrichmentModuleContext enrich_context = getMockEnrichmentContext();
			
			final DataBucketBean write_bucket = addTimestampField(ts_field, getDocBucket("/dedup/context1",
					BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_policy, DeduplicationPolicy.overwrite)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_fields, Arrays.asList("dup_field", "dup"))
					.done().get()
					));
			
			// Test
			
			test_puttingItAllTogether_runTest(write_bucket, enrich_context);			
			
			// Things to check:
			
			// Should have called emit 2*"num_write_records" times (50% of them are duplicates but they get overwritten)
			Mockito.verify(enrich_context, Mockito.times(2*num_write_records)).emitImmutableObject(Mockito.any(Long.class), Mockito.any(JsonNode.class), Mockito.any(Optional.class), Mockito.any(Optional.class), Mockito.any(Optional.class));
		}
		// TEST 2c: OVERWRITE, SINGLE-FIELD, MULTI-BUCKET CONTEXT
		{
			final IEnrichmentModuleContext enrich_context = getMockEnrichmentContext();
			
			final DataBucketBean write_bucket = addTimestampField(ts_field, getDocBucket("/test/dedup/write/c",
					BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_policy, DeduplicationPolicy.overwrite)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_contexts, Arrays.asList("/dedup/*"))
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_fields, Arrays.asList("alt_dup_field"))
					.done().get()
					));
			
			// Test
			
			test_puttingItAllTogether_runTest(write_bucket, enrich_context);			
			
			// Things to check:
			
			// Should have called emit 2*"num_write_records" times (50% of them are duplicates but they get overwritten)
			Mockito.verify(enrich_context, Mockito.times(2*num_write_records)).emitImmutableObject(Mockito.any(Long.class), Mockito.any(JsonNode.class), Mockito.any(Optional.class), Mockito.any(Optional.class), Mockito.any(Optional.class));
		}		
		// TEST 2d: OVERWRITE, MULTI-FIELD, SINGLE-BUCKET CONTEXT
		{
			final IEnrichmentModuleContext enrich_context = getMockEnrichmentContext();
			
			final DataBucketBean write_bucket = addTimestampField(ts_field, getDocBucket("/dedup/context1",
					BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_policy, DeduplicationPolicy.overwrite)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_fields, Arrays.asList("alt_dup_field"))
					.done().get()
					));
			
			// Test
			
			test_puttingItAllTogether_runTest(write_bucket, enrich_context);			
			
			// Things to check:
			
			// Should have called emit 2*"num_write_records" times (50% of them are duplicates but they get overwritten)
			Mockito.verify(enrich_context, Mockito.times(2*num_write_records)).emitImmutableObject(Mockito.any(Long.class), Mockito.any(JsonNode.class), Mockito.any(Optional.class), Mockito.any(Optional.class), Mockito.any(Optional.class));
		}
		
		// Test 3: UPDATE
		
		// TEST 3a: UPDATE, MULTI-FIELD, MULTI-BUCKET CONTEXT
		{
			final IEnrichmentModuleContext enrich_context = getMockEnrichmentContext();
			
			final DataBucketBean write_bucket = addTimestampField(ts_field, getDocBucket("/test/dedup/write/a",
					BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_policy, DeduplicationPolicy.update)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_contexts, Arrays.asList("/dedup/*"))
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_fields, Arrays.asList("dup_field", "dup"))
					.done().get()
					));
			
			// Test
			
			test_puttingItAllTogether_runTest(write_bucket, enrich_context);			
			
			// Things to check:
			
			// Should have called emit 1.5*"num_write_records" times (50% of them are duplicates but they 50% of those get overwritten)
			Mockito.verify(enrich_context, Mockito.times(3*num_write_records/2)).emitImmutableObject(Mockito.any(Long.class), Mockito.any(JsonNode.class), Mockito.any(Optional.class), Mockito.any(Optional.class), Mockito.any(Optional.class));
		}		
		// TEST 3b: UPDATE, MULTI-FIELD, SINGLE-BUCKET CONTEXT
		{
			final IEnrichmentModuleContext enrich_context = getMockEnrichmentContext();
			
			final DataBucketBean write_bucket = addTimestampField(ts_field, getDocBucket("/dedup/context1",
					BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_policy, DeduplicationPolicy.update)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_fields, Arrays.asList("dup_field", "dup"))
					.done().get()
					));
			
			// Test
			
			test_puttingItAllTogether_runTest(write_bucket, enrich_context);			
			
			// Things to check:
			
			// Should have called emit 1.5*"num_write_records" times (50% of them are duplicates but they 50% of those get overwritten)
			Mockito.verify(enrich_context, Mockito.times(3*num_write_records/2)).emitImmutableObject(Mockito.any(Long.class), Mockito.any(JsonNode.class), Mockito.any(Optional.class), Mockito.any(Optional.class), Mockito.any(Optional.class));
		}
		// TEST 3c: UPDATE, SINGLE-FIELD, MULTI-BUCKET CONTEXT
		{
			final IEnrichmentModuleContext enrich_context = getMockEnrichmentContext();
			
			final DataBucketBean write_bucket = addTimestampField(ts_field, getDocBucket("/test/dedup/write/c",
					BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_policy, DeduplicationPolicy.update)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_contexts, Arrays.asList("/dedup/*"))
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_fields, Arrays.asList("alt_dup_field"))
					.done().get()
					));
			
			// Test
			
			test_puttingItAllTogether_runTest(write_bucket, enrich_context);			
			
			// Things to check:
			
			// Should have called emit 1.5*"num_write_records" times (50% of them are duplicates but they 50% of those get overwritten)
			Mockito.verify(enrich_context, Mockito.times(3*num_write_records/2)).emitImmutableObject(Mockito.any(Long.class), Mockito.any(JsonNode.class), Mockito.any(Optional.class), Mockito.any(Optional.class), Mockito.any(Optional.class));
		}		
		// TEST 3d: UPDATE, MULTI-FIELD, SINGLE-BUCKET CONTEXT
		{
			final IEnrichmentModuleContext enrich_context = getMockEnrichmentContext();
			
			final DataBucketBean write_bucket = addTimestampField(ts_field, getDocBucket("/dedup/context1",
					BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_policy, DeduplicationPolicy.update)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_fields, Arrays.asList("alt_dup_field"))
					.done().get()
					));
			
			// Test
			
			test_puttingItAllTogether_runTest(write_bucket, enrich_context);			
			
			// Things to check:
			
			// Should have called emit 1.5*"num_write_records" times (50% of them are duplicates but they 50% of those get overwritten)
			Mockito.verify(enrich_context, Mockito.times(3*num_write_records/2)).emitImmutableObject(Mockito.any(Long.class), Mockito.any(JsonNode.class), Mockito.any(Optional.class), Mockito.any(Optional.class), Mockito.any(Optional.class));
		}
		
		// Test 4: CUSTOM
		
		// TEST 4a: CUSTOM, MULTI-FIELD, MULTI-BUCKET CONTEXT
		{
			_called_batch.set(0);
			
			final IEnrichmentModuleContext enrich_context = getMockEnrichmentContext();
			
			final DataBucketBean write_bucket = addTimestampField(ts_field, getDocBucket("/test/dedup/write/a",
					BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_policy, DeduplicationPolicy.custom)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_contexts, Arrays.asList("/dedup/*"))
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_fields, Arrays.asList("dup_field", "dup"))
						.with(DocumentSchemaBean::custom_deduplication_configs,
								Arrays.asList(
										BeanTemplateUtils.build(EnrichmentControlMetadataBean.class)
											.with(EnrichmentControlMetadataBean::name, "custom_test")
											.with(EnrichmentControlMetadataBean::module_name_or_id, "/app/aleph2/library/test.jar")
										.done().get()										
										)
								)
					.done().get()
					));
			
			// Test
			
			test_puttingItAllTogether_runTest(write_bucket, enrich_context);			
			
			// Things to check:
			
			// The 50% of non-matching data objects are emitted
			Mockito.verify(enrich_context, Mockito.times(num_write_records)).emitImmutableObject(Mockito.any(Long.class), Mockito.any(JsonNode.class), Mockito.any(Optional.class), Mockito.any(Optional.class), Mockito.any(Optional.class));
			// The other 50% get passed to the batch (*2, 1 for new + 1 for old) + 10% dups
			assertEquals(num_write_records + 11*num_write_records/10, _called_batch.get());
		}		
		// TEST 4b: CUSTOM, MULTI-FIELD, SINGLE-BUCKET CONTEXT
		{
			_called_batch.set(0);
			
			final IEnrichmentModuleContext enrich_context = getMockEnrichmentContext();
			
			final DataBucketBean write_bucket = addTimestampField(ts_field, getDocBucket("/dedup/context1",
					BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_policy, DeduplicationPolicy.custom)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_fields, Arrays.asList("dup_field", "dup"))
						.with(DocumentSchemaBean::custom_deduplication_configs,
								Arrays.asList(
										BeanTemplateUtils.build(EnrichmentControlMetadataBean.class)
											.with(EnrichmentControlMetadataBean::name, "custom_test")
											.with(EnrichmentControlMetadataBean::module_name_or_id, "/app/aleph2/library/test.jar")
										.done().get()										
										)
								)
					.done().get()
					));
			
			// Test
			
			test_puttingItAllTogether_runTest(write_bucket, enrich_context);			
			
			// Things to check:
			
			// The 50% of non-matching data objects are emitted
			Mockito.verify(enrich_context, Mockito.times(num_write_records)).emitImmutableObject(Mockito.any(Long.class), Mockito.any(JsonNode.class), Mockito.any(Optional.class), Mockito.any(Optional.class), Mockito.any(Optional.class));
			// The other 50% get passed to the batch (*2, 1 for new + 1 for old) + 10% dups
			assertEquals(num_write_records + 11*num_write_records/10, _called_batch.get());
		}
		// TEST 4c: CUSTOM, SINGLE-FIELD, MULTI-BUCKET CONTEXT
		{
			_called_batch.set(0);
			
			final IEnrichmentModuleContext enrich_context = getMockEnrichmentContext();
			
			final DataBucketBean write_bucket = addTimestampField(ts_field, getDocBucket("/test/dedup/write/c",
					BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_policy, DeduplicationPolicy.custom)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_contexts, Arrays.asList("/dedup/*"))
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_fields, Arrays.asList("alt_dup_field"))
						.with(DocumentSchemaBean::custom_deduplication_configs,
								Arrays.asList(
										BeanTemplateUtils.build(EnrichmentControlMetadataBean.class)
											.with(EnrichmentControlMetadataBean::name, "custom_test")
											.with(EnrichmentControlMetadataBean::module_name_or_id, "/app/aleph2/library/test.jar")
										.done().get()										
										)
								)
					.done().get()
					));
			
			// Test
			
			test_puttingItAllTogether_runTest(write_bucket, enrich_context);			
			
			// Things to check:
			
			// The 50% of non-matching data objects are emitted
			Mockito.verify(enrich_context, Mockito.times(num_write_records)).emitImmutableObject(Mockito.any(Long.class), Mockito.any(JsonNode.class), Mockito.any(Optional.class), Mockito.any(Optional.class), Mockito.any(Optional.class));
			// The other 50% get passed to the batch (*2, 1 for new + 1 for old) + 10% dups
			assertEquals(num_write_records + 11*num_write_records/10, _called_batch.get());
		}		
		// TEST 4d: CUSTOM, MULTI-FIELD, SINGLE-BUCKET CONTEXT
		{
			_called_batch.set(0);
			
			final IEnrichmentModuleContext enrich_context = getMockEnrichmentContext();
			
			final DataBucketBean write_bucket = addTimestampField(ts_field, getDocBucket("/dedup/context1",
					BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_policy, DeduplicationPolicy.custom)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_fields, Arrays.asList("alt_dup_field"))
						.with(DocumentSchemaBean::custom_deduplication_configs,
								Arrays.asList(
										BeanTemplateUtils.build(EnrichmentControlMetadataBean.class)
											.with(EnrichmentControlMetadataBean::name, "custom_test")
											.with(EnrichmentControlMetadataBean::module_name_or_id, "/app/aleph2/library/test.jar")
										.done().get()										
										)
								)
					.done().get()
					));
			
			// Test
			
			test_puttingItAllTogether_runTest(write_bucket, enrich_context);			
			
			// Things to check:
			
			// The 50% of non-matching data objects are emitted
			Mockito.verify(enrich_context, Mockito.times(num_write_records)).emitImmutableObject(Mockito.any(Long.class), Mockito.any(JsonNode.class), Mockito.any(Optional.class), Mockito.any(Optional.class), Mockito.any(Optional.class));
			// The other 50% get passed to the batch (*2, 1 for new + 1 for old) + 10% dups
			assertEquals(num_write_records + 11*num_write_records/10, _called_batch.get());
		}
		
		// Test 5: CUSTOM
		
		// TEST 5a: CUSTOM UPDATE, MULTI-FIELD, MULTI-BUCKET CONTEXT
		{
			_called_batch.set(0);
			
			final IEnrichmentModuleContext enrich_context = getMockEnrichmentContext();
			
			final DataBucketBean write_bucket = addTimestampField(ts_field, getDocBucket("/test/dedup/write/a",
					BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_policy, DeduplicationPolicy.custom_update)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_contexts, Arrays.asList("/dedup/*"))
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_fields, Arrays.asList("dup_field", "dup"))
						.with(DocumentSchemaBean::custom_deduplication_configs,
								Arrays.asList(
										BeanTemplateUtils.build(EnrichmentControlMetadataBean.class)
											.with(EnrichmentControlMetadataBean::name, "custom_test")
											.with(EnrichmentControlMetadataBean::module_name_or_id, "/app/aleph2/library/test.jar")
										.done().get()										
										)
								)
					.done().get()
					));
			
			// Test
			
			test_puttingItAllTogether_runTest(write_bucket, enrich_context);			
			
			// Things to check:
			
			// The 50% of non-matching data objects are emitted
			Mockito.verify(enrich_context, Mockito.times(num_write_records)).emitImmutableObject(Mockito.any(Long.class), Mockito.any(JsonNode.class), Mockito.any(Optional.class), Mockito.any(Optional.class), Mockito.any(Optional.class));
			// Of the other 50%, the 50% newer ones get passed to the batch (*2, 1 for new + 1 for old) - but no dups because they are always timestamp==0
			assertEquals(num_write_records, _called_batch.get());
		}		
		// TEST 5b: CUSTOM UPDATE, MULTI-FIELD, SINGLE-BUCKET CONTEXT
		{
			_called_batch.set(0);
			
			final IEnrichmentModuleContext enrich_context = getMockEnrichmentContext();
			
			final DataBucketBean write_bucket = addTimestampField(ts_field, getDocBucket("/dedup/context1",
					BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_policy, DeduplicationPolicy.custom_update)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_fields, Arrays.asList("dup_field", "dup"))
						.with(DocumentSchemaBean::custom_deduplication_configs,
								Arrays.asList(
										BeanTemplateUtils.build(EnrichmentControlMetadataBean.class)
											.with(EnrichmentControlMetadataBean::name, "custom_test")
											.with(EnrichmentControlMetadataBean::module_name_or_id, "/app/aleph2/library/test.jar")
										.done().get()										
										)
								)
					.done().get()
					));
			
			// Test
			
			test_puttingItAllTogether_runTest(write_bucket, enrich_context);			
			
			// Things to check:
			
			// The 50% of non-matching data objects are emitted
			Mockito.verify(enrich_context, Mockito.times(num_write_records)).emitImmutableObject(Mockito.any(Long.class), Mockito.any(JsonNode.class), Mockito.any(Optional.class), Mockito.any(Optional.class), Mockito.any(Optional.class));
			// Of the other 50%, the 50% newer ones get passed to the batch (*2, 1 for new + 1 for old) - but no dups because they are always timestamp==0
			assertEquals(num_write_records, _called_batch.get());
		}
		// TEST 5c: CUSTOM UPDATE, SINGLE-FIELD, MULTI-BUCKET CONTEXT
		{
			_called_batch.set(0);
			
			final IEnrichmentModuleContext enrich_context = getMockEnrichmentContext();
			
			final DataBucketBean write_bucket = addTimestampField(ts_field, getDocBucket("/test/dedup/write/c",
					BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_policy, DeduplicationPolicy.custom_update)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_contexts, Arrays.asList("/dedup/*"))
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_fields, Arrays.asList("alt_dup_field"))
						.with(DocumentSchemaBean::custom_deduplication_configs,
								Arrays.asList(
										BeanTemplateUtils.build(EnrichmentControlMetadataBean.class)
											.with(EnrichmentControlMetadataBean::name, "custom_test")
											.with(EnrichmentControlMetadataBean::module_name_or_id, "/app/aleph2/library/test.jar")
										.done().get()										
										)
								)
					.done().get()
					));
			
			// Test
			
			test_puttingItAllTogether_runTest(write_bucket, enrich_context);			
			
			// Things to check:
			
			// The 50% of non-matching data objects are emitted
			Mockito.verify(enrich_context, Mockito.times(num_write_records)).emitImmutableObject(Mockito.any(Long.class), Mockito.any(JsonNode.class), Mockito.any(Optional.class), Mockito.any(Optional.class), Mockito.any(Optional.class));
			// Of the other 50%, the 50% newer ones get passed to the batch (*2, 1 for new + 1 for old) - but no dups because they are always timestamp==0
			assertEquals(num_write_records, _called_batch.get());
		}		
		// TEST 5d: CUSTOM UPDATE, MULTI-FIELD, SINGLE-BUCKET CONTEXT
		{
			_called_batch.set(0);
			
			final IEnrichmentModuleContext enrich_context = getMockEnrichmentContext();
			
			final DataBucketBean write_bucket = addTimestampField(ts_field, getDocBucket("/dedup/context1",
					BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_policy, DeduplicationPolicy.custom_update)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_fields, Arrays.asList("alt_dup_field"))
						.with(DocumentSchemaBean::custom_deduplication_configs,
								Arrays.asList(
										BeanTemplateUtils.build(EnrichmentControlMetadataBean.class)
											.with(EnrichmentControlMetadataBean::name, "custom_test")
											.with(EnrichmentControlMetadataBean::module_name_or_id, "/app/aleph2/library/test.jar")
										.done().get()										
										)
								)
					.done().get()
					));
			
			// Test
			
			test_puttingItAllTogether_runTest(write_bucket, enrich_context);			
			
			// Things to check:
			
			// The 50% of non-matching data objects are emitted
			Mockito.verify(enrich_context, Mockito.times(num_write_records)).emitImmutableObject(Mockito.any(Long.class), Mockito.any(JsonNode.class), Mockito.any(Optional.class), Mockito.any(Optional.class), Mockito.any(Optional.class));
			// Of the other 50%, the 50% newer ones get passed to the batch (*2, 1 for new + 1 for old) - but no dups because they are always timestamp==0
			assertEquals(num_write_records, _called_batch.get());
		}
		// TEST 5e: CUSTOM UPDATE, MULTI-FIELD, SINGLE-BUCKET CONTEXT
		{
			_called_batch.set(0);
			
			final IEnrichmentModuleContext enrich_context = getMockEnrichmentContext();
			
			final DataBucketBean write_bucket = addTimestampField(ts_field, getDocBucket("/dedup/context1",
					BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_policy, DeduplicationPolicy.custom_update)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_fields, Arrays.asList("alt_dup_field"))
						.with(DataSchemaBean.DocumentSchemaBean::custom_finalize_all_objects, true)
						.with(DocumentSchemaBean::custom_deduplication_configs,
								Arrays.asList(
										BeanTemplateUtils.build(EnrichmentControlMetadataBean.class)
											.with(EnrichmentControlMetadataBean::name, "custom_test")
											.with(EnrichmentControlMetadataBean::module_name_or_id, "/app/aleph2/library/test.jar")
										.done().get()										
										)
								)
					.done().get()
					));
			
			// Test
			
			test_puttingItAllTogether_runTest(write_bucket, enrich_context);			
			
			// Things to check:
			
			// Nothing is emitted
			Mockito.verify(enrich_context, Mockito.times(0)).emitImmutableObject(Mockito.any(Long.class), Mockito.any(JsonNode.class), Mockito.any(Optional.class), Mockito.any(Optional.class), Mockito.any(Optional.class));
			// 50% + will go through the deduplication (context1, but _not_ dups, see above) - the other 50% (context2) + 10% dups will go through finalize 
			assertEquals(num_write_records + 11*num_write_records/10, _called_batch.get());
		}
		
		// Final error case:
		
		// TEST Xa: LEAVE, MULTI-FIELD, MULTI-BUCKET CONTEXT
		{
			final IEnrichmentModuleContext enrich_context = getMockEnrichmentContext();
			
			final DataBucketBean write_bucket = addTimestampField(ts_field, getDocBucket("/test/dedup/write/a",
					BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_policy, DeduplicationPolicy.leave)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_contexts, Arrays.asList("/dedup/*"))
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_fields, Arrays.asList("missing_dup_field", "missing_dup"))
					.done().get()
					));
			
			// Test
			
			test_puttingItAllTogether_runTest(write_bucket, enrich_context);			
			
			// Things to check:
			
			// Should not emit anything
			Mockito.verify(enrich_context, Mockito.times(0)).emitImmutableObject(Mockito.any(Long.class), Mockito.any(JsonNode.class), Mockito.any(Optional.class), Mockito.any(Optional.class), Mockito.any(Optional.class));
		}		
		// TEST Xc: LEAVE, SINGLE-FIELD, MULTI-BUCKET CONTEXT
		{
			final IEnrichmentModuleContext enrich_context = getMockEnrichmentContext();
			
			final DataBucketBean write_bucket = addTimestampField(ts_field, getDocBucket("/test/dedup/write/c",
					BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_policy, DeduplicationPolicy.leave)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_contexts, Arrays.asList("/dedup/*"))
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_fields, Arrays.asList("missing_alt_dup_field"))
					.done().get()
					));
			
			// Test
			
			test_puttingItAllTogether_runTest(write_bucket, enrich_context);			
			
			// Things to check:
			
			// Should not emit anything
			Mockito.verify(enrich_context, Mockito.times(0)).emitImmutableObject(Mockito.any(Long.class), Mockito.any(JsonNode.class), Mockito.any(Optional.class), Mockito.any(Optional.class), Mockito.any(Optional.class));
		}		
		
		System.out.println("COMPLETING DEDUP TEST NOW: " + new Date());
	}
	
	/** 2 Things
	 *  1) Check that the 
	 * @throws InterruptedException 
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void test_puttingItAllTogether_deletionTests() throws InterruptedException {
		
		test_puttingItAllTogether_genericPhase();
		
		// Generic intialization:
		
		final String ts_field = "@timestamp";
		
		System.out.println("STARTING COMPLEX DEDUP TEST NOW: " + new Date());

		// Test 1) All the duplicates get through the overwrite dedup module - will also delete lots of the dups
		{			
			final IEnrichmentModuleContext enrich_context = getMockEnrichmentContext();
			
			final DataBucketBean write_bucket = addTimestampField(ts_field, getDocBucket("/test/dedup/multiple/overwrite/delete",
					BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_policy, DeduplicationPolicy.overwrite)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_contexts, Arrays.asList("/dedup/*"))
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_fields, Arrays.asList("dup_field"))
						.with(DataSchemaBean.DocumentSchemaBean::delete_unhandled_duplicates, true)
					.done().get()
					));
			
			// Test
			
			test_puttingItAllTogether_runTest(write_bucket, enrich_context);			
			
			// Things to check:
			
			// Should have called emit 2*"num_write_records" times (50% of them are duplicates but they get overwritten)
			Mockito.verify(enrich_context, Mockito.times(2*num_write_records)).emitImmutableObject(Mockito.any(Long.class), Mockito.any(JsonNode.class), Mockito.any(Optional.class), Mockito.any(Optional.class), Mockito.any(Optional.class));
	
			// Check deleted all the extra duplicates
			final IDataWriteService<JsonNode> store1 = getDataStore("/dedup/context1");
			final IDataWriteService<JsonNode> store2 = getDataStore("/dedup/context2");
			
			for (int i = 0; i < 40; ++i) {
				Thread.sleep(250L);
				if ((store1.countObjects().join() +
						store2.countObjects().join()) <= num_write_records) {
					break;
				}
			}
			assertEquals(num_write_records, store1.countObjects().join() + store2.countObjects().join());			
		}
		
		System.out.println("(replenishing dedup contexts)");
		test_puttingItAllTogether_genericPhase();
		{
			_called_batch.set(0);
			
			final IEnrichmentModuleContext enrich_context = getMockEnrichmentContext();
			
			final DataBucketBean write_bucket = addTimestampField(ts_field, getDocBucket("/test/dedup/multiple/overwrite/custom",
					BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_policy, DeduplicationPolicy.custom)
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_fields, Arrays.asList("dup_field"))
						.with(DataSchemaBean.DocumentSchemaBean::deduplication_contexts, Arrays.asList("/dedup/*"))
						.with(DataSchemaBean.DocumentSchemaBean::delete_unhandled_duplicates, true)
						.with(DocumentSchemaBean::custom_deduplication_configs,
								Arrays.asList(
										BeanTemplateUtils.build(EnrichmentControlMetadataBean.class)
											.with(EnrichmentControlMetadataBean::name, "custom_test")
											.with(EnrichmentControlMetadataBean::module_name_or_id, "/app/aleph2/library/test.jar")
										.done().get()										
										)
								)
					.done().get()
					));
			
			// Test
			
			test_puttingItAllTogether_runTest(write_bucket, enrich_context);			
			
			// Things to check:
			
			// The 50% of non-matching data objects are emitted
			Mockito.verify(enrich_context, Mockito.times(num_write_records)).emitImmutableObject(Mockito.any(Long.class), Mockito.any(JsonNode.class), Mockito.any(Optional.class), Mockito.any(Optional.class), Mockito.any(Optional.class));
			// The other 50% get passed to the batch (*3, 1 for new + 2 for old) + 10% dups, ie *2 + *11/10 total
			assertEquals(2*num_write_records + 11*num_write_records/10, _called_batch.get());

			// Check deleted all the extra duplicates
			final IDataWriteService<JsonNode> store1 = getDataStore("/dedup/context1");
			final IDataWriteService<JsonNode> store2 = getDataStore("/dedup/context2");
			
			for (int i = 0; i < 40; ++i) {
				Thread.sleep(250L);
				if ((store1.countObjects().join() + store2.countObjects().join()) <= 0) {
					break;
				}
			}
			assertEquals(0, store1.countObjects().join() + store2.countObjects().join());
		}
	}
	
	private IDataWriteService<JsonNode> getDataStore(final String bucket_name) {
		final DataBucketBean context_bucket =
				BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::full_name, bucket_name)
				.with(DataBucketBean::data_schema,
						BeanTemplateUtils.build(DataSchemaBean.class)
							.with(DataSchemaBean::document_schema, 
									BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
									.done().get()
							)
						.done().get()
						)
			.done().get();
		
		final IDocumentService doc_service = _service_context.getDocumentService().get();
		return doc_service.getDataService().get().getWritableDataService(JsonNode.class, context_bucket, Optional.empty(), Optional.empty()).get();
	}
	
	public void test_puttingItAllTogether_genericPhase() throws InterruptedException {
		
		// 1) Create 2 "context" buckets
		
		final DataBucketBean context_bucket1 = 
				BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::full_name, "/dedup/context1")
					.with(DataBucketBean::data_schema,
							BeanTemplateUtils.build(DataSchemaBean.class)
								.with(DataSchemaBean::document_schema, 
										BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
										.done().get()
								)
							.done().get()
							)
				.done().get();
									
		final DataBucketBean context_bucket2 =
				BeanTemplateUtils.clone(context_bucket1)
					.with(DataBucketBean::full_name, "/dedup/context2")
				.done();
		
		_service_context.getCoreManagementDbService().getDataBucketStore().deleteDatastore().join();
		_service_context.getCoreManagementDbService().getDataBucketStore().storeObject(context_bucket1, true).join();
		_service_context.getCoreManagementDbService().getDataBucketStore().storeObject(context_bucket2, true).join();
		assertEquals(2, _service_context.getCoreManagementDbService().getDataBucketStore().countObjects().join().intValue());
		
		IDocumentService doc_service = _service_context.getDocumentService().get();
		
		IDataWriteService<JsonNode> write_context1 = doc_service.getDataService().get().getWritableDataService(JsonNode.class, context_bucket1, Optional.empty(), Optional.empty()).get();
		IDataWriteService<JsonNode> write_context2 = doc_service.getDataService().get().getWritableDataService(JsonNode.class, context_bucket2, Optional.empty(), Optional.empty()).get();
		
		write_context1.deleteDatastore();
		write_context2.deleteDatastore();
		for (int i = 0; i < 40; ++i) {
			Thread.sleep(250L);
			if ((write_context1.countObjects().join() + write_context2.countObjects().join()) <= 0)
			{
				break;
			}
			//System.out.println("?? " + store1.countObjects().join() + "..." + store2.countObjects().join());
		}
		assertEquals(0, write_context1.countObjects().join() + write_context2.countObjects().join());			
		
		// 2) Fill with 50% duplicates, 50% random records
		
		List<JsonNode> objs_for_context1 = IntStream.rangeClosed(1, num_write_records).boxed().map(i -> {
			final ObjectNode obj = _mapper.createObjectNode();
			obj.put("_id", "id1_" + i);
			obj.put("dup", true);
			obj.put("dup_field", i);
			obj.put("alt_dup_field", i);
			obj.put("@timestamp", 0L);
			return (JsonNode) obj;
		}).collect(Collectors.toList());
		
		List<JsonNode> objs_for_context2 = IntStream.rangeClosed(1, num_write_records).boxed().map(i -> {
			final ObjectNode obj = _mapper.createObjectNode();
			obj.put("_id", "id2_" + i);
			obj.put("dup", false);
			obj.put("dup_field", i);
			obj.put("alt_dup_field", -i);
			obj.put("@timestamp", 0L);
			return (JsonNode) obj;
		}).collect(Collectors.toList());
		
		write_context1.storeObjects(objs_for_context1).join();
		write_context2.storeObjects(objs_for_context2).join();
		
		// OK wait for these writes to be complete
		
		for (;;) {
			Thread.sleep(250L);
			if ((write_context1.countObjects().join() >= num_write_records)
					&&
					(write_context2.countObjects().join() >= num_write_records))
			{
				break;
			}
		}
		assertEquals(500, write_context1.countObjects().join().intValue());
		assertEquals(500, write_context2.countObjects().join().intValue());
		
		// OK now need to create a shared library bean and insert it
		
		final SharedLibraryBean bean = 
				BeanTemplateUtils.build(SharedLibraryBean.class)
					.with(SharedLibraryBean::path_name, "/app/aleph2/library/test.jar")
					.with(SharedLibraryBean::batch_enrichment_entry_point, TestDedupEnrichmentModule.class.getName())
				.done().get();
		
		_service_context.getService(IManagementDbService.class, Optional.empty()).get()
			.getSharedLibraryStore().storeObject(bean, true)
			.join()
			;					
	}
	
	public void test_puttingItAllTogether_runTest(final DataBucketBean write_bucket, final IEnrichmentModuleContext enrich_context) {
		// OK now create a new batch of objects
		
		List<Tuple2<Long, IBatchRecord>> imcoming_objects = IntStream.rangeClosed(1, 2*num_write_records).boxed().map(i -> {
			final ObjectNode obj = _mapper.createObjectNode();
			obj.put("_id", "id" + i);
			obj.put("dup", true);
			obj.put("dup_field", i);
			obj.put("alt_dup_field", i);
			obj.put("@timestamp", (0 == (i % 2)) ? 0L : 1L); // (ie alternate new with old - in some cases the old won't update)
			return (0 == (i % 10))
					? Stream.of((JsonNode) obj, (JsonNode) obj) // (every 10th emit a duplicate)
					: Stream.of((JsonNode) obj);
		})		
		.flatMap(s -> s)
		.map(j -> Tuples._2T(0L, (IBatchRecord)new BatchRecordUtils.JsonBatchRecord(j)))
		.collect(Collectors.toList());
		
		// Other things we need:
		
		IEnrichmentBatchModule test_module = new DeduplicationService();
		
		final EnrichmentControlMetadataBean control = BeanTemplateUtils.build(EnrichmentControlMetadataBean.class)
					.with(EnrichmentControlMetadataBean::name, "custom_test")
				.done().get();		
		
		// Initialize
		
		test_module.onStageInitialize(enrich_context, write_bucket, control, Tuples._2T(ProcessingStage.input, ProcessingStage.output), Optional.empty());
		
		// Run
		
		test_module.onObjectBatch(imcoming_objects.stream(), Optional.empty(), Optional.empty());
		
		// (Finish)
		test_module.onStageComplete(true);
	}
	
	////////////////////////////////////////////////////
	
	@SuppressWarnings("unchecked")
	public IEnrichmentModuleContext getMockEnrichmentContext() {
		
		final IEnrichmentModuleContext enrich_context = Mockito.mock(IEnrichmentModuleContext.class);
		final IBucketLogger mock_logger = Mockito.mock(IBucketLogger.class);
		Mockito.when(enrich_context.getLogger(Mockito.any())).thenReturn(mock_logger);			
		
		Mockito.when(enrich_context.getServiceContext()).thenReturn(_service_context);
		
		Mockito.when(enrich_context.emitImmutableObject(Mockito.any(Long.class), Mockito.any(JsonNode.class), Mockito.any(Optional.class), Mockito.any(Optional.class), Mockito.any(Optional.class)))
					.thenReturn(Validation.success(_mapper.createObjectNode()));
		
		return enrich_context;
	}
	
	public static DataBucketBean getDocBucket(final String name, final DataSchemaBean.DocumentSchemaBean doc_schema) {
		return BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::full_name, name)
				.with(DataBucketBean::data_schema,
						BeanTemplateUtils.build(DataSchemaBean.class)
							.with(DataSchemaBean::document_schema, doc_schema)
						.done().get()
						)
				.done().get();
	}
	
	public static DataBucketBean addTimestampField(String field_name, DataBucketBean to_clone) {
		return BeanTemplateUtils.clone(to_clone)
					.with(DataBucketBean::data_schema,
							BeanTemplateUtils.clone(to_clone.data_schema())
								.with(DataSchemaBean::temporal_schema, 
										BeanTemplateUtils.build(DataSchemaBean.TemporalSchemaBean.class)
											.with(DataSchemaBean.TemporalSchemaBean::time_field, field_name)
										.done().get()
									)
							.done()
							)
				.done();
	}
	
	////////////////////////////////////////////////////
	
	public static AtomicInteger _called_batch = new AtomicInteger(0);
	
	public static class TestDedupEnrichmentModule implements IEnrichmentBatchModule {

		@Override
		public void onStageInitialize(IEnrichmentModuleContext context,
				DataBucketBean bucket, EnrichmentControlMetadataBean control,
				Tuple2<ProcessingStage, ProcessingStage> previous_next,
				Optional<List<String>> next_grouping_fields) {			
			assertEquals("custom_test", control.name());			
		}

		@Override
		public void onObjectBatch(Stream<Tuple2<Long, IBatchRecord>> batch,
				Optional<Integer> batch_size, Optional<JsonNode> grouping_key) {
			
			List<Tuple2<Long, IBatchRecord>> l = batch.collect(Collectors.toList());
			long n = l.stream().filter(b -> !b._2().injected()).count();
			batch_size.ifPresent(bs -> assertEquals(n, bs.longValue())); //(in finalize cases this is not filled in)
			_called_batch.addAndGet(l.size());
		}

		@Override
		public void onStageComplete(boolean is_original) {
		}
		
	}
	
	
}
