/*******************************************************************************
 * Copyright 2016, The IKANOW Open Source Project.
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

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;



















import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;



















import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ikanow.aleph2.data_model.interfaces.data_services.IColumnarService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IDataWarehouseService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IDocumentService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IGraphService;
import com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.data_services.ITemporalService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService.IBatchSubservice;
import com.ikanow.aleph2.data_model.interfaces.shared_services.MockServiceContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider.IGenericDataService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.DocumentSchemaBean.DeduplicationPolicy;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;

import fj.Unit;

/**
 * @author Alex
 *
 */
public class TestMultiDataService {
	protected final static ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());

	protected int _apply_count = 0;
	protected Map<String, Integer> _crud_responses = new HashMap<>();
	protected Map<String, Integer> _batch_responses = new HashMap<>();		
	
	@Test
	public void test_MultiDataService_allDifferent() {
	
		final DataBucketBean bucket =
				BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::data_schema,
							BeanTemplateUtils.build(DataSchemaBean.class)
								.with(DataSchemaBean::search_index_schema, 
										BeanTemplateUtils.build(DataSchemaBean.SearchIndexSchemaBean.class)
										.done().get())
								.with(DataSchemaBean::document_schema, 
										BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
											.with(DataSchemaBean.DocumentSchemaBean::service_name, "test")
										.done().get())
								.with(DataSchemaBean::temporal_schema, 
										BeanTemplateUtils.build(DataSchemaBean.TemporalSchemaBean.class)
											.with(DataSchemaBean.TemporalSchemaBean::service_name, "test")
										.done().get())
								.with(DataSchemaBean::columnar_schema, 
										BeanTemplateUtils.build(DataSchemaBean.ColumnarSchemaBean.class)
											.with(DataSchemaBean.ColumnarSchemaBean::service_name, "test")
										.done().get())
								.with(DataSchemaBean::data_warehouse_schema, 
										BeanTemplateUtils.build(DataSchemaBean.DataWarehouseSchemaBean.class)
											.with(DataSchemaBean.DataWarehouseSchemaBean::service_name, "test")
										.done().get())
								.with(DataSchemaBean::graph_schema, 
										BeanTemplateUtils.build(DataSchemaBean.GraphSchemaBean.class)
											.with(DataSchemaBean.GraphSchemaBean::service_name, "test")
										.done().get())
								.with(DataSchemaBean::storage_schema, 
										BeanTemplateUtils.build(DataSchemaBean.StorageSchemaBean.class)
										.done().get())
							.done().get()
							)
				.done().get();
				
		// All services present, but configured to not write
		{
			final MockServiceContext mock_service_context = getPopulatedServiceContext(0);
			
			final MultiDataService mds = new MultiDataService(bucket, mock_service_context, Optional.empty(), Optional.empty());
			
			assertEquals("All services", 7, mds.getDataServices().size());
			assertEquals("No batches", 0, mds.getBatchWriters().size());
			assertEquals("No cruds", 0, mds.getCrudOnlyWriters().size());
			assertEquals("No cruds", 0, mds.getCrudWriters().size());
			assertEquals(false, mds._doc_write_mode);
			
			assertEquals("No data services", false, mds.batchWrite(_mapper.createObjectNode()));
			assertTrue("No batch responses: " + _batch_responses.keySet(), _batch_responses.isEmpty());
			assertTrue("No CRUD responses: " + _crud_responses.keySet(), _crud_responses.isEmpty());
		}
		// All services present, use slow CRUD service
		{
			final MockServiceContext mock_service_context = getPopulatedServiceContext(1);
			
			final MultiDataService mds = new MultiDataService(bucket, mock_service_context, Optional.empty(), Optional.empty());
			
			assertEquals("All services", 7, mds.getDataServices().size());
			assertEquals("No batches", 0, mds.getBatchWriters().size());
			assertEquals("All cruds", 7, mds.getCrudOnlyWriters().size());
			assertEquals("All cruds", 7, mds.getCrudWriters().size());
			assertEquals(false, mds._doc_write_mode);
			assertEquals(0, _apply_count);
			
			assertEquals("Found data services", true, mds.batchWrite(_mapper.createObjectNode()));
			
			assertTrue("No batch responses: " + _batch_responses.keySet(), _batch_responses.isEmpty());
			assertEquals("CRUD responses: " + _crud_responses.keySet(), 7, _crud_responses.size());
			_crud_responses.clear();
			
			//check that the the flush batch output returns with no errors
			assertTrue(mds.flushBatchOutput().isDone());
		}
		// All services present, use fast CRUD service
		{
			final MockServiceContext mock_service_context = getPopulatedServiceContext(2);
			
			final MultiDataService mds = new MultiDataService(bucket, mock_service_context, Optional.empty(), Optional.of(__ -> { _apply_count++; return Optional.empty(); }));
			assertEquals(false, mds._doc_write_mode);
			
			assertEquals("All services", 7, mds.getDataServices().size());
			assertEquals("All batches", 7, mds.getBatchWriters().size());
			assertEquals("No cruds", 0, mds.getCrudOnlyWriters().size());
			assertEquals("No cruds", 7, mds.getCrudWriters().size());
			assertEquals(7, _apply_count);
			
			assertEquals("Found data services", true, mds.batchWrite(_mapper.createObjectNode()));
			
			assertTrue("No CRUD responses: " + _crud_responses.keySet(), _crud_responses.isEmpty());
			assertEquals("batch responses: " + _batch_responses.keySet(), 7, _batch_responses.size());			
			_batch_responses.clear();
			
			//check that the the flush batch output returns with no errors
			assertTrue(mds.flushBatchOutput().isDone());
			assertEquals("batch responses: " + _batch_responses.keySet(), 7, _batch_responses.size());			
			_batch_responses.clear();
		}
	}
	
	
	@Test
	public void test_MultiDataService_commonServices() {	

		final DataBucketBean bucket =
				BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::data_schema,
							BeanTemplateUtils.build(DataSchemaBean.class)
								.with(DataSchemaBean::search_index_schema, 
										BeanTemplateUtils.build(DataSchemaBean.SearchIndexSchemaBean.class)
										.done().get())
								.with(DataSchemaBean::document_schema, 
										BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
											.with(DataSchemaBean.DocumentSchemaBean::deduplication_policy, DeduplicationPolicy.update)
										.done().get())
								.with(DataSchemaBean::temporal_schema, 
										BeanTemplateUtils.build(DataSchemaBean.TemporalSchemaBean.class)
										.done().get())
								.with(DataSchemaBean::columnar_schema, 
										BeanTemplateUtils.build(DataSchemaBean.ColumnarSchemaBean.class)
										.done().get())
								.with(DataSchemaBean::data_warehouse_schema, 
										BeanTemplateUtils.build(DataSchemaBean.DataWarehouseSchemaBean.class)
										.done().get())
								.with(DataSchemaBean::storage_schema, 
										BeanTemplateUtils.build(DataSchemaBean.StorageSchemaBean.class)
										.done().get())
							.done().get()
							)
				.done().get();
				
		// All services present, but configured to not write
		{
			final MockServiceContext mock_service_context = getPopulatedServiceContext(0);
			
			final MultiDataService mds = MultiDataService.getMultiWriter(bucket, mock_service_context);
			
			assertEquals("All services: " + mds.getDataServices(), 3, mds.getDataServices().size());
			assertEquals("No batches", 0, mds.getBatchWriters().size());
			assertEquals("No cruds", 0, mds.getCrudOnlyWriters().size());
			assertEquals("No cruds", 0, mds.getCrudWriters().size());
			assertEquals(true, mds._doc_write_mode);
			
			assertEquals("No data services", false, mds.batchWrite(_mapper.createObjectNode()));
			assertTrue("No batch responses: " + _batch_responses.keySet(), _batch_responses.isEmpty());
			assertTrue("No CRUD responses: " + _crud_responses.keySet(), _crud_responses.isEmpty());
		}
		// All services present, use slow CRUD service
		{
			final MockServiceContext mock_service_context = getPopulatedServiceContext(1);
			
			final MultiDataService mds = MultiDataService.getMultiWriter(bucket, mock_service_context, Optional.empty(), Optional.empty());
			
			assertEquals("All services", 3, mds.getDataServices().size());
			assertEquals("All services", 6, mds._services.keys().size());
			assertEquals("No batches", 0, mds.getBatchWriters().size());
			assertEquals("All cruds", 3, mds.getCrudOnlyWriters().size());
			assertEquals("All cruds", 3, mds.getCrudWriters().size());
			assertEquals(true, mds._doc_write_mode);
			assertEquals(0, _apply_count);
			
			assertEquals("Found data services", true, mds.batchWrite(_mapper.createObjectNode()));
			
			assertTrue("No batch responses: " + _batch_responses.keySet(), _batch_responses.isEmpty());
			assertEquals("CRUD responses: " + _crud_responses.keySet(), 3, _crud_responses.size());
			_crud_responses.clear();
		}
		// All services present, use fast CRUD service
		{
			final MockServiceContext mock_service_context = getPopulatedServiceContext(2);
			
			final MultiDataService mds = new MultiDataService(bucket, mock_service_context, Optional.empty(), Optional.of(__ -> { _apply_count++; return Optional.empty(); }));
			assertEquals(true, mds._doc_write_mode);
			
			assertEquals("All services", 3, mds.getDataServices().size());
			assertEquals("All services", 6, mds._services.keys().size());
			assertEquals("No batches", 3, mds.getBatchWriters().size());
			assertEquals("No cruds", 0, mds.getCrudOnlyWriters().size());
			assertEquals("No cruds", 3, mds.getCrudWriters().size());
			assertEquals(3, _apply_count);
			
			assertEquals("Found data services", true, mds.batchWrite(_mapper.createObjectNode()));
			
			assertTrue("No CRUD responses: " + _crud_responses.keySet(), _crud_responses.isEmpty());
			assertEquals("batch responses: " + _batch_responses.keySet(), 3, _batch_responses.size());			
			_batch_responses.clear();
			
			//check that the the flush batch output returns with no errors
			assertTrue(mds.flushBatchOutput().isDone());
			assertEquals("batch responses: " + _batch_responses.keySet(), 3, _batch_responses.size());			
			_batch_responses.clear();
		}
	}
	
	@Test
	public void test_transientWriteMode() {
		
		final DataBucketBean bucket =
				BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::data_schema,
							BeanTemplateUtils.build(DataSchemaBean.class)
								.with(DataSchemaBean::search_index_schema, 
										BeanTemplateUtils.build(DataSchemaBean.SearchIndexSchemaBean.class)
										.done().get())
								.with(DataSchemaBean::document_schema, 
										BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
											.with(DataSchemaBean.DocumentSchemaBean::deduplication_policy, DeduplicationPolicy.update)
										.done().get())
								.with(DataSchemaBean::temporal_schema, 
										BeanTemplateUtils.build(DataSchemaBean.TemporalSchemaBean.class)
										.done().get())
								.with(DataSchemaBean::columnar_schema, 
										BeanTemplateUtils.build(DataSchemaBean.ColumnarSchemaBean.class)
										.done().get())
								.with(DataSchemaBean::data_warehouse_schema, 
										BeanTemplateUtils.build(DataSchemaBean.DataWarehouseSchemaBean.class)
										.done().get())
								.with(DataSchemaBean::storage_schema, 
										BeanTemplateUtils.build(DataSchemaBean.StorageSchemaBean.class)
										.done().get())
							.done().get()
							)
				.done().get();
				
		// All services present, but configured to not write
		{
			final MockServiceContext mock_service_context = getPopulatedServiceContext(2);
			
			final MultiDataService mds = MultiDataService.getTransientMultiWriter(bucket, mock_service_context, "test:alex", Optional.empty());
			
			assertEquals("All services", 1, mds.getDataServices().size());
			assertEquals("No batches", 1, mds.getBatchWriters().size());
			assertEquals("No cruds", 0, mds.getCrudOnlyWriters().size());
			assertEquals("No cruds", 1, mds.getCrudWriters().size());
			assertEquals(false, mds._doc_write_mode);
			
			assertEquals("Wrote", true, mds.batchWrite(_mapper.createObjectNode()));
			assertEquals("No batch responses: " + _batch_responses.keySet(), 1, _batch_responses.size());
			assertTrue("No CRUD responses: " + _crud_responses.keySet(), _crud_responses.isEmpty());
		}
	}
	
	@Test
	public void test_getWriteMode() {
		
		final DataBucketBean bucket0 =
				BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::data_schema,
							BeanTemplateUtils.build(DataSchemaBean.class)
								.with(DataSchemaBean::document_schema, 
										BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
											.with(DataSchemaBean.DocumentSchemaBean::enabled, false)
											.with(DataSchemaBean.DocumentSchemaBean::deduplication_policy, DeduplicationPolicy.update)
										.done().get())
							.done().get()
							)
				.done().get();
		
		
		final DataBucketBean bucket1 =
				BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::data_schema,
							BeanTemplateUtils.build(DataSchemaBean.class)
								.with(DataSchemaBean::document_schema, 
										BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
											.with(DataSchemaBean.DocumentSchemaBean::enabled, true)
											.with(DataSchemaBean.DocumentSchemaBean::deduplication_policy, DeduplicationPolicy.update)
										.done().get())
							.done().get()
							)
				.done().get();
		
		final DataBucketBean bucket2 =
				BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::data_schema,
							BeanTemplateUtils.build(DataSchemaBean.class)
								.with(DataSchemaBean::document_schema, 
										BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
											.with(DataSchemaBean.DocumentSchemaBean::enabled, true)
											.with(DataSchemaBean.DocumentSchemaBean::deduplication_fields, java.util.Arrays.asList("x"))
										.done().get())
							.done().get()
							)
				.done().get();
		
		final DataBucketBean bucket3 =
				BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::data_schema,
							BeanTemplateUtils.build(DataSchemaBean.class)
								.with(DataSchemaBean::document_schema, 
										BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
											.with(DataSchemaBean.DocumentSchemaBean::deduplication_contexts, java.util.Arrays.asList("y"))
										.done().get())
							.done().get()
							)
				.done().get();
	
		assertEquals(false, MultiDataService.getWriteMode(bucket0));
		assertEquals(true, MultiDataService.getWriteMode(bucket1));
		assertEquals(true, MultiDataService.getWriteMode(bucket2));
		assertEquals(true, MultiDataService.getWriteMode(bucket3));
	}
	
	
	/** Found this more comprehensive test after writing the one above so keeping them both
	 */
	@Test
	public void test_docWriteMode_copiedFromContext() {
		{
			
			final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::_id, "test")
					.with(DataBucketBean::full_name, "/test/basicContextCreation")
					.with(DataBucketBean::modified, new Date())
					.with("data_schema", BeanTemplateUtils.build(DataSchemaBean.class)
							.with("document_schema", BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
									.with(DataSchemaBean.DocumentSchemaBean::enabled, false)
									.done().get())
							.done().get())
					.done().get();
			
			assertEquals(false, MultiDataService.getWriteMode(test_bucket));
		}
		{
			final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::_id, "test")
					.with(DataBucketBean::full_name, "/test/basicContextCreation")
					.with(DataBucketBean::modified, new Date())
					.with("data_schema", BeanTemplateUtils.build(DataSchemaBean.class)
							.with("document_schema", BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
									.with(DataSchemaBean.DocumentSchemaBean::enabled, true)
									.done().get())
							.done().get())
					.done().get();
			
			assertEquals(false, MultiDataService.getWriteMode(test_bucket));
		}
		{
			final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::_id, "test")
					.with(DataBucketBean::full_name, "/test/basicContextCreation")
					.with(DataBucketBean::modified, new Date())
					.with("data_schema", BeanTemplateUtils.build(DataSchemaBean.class)
							.with("document_schema", BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
									.with(DataSchemaBean.DocumentSchemaBean::deduplication_policy, DeduplicationPolicy.leave)
									.done().get())
							.done().get())
					.done().get();
			
			assertEquals(true, MultiDataService.getWriteMode(test_bucket));
		}
		{
			final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::_id, "test")
					.with(DataBucketBean::full_name, "/test/basicContextCreation")
					.with(DataBucketBean::modified, new Date())
					.with("data_schema", BeanTemplateUtils.build(DataSchemaBean.class)
							.with("document_schema", BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
									.with(DataSchemaBean.DocumentSchemaBean::deduplication_fields, Arrays.asList("test"))
									.done().get())
							.done().get())
					.done().get();
			
			assertEquals(true, MultiDataService.getWriteMode(test_bucket));
		}
		{
			final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::_id, "test")
					.with(DataBucketBean::full_name, "/test/basicContextCreation")
					.with(DataBucketBean::modified, new Date())
					.with("data_schema", BeanTemplateUtils.build(DataSchemaBean.class)
							.with("document_schema", BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
									.with(DataSchemaBean.DocumentSchemaBean::deduplication_contexts, Arrays.asList("test"))
									.done().get())
							.done().get())
					.done().get();
			
			assertEquals(true, MultiDataService.getWriteMode(test_bucket));
		}
		{
			final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::_id, "test")
					.with(DataBucketBean::full_name, "/test/basicContextCreation")
					.with(DataBucketBean::modified, new Date())
					.with("data_schema", BeanTemplateUtils.build(DataSchemaBean.class)
							.with("document_schema", BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
									.with(DataSchemaBean.DocumentSchemaBean::deduplication_policy, DeduplicationPolicy.leave)
									.with(DataSchemaBean.DocumentSchemaBean::deduplication_fields, Arrays.asList("test"))
									.done().get())
							.done().get())
					.done().get();
			
			assertEquals(true, MultiDataService.getWriteMode(test_bucket));
		}
	}
	
	
	/**
	 * @param mode: 0 => no data service provider (or no writable), 1 => no batch provider
	 * @return
	 */
	public MockServiceContext getPopulatedServiceContext(int mode) {
		MockServiceContext mock_service_context = new MockServiceContext();
		
		final SearchIndexAndMoreService search_index = Mockito.mock(SearchIndexAndMoreService.class);
		final IDocumentService document = Mockito.mock(IDocumentService.class);
		final IColumnarService columnar = Mockito.mock(IColumnarService.class);
		final ITemporalService temporal = Mockito.mock(ITemporalService.class);
		final IDataWarehouseService data_warehouse = Mockito.mock(IDataWarehouseService.class);
		final IGraphService graph = Mockito.mock(IGraphService.class);
		final IStorageService storage = Mockito.mock(IStorageService.class);
		
		Stream.<IDataServiceProvider>of(search_index, document, columnar, temporal, data_warehouse, graph, storage)
			.forEach(ds -> {
				if (0 == mode) {
					boolean tiebreak = 0 == (ds.getClass().toString().hashCode() % 2); //(get some code coverage)
					if (tiebreak) {
						Mockito.when(ds.getDataService()).thenReturn(Optional.empty());
					}
					else {
						final IGenericDataService gds = Mockito.mock(IGenericDataService.class);
						Mockito.when(gds.getWritableDataService(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(Optional.empty());
						Mockito.when(ds.getDataService()).thenReturn(Optional.of(gds));
					}
				}
				else if (1 == mode) { // use slow write mode
					
					@SuppressWarnings("unchecked")
					final IDataWriteService<JsonNode> dws = (IDataWriteService<JsonNode>)Mockito.mock(IDataWriteService.class);
					Mockito.when(dws.getBatchWriteSubservice()).thenReturn(Optional.empty());
					final Answer<Object> callback = new Answer<Object>() {
				        public Object answer(InvocationOnMock invocation) {
				        	_crud_responses.compute(ds.getClass().toString(), (k, v) -> (null == v) ? 1 : v + 1);
				            return (Object) CompletableFuture.completedFuture(Unit.unit());
				        }						
					};
					Mockito.when(dws.storeObject(Mockito.any())).then(callback);
					Mockito.when(dws.storeObject(Mockito.any(), Mockito.anyBoolean())).then(callback);
					
					final IGenericDataService gds = Mockito.mock(IGenericDataService.class);
					Mockito.when(gds.getWritableDataService(Mockito.<Class<JsonNode>>any(), Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(Optional.of(dws));
					Mockito.when(ds.getDataService()).thenReturn(Optional.of(gds));
				}
				else { //full mapper
					
					@SuppressWarnings("unchecked")
					final IBatchSubservice<JsonNode> bss = Mockito.mock(IBatchSubservice.class);

					final Answer<Object> callback = new Answer<Object>() {
				        public Object answer(InvocationOnMock invocation) {
				        	_batch_responses.compute(ds.getClass().toString(), (k, v) -> (null == v) ? 1 : v + 1);
				            return (Object) CompletableFuture.completedFuture(Unit.unit());
				        }						
					};
					Mockito.doAnswer(callback).when(bss).flushOutput();
					Mockito.doAnswer(callback).when(bss).storeObject(Mockito.any());
					Mockito.doAnswer(callback).when(bss).storeObject(Mockito.any(), Mockito.anyBoolean());
					
					@SuppressWarnings("unchecked")
					final IDataWriteService<JsonNode> dws = (IDataWriteService<JsonNode>)Mockito.mock(IDataWriteService.class);
					Mockito.when(dws.getBatchWriteSubservice()).thenReturn(Optional.of(bss));
					
					final IGenericDataService gds = Mockito.mock(IGenericDataService.class);
					Mockito.when(gds.getWritableDataService(Mockito.<Class<JsonNode>>any(), Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(Optional.of(dws));
					Mockito.when(ds.getDataService()).thenReturn(Optional.of(gds));
				}
			});
			;
		
		
		mock_service_context.addService(ISearchIndexService.class, Optional.empty(), search_index);
		// Defaults:
		mock_service_context.addService(IDocumentService.class, Optional.empty(), search_index);
		mock_service_context.addService(IColumnarService.class, Optional.empty(), search_index);
		mock_service_context.addService(ITemporalService.class, Optional.empty(), search_index);
		mock_service_context.addService(IDataWarehouseService.class, Optional.empty(), data_warehouse);
		mock_service_context.addService(IStorageService.class, Optional.empty(), storage);
		mock_service_context.addService(IGraphService.class, Optional.empty(), graph);
		// Alts:
		mock_service_context.addService(IDocumentService.class, Optional.of("test"), document);
		mock_service_context.addService(IColumnarService.class, Optional.of("test"), columnar);
		mock_service_context.addService(ITemporalService.class, Optional.of("test"), temporal);
		mock_service_context.addService(IDataWarehouseService.class, Optional.of("test"), data_warehouse);
		mock_service_context.addService(IGraphService.class, Optional.of("test"), graph);
		
		return mock_service_context;
	}
	
	public static interface SearchIndexAndMoreService extends ISearchIndexService, IDocumentService, IColumnarService, ITemporalService {
		
	};
}
