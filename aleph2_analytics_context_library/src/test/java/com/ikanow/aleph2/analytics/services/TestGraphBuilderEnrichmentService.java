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

package com.ikanow.aleph2.analytics.services;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.junit.Test;
import org.mockito.Mockito;

import scala.Tuple2;

import com.codepoetics.protonpack.Streamable;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ikanow.aleph2.analytics.data_model.GraphConfigBean;
import com.ikanow.aleph2.core.shared.utils.BatchRecordUtils;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.interfaces.data_services.IGraphService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.MockServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.GraphSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;

/**
 * @author Alex
 *
 */
public class TestGraphBuilderEnrichmentService {
	final protected static ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());

	@Test
	public void test_delegation() {
		final AtomicInteger wrapper_counter = new AtomicInteger(0);
		final AtomicInteger emit_counter = new AtomicInteger(0);
		final AtomicInteger init_counter = new AtomicInteger(0);
		final AtomicInteger done_counter = new AtomicInteger(0);
		
		final Streamable<Tuple2<Long, IBatchRecord>> test_stream = 
				Streamable.of(Arrays.asList(
						_mapper.createObjectNode()
						))
						.<Tuple2<Long, IBatchRecord>>map(j -> Tuples._2T(0L, new BatchRecordUtils.JsonBatchRecord(j)))
						;

		final IEnrichmentBatchModule delegate = Mockito.mock(IEnrichmentBatchModule.class);
		Mockito.doAnswer(__ -> {
			init_counter.incrementAndGet();
			return null;
		})
		.when(delegate).onStageInitialize(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
		Mockito.doAnswer(in -> {
			@SuppressWarnings("unchecked")
			final Stream<Tuple2<Long, IBatchRecord>> stream = (Stream<Tuple2<Long, IBatchRecord>>) in.getArguments()[0];
			stream.forEach(t2 -> emit_counter.incrementAndGet());
			return null;
		})
		.when(delegate).onObjectBatch(Mockito.any(), Mockito.any(), Mockito.any());
		Mockito.doAnswer(__ -> {
			done_counter.incrementAndGet();
			return null;
		})
		.when(delegate).onStageComplete(Mockito.anyBoolean());
		Mockito.when(delegate.cloneForNewGrouping()).thenReturn(delegate);
		Mockito.when(delegate.validateModule(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(Arrays.asList(ErrorUtils.buildErrorMessage("", "", "")));
		
		
		final IGraphService throwing_graph_service = Mockito.mock(IGraphService.class);
		Mockito.when(throwing_graph_service.getUnderlyingPlatformDriver(Mockito.any(), Mockito.any())).thenReturn(Optional.of(delegate));
		final MockServiceContext mock_service_context = new MockServiceContext();
		mock_service_context.addService(IGraphService.class, Optional.empty(), throwing_graph_service);
		final IEnrichmentModuleContext enrich_context = Mockito.mock(IEnrichmentModuleContext.class);
		Mockito.when(enrich_context.getServiceContext()).thenReturn(mock_service_context);
		Mockito.when(enrich_context.emitImmutableObject(Mockito.anyLong(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
					.thenAnswer(invocation -> {
						wrapper_counter.incrementAndGet();						
						return null;
					});

		final GraphSchemaBean graph_schema = BeanTemplateUtils.build(GraphSchemaBean.class).done().get();
		final DataBucketBean bucket = BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::data_schema, 
						BeanTemplateUtils.build(DataSchemaBean.class)
							.with(DataSchemaBean::graph_schema, graph_schema)
						.done().get()
						)
				.done().get(); 
		final EnrichmentControlMetadataBean control = BeanTemplateUtils.build(EnrichmentControlMetadataBean.class).done().get();

		final GraphBuilderEnrichmentService under_test = new GraphBuilderEnrichmentService();
		under_test.onStageInitialize(enrich_context, bucket, control, Tuples._2T(null, null), Optional.empty());
		under_test.onObjectBatch(test_stream.stream(), Optional.empty(), Optional.empty());
		under_test.onStageComplete(true);			
		assertEquals(delegate, under_test.cloneForNewGrouping());
		assertEquals(1, under_test.validateModule(enrich_context, bucket, control).size());
		assertEquals(1, emit_counter.getAndSet(0));
		assertEquals(1, init_counter.getAndSet(0));
		assertEquals(1, done_counter.getAndSet(0));		
		assertEquals(1, wrapper_counter.getAndSet(0));
	}
	
	@Test
	public void test_empty() {
		
		final AtomicInteger counter = new AtomicInteger(0);
		
		final Streamable<Tuple2<Long, IBatchRecord>> test_stream = 
				Streamable.of(Arrays.asList(
						_mapper.createObjectNode()
						))
						.<Tuple2<Long, IBatchRecord>>map(j -> Tuples._2T(0L, new BatchRecordUtils.JsonBatchRecord(j)))
						;
		
		final IGraphService throwing_graph_service = Mockito.mock(IGraphService.class);
		Mockito.when(throwing_graph_service.getUnderlyingPlatformDriver(Mockito.any(), Mockito.any())).thenThrow(new RuntimeException("getUnderlyingPlatformDriver"));
		final MockServiceContext mock_service_context = new MockServiceContext();
		final IEnrichmentModuleContext enrich_context = Mockito.mock(IEnrichmentModuleContext.class);
		Mockito.when(enrich_context.getServiceContext()).thenReturn(mock_service_context);
		Mockito.when(enrich_context.emitImmutableObject(Mockito.anyLong(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
					.thenAnswer(invocation -> {
						counter.incrementAndGet();						
						return null;
					});

		// Bucket enabled but no graph service
		{
			final GraphBuilderEnrichmentService under_test = new GraphBuilderEnrichmentService();
			
			final GraphSchemaBean graph_schema = BeanTemplateUtils.build(GraphSchemaBean.class).done().get();
			final DataBucketBean bucket = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::data_schema, 
							BeanTemplateUtils.build(DataSchemaBean.class)
								.with(DataSchemaBean::graph_schema, graph_schema)
							.done().get()
							)
					.done().get(); 
			final EnrichmentControlMetadataBean control = BeanTemplateUtils.build(EnrichmentControlMetadataBean.class).done().get();
	
			under_test.onStageInitialize(enrich_context, bucket, control, Tuples._2T(null, null), Optional.empty());
			under_test.onObjectBatch(test_stream.stream(), Optional.empty(), Optional.empty());
			under_test.onStageComplete(true);			
			assertEquals(under_test, under_test.cloneForNewGrouping());
			assertEquals(Collections.emptyList(), under_test.validateModule(enrich_context, bucket, control));
			assertEquals(1, counter.getAndSet(0));
		}
		// Use override
		{
			final GraphBuilderEnrichmentService under_test = new GraphBuilderEnrichmentService();
			
			final GraphSchemaBean graph_schema = BeanTemplateUtils.build(GraphSchemaBean.class).done().get();
			final GraphConfigBean graph_config = BeanTemplateUtils.build(GraphConfigBean.class)
						.with(GraphConfigBean::graph_schema_override, graph_schema)
					.done().get();
			final DataBucketBean bucket = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::data_schema, 
							BeanTemplateUtils.build(DataSchemaBean.class)
								.with(DataSchemaBean::graph_schema, graph_schema)
							.done().get()
							)
					.done().get(); 
			final EnrichmentControlMetadataBean control = BeanTemplateUtils.build(EnrichmentControlMetadataBean.class)
						.with(EnrichmentControlMetadataBean::config, new LinkedHashMap<String, Object>(BeanTemplateUtils.toMap(graph_config)))
					.done().get();
	
			under_test.onStageInitialize(enrich_context, bucket, control, Tuples._2T(null, null), Optional.empty());
			under_test.onObjectBatch(test_stream.stream(), Optional.empty(), Optional.empty());
			under_test.onStageComplete(true);						
			assertEquals(under_test, under_test.cloneForNewGrouping());
			assertEquals(Collections.emptyList(), under_test.validateModule(enrich_context, bucket, control));
			assertEquals(1, counter.getAndSet(0));
		}
		
		mock_service_context.addService(IGraphService.class, Optional.empty(), throwing_graph_service);
		
		// Add graph service, check it starts failing (bucket enabled)
		{
			final GraphBuilderEnrichmentService under_test = new GraphBuilderEnrichmentService();
			
			final GraphSchemaBean graph_schema = BeanTemplateUtils.build(GraphSchemaBean.class).done().get();
			final DataBucketBean bucket = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::data_schema, 
							BeanTemplateUtils.build(DataSchemaBean.class)
								.with(DataSchemaBean::graph_schema, graph_schema)
							.done().get()
							)
					.done().get(); 
			final EnrichmentControlMetadataBean control = BeanTemplateUtils.build(EnrichmentControlMetadataBean.class).done().get();
	
			try {
				under_test.onStageInitialize(enrich_context, bucket, control, Tuples._2T(null, null), Optional.empty());
				fail("Should have thrown");
			}
			catch (Exception e) {}
		}
		// Add graph service, check it starts failing (override)
		{
			final GraphBuilderEnrichmentService under_test = new GraphBuilderEnrichmentService();
			
			final GraphSchemaBean graph_schema = BeanTemplateUtils.build(GraphSchemaBean.class).done().get();
			final GraphConfigBean graph_config = BeanTemplateUtils.build(GraphConfigBean.class)
						.with(GraphConfigBean::graph_schema_override, graph_schema)
					.done().get();
			final DataBucketBean bucket = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::data_schema, 
							BeanTemplateUtils.build(DataSchemaBean.class)
								.with(DataSchemaBean::graph_schema, graph_schema)
							.done().get()
							)
					.done().get(); 
			final EnrichmentControlMetadataBean control = BeanTemplateUtils.build(EnrichmentControlMetadataBean.class)
						.with(EnrichmentControlMetadataBean::config, new LinkedHashMap<String, Object>(BeanTemplateUtils.toMap(graph_config)))
					.done().get();
			
			try {
				under_test.onStageInitialize(enrich_context, bucket, control, Tuples._2T(null, null), Optional.empty());
				fail("Should have thrown");
			}
			catch (Exception e) {}
		}
		// From bucket, graph service disabled, won't fail
		{
			final GraphBuilderEnrichmentService under_test = new GraphBuilderEnrichmentService();
			
			final DataBucketBean bucket = BeanTemplateUtils.build(DataBucketBean.class).done().get(); // (no data_schema.graph_schema)
			final EnrichmentControlMetadataBean control = BeanTemplateUtils.build(EnrichmentControlMetadataBean.class).done().get();
	
			under_test.onStageInitialize(enrich_context, bucket, control, Tuples._2T(null, null), Optional.empty());
			under_test.onObjectBatch(test_stream.stream(), Optional.empty(), Optional.empty());
			under_test.onStageComplete(true);
			assertEquals(under_test, under_test.cloneForNewGrouping());
			assertEquals(Collections.emptyList(), under_test.validateModule(enrich_context, bucket, control));
			assertEquals(1, counter.getAndSet(0));
		}
		// From override, graph service disabled, won't fail
		{
			final GraphBuilderEnrichmentService under_test = new GraphBuilderEnrichmentService();
			
			final GraphSchemaBean graph_schema = BeanTemplateUtils.build(GraphSchemaBean.class)
						.with(GraphSchemaBean::enabled, false)
					.done().get();
			final GraphConfigBean graph_config = BeanTemplateUtils.build(GraphConfigBean.class)
						.with(GraphConfigBean::graph_schema_override, graph_schema)
					.done().get();
			final DataBucketBean bucket = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::data_schema, 
							BeanTemplateUtils.build(DataSchemaBean.class)
								.with(DataSchemaBean::graph_schema, graph_schema)
							.done().get()
							)
					.done().get(); 
			final EnrichmentControlMetadataBean control = BeanTemplateUtils.build(EnrichmentControlMetadataBean.class)
						.with(EnrichmentControlMetadataBean::config, new LinkedHashMap<String, Object>(BeanTemplateUtils.toMap(graph_config)))
					.done().get();
	
			under_test.onStageInitialize(enrich_context, bucket, control, Tuples._2T(null, null), Optional.empty());
			under_test.onObjectBatch(test_stream.stream(), Optional.empty(), Optional.empty());
			under_test.onStageComplete(true);
			assertEquals(under_test, under_test.cloneForNewGrouping());
			assertEquals(Collections.emptyList(), under_test.validateModule(enrich_context, bucket, control));
			assertEquals(1, counter.getAndSet(0));
		}
		
	}
}
