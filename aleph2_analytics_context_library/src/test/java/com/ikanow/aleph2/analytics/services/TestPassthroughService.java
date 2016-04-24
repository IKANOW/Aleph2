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

import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.Test;
import org.mockito.Mockito;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule.ProcessingStage;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;

/** Passthrough service doesn't really do anything, so this is 
 * @author Alex
 */
public class TestPassthroughService {
	final static protected ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());
	
	class TestBatchRecord implements IBatchRecord {
		@Override
		public JsonNode getJson() {
			return _mapper.createObjectNode().set("object_field",  _mapper.createObjectNode().put("field", "/test"));
		}		
	}
	
	@Test
	public void test_passthroughService() {

		// Pass (default)
		{
			// Some globals
			final EnrichmentControlMetadataBean control = BeanTemplateUtils.build(EnrichmentControlMetadataBean.class)
						.with(EnrichmentControlMetadataBean::name, "test")
					.done().get();
			final IEnrichmentModuleContext enrich_context = Mockito.mock(IEnrichmentModuleContext.class);
			Mockito.when(enrich_context.emitImmutableObject(Mockito.anyLong(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
				.thenThrow(new RuntimeException("normal_output"));
			Mockito.when(enrich_context.externalEmit(Mockito.any(), Mockito.any(), Mockito.any()))
				.thenThrow(new RuntimeException("external_emit"));
			
			final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class).with(DataBucketBean::full_name, "/test").done().get();
			
			final PassthroughService test_module = new PassthroughService();
			
			test_module.onStageInitialize(enrich_context, test_bucket, control, Tuples._2T(ProcessingStage.input, ProcessingStage.output), Optional.empty());
			
			test_module.onObjectBatch(Stream.empty(), Optional.empty(), Optional.empty());
	
			try {
				test_module.onObjectBatch(Stream.of(Tuples._2T(0L, new TestBatchRecord())), Optional.empty(), Optional.empty());
				fail("Should have thrown");
			}
			catch (Exception e) {
				assertEquals("normal_output", e.getMessage());
			}
			
			test_module.onStageComplete(true);		
		}
		// Pass (specify)
		{
			// Some globals
			final EnrichmentControlMetadataBean control = BeanTemplateUtils.build(EnrichmentControlMetadataBean.class)
						.with(EnrichmentControlMetadataBean::name, "test")
						.with(EnrichmentControlMetadataBean::config, new LinkedHashMap<>(ImmutableMap.of(PassthroughService.OUTPUT_TO_FIELDNAME, PassthroughService.OUTPUT_TO_INTERNAL)))
					.done().get();
			final IEnrichmentModuleContext enrich_context = Mockito.mock(IEnrichmentModuleContext.class);
			Mockito.when(enrich_context.emitImmutableObject(Mockito.anyLong(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
				.thenThrow(new RuntimeException("normal_output"));
			Mockito.when(enrich_context.externalEmit(Mockito.any(), Mockito.any(), Mockito.any()))
				.thenThrow(new RuntimeException("external_emit"));
			
			final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class).with(DataBucketBean::full_name, "/test").done().get();
			
			final PassthroughService test_module = new PassthroughService();
			
			test_module.onStageInitialize(enrich_context, test_bucket, control, Tuples._2T(ProcessingStage.input, ProcessingStage.output), Optional.empty());
			
			test_module.onObjectBatch(Stream.empty(), Optional.empty(), Optional.empty());
	
			try {
				test_module.onObjectBatch(Stream.of(Tuples._2T(0L, new TestBatchRecord())), Optional.empty(), Optional.empty());
				fail("Should have thrown");
			}
			catch (Exception e) {
				assertEquals("normal_output", e.getMessage());
			}
			
			test_module.onStageComplete(true);		
		}
		// Stop
		{
			// Some globals
			final EnrichmentControlMetadataBean control = BeanTemplateUtils.build(EnrichmentControlMetadataBean.class)
						.with(EnrichmentControlMetadataBean::name, "test")
						.with(EnrichmentControlMetadataBean::config, new LinkedHashMap<>(ImmutableMap.of(PassthroughService.OUTPUT_TO_FIELDNAME, PassthroughService.OUTPUT_TO_STOP)))
					.done().get();
			final IEnrichmentModuleContext enrich_context = Mockito.mock(IEnrichmentModuleContext.class);
			Mockito.when(enrich_context.emitImmutableObject(Mockito.anyLong(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
				.thenThrow(new RuntimeException("normal_output"));
			Mockito.when(enrich_context.externalEmit(Mockito.any(), Mockito.any(), Mockito.any()))
				.thenThrow(new RuntimeException("external_emit"));
			
			final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class).with(DataBucketBean::full_name, "/test").done().get();
			
			final PassthroughService test_module = new PassthroughService();
			
			test_module.onStageInitialize(enrich_context, test_bucket, control, Tuples._2T(ProcessingStage.input, ProcessingStage.output), Optional.empty());
			
			test_module.onObjectBatch(Stream.empty(), Optional.empty(), Optional.empty());
	
			try {
				test_module.onObjectBatch(Stream.of(Tuples._2T(0L, new TestBatchRecord())), Optional.empty(), Optional.empty());
			}
			catch (Exception e) {
				fail("Shouldn't have thrown");
			}
			
			test_module.onStageComplete(true);		
		}
		// By field - external emit
		{
			// Some globals
			final EnrichmentControlMetadataBean control = BeanTemplateUtils.build(EnrichmentControlMetadataBean.class)
						.with(EnrichmentControlMetadataBean::name, "test")
						.with(EnrichmentControlMetadataBean::config, new LinkedHashMap<>(ImmutableMap.of(PassthroughService.OUTPUT_TO_FIELDNAME, "$object_field.field")))
					.done().get();
			final IEnrichmentModuleContext enrich_context = Mockito.mock(IEnrichmentModuleContext.class);
			Mockito.when(enrich_context.emitImmutableObject(Mockito.anyLong(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
				.thenThrow(new RuntimeException("normal_output"));
			Mockito.when(enrich_context.externalEmit(Mockito.any(), Mockito.any(), Mockito.any()))
				.thenThrow(new RuntimeException("external_emit"));
			
			final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class).with(DataBucketBean::full_name, "/test").done().get();
			
			final PassthroughService test_module = new PassthroughService();
			
			test_module.onStageInitialize(enrich_context, test_bucket, control, Tuples._2T(ProcessingStage.input, ProcessingStage.output), Optional.empty());
			
			test_module.onObjectBatch(Stream.empty(), Optional.empty(), Optional.empty());
	
			try {
				test_module.onObjectBatch(Stream.of(Tuples._2T(0L, new TestBatchRecord())), Optional.empty(), Optional.empty());
				fail("Should have thrown");
			}
			catch (Exception e) {
				assertEquals("external_emit", e.getMessage());
			}
			
			test_module.onStageComplete(true);					
		}
		// Error case - missing field
		{
			// Some globals
			final EnrichmentControlMetadataBean control = BeanTemplateUtils.build(EnrichmentControlMetadataBean.class)
						.with(EnrichmentControlMetadataBean::name, "test")
						.with(EnrichmentControlMetadataBean::config, new LinkedHashMap<>(ImmutableMap.of(PassthroughService.OUTPUT_TO_FIELDNAME, "$missing_field")))
					.done().get();
			final IEnrichmentModuleContext enrich_context = Mockito.mock(IEnrichmentModuleContext.class);
			Mockito.when(enrich_context.emitImmutableObject(Mockito.anyLong(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
				.thenThrow(new RuntimeException("normal_output"));
			Mockito.when(enrich_context.externalEmit(Mockito.any(), Mockito.any(), Mockito.any()))
				.thenThrow(new RuntimeException("external_emit"));
			
			final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class).with(DataBucketBean::full_name, "/test").done().get();
			
			final PassthroughService test_module = new PassthroughService();
			
			test_module.onStageInitialize(enrich_context, test_bucket, control, Tuples._2T(ProcessingStage.input, ProcessingStage.output), Optional.empty());
			
			test_module.onObjectBatch(Stream.empty(), Optional.empty(), Optional.empty());
	
			try {
				test_module.onObjectBatch(Stream.of(Tuples._2T(0L, new TestBatchRecord())), Optional.empty(), Optional.empty());
			}
			catch (Exception e) {
				fail("Shouldn't have thrown: " + e.getMessage());
			}
			
			test_module.onStageComplete(true);		
		}
		// Error case - non-string field
		{
			// Some globals
			final EnrichmentControlMetadataBean control = BeanTemplateUtils.build(EnrichmentControlMetadataBean.class)
						.with(EnrichmentControlMetadataBean::name, "test")
						.with(EnrichmentControlMetadataBean::config, new LinkedHashMap<>(ImmutableMap.of(PassthroughService.OUTPUT_TO_FIELDNAME, "$object_field")))
					.done().get();
			final IEnrichmentModuleContext enrich_context = Mockito.mock(IEnrichmentModuleContext.class);
			Mockito.when(enrich_context.emitImmutableObject(Mockito.anyLong(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
				.thenThrow(new RuntimeException("normal_output"));
			Mockito.when(enrich_context.externalEmit(Mockito.any(), Mockito.any(), Mockito.any()))
				.thenThrow(new RuntimeException("external_emit"));
			
			final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class).with(DataBucketBean::full_name, "/test").done().get();
			
			final PassthroughService test_module = new PassthroughService();
			
			test_module.onStageInitialize(enrich_context, test_bucket, control, Tuples._2T(ProcessingStage.input, ProcessingStage.output), Optional.empty());
			
			test_module.onObjectBatch(Stream.empty(), Optional.empty(), Optional.empty());
	
			try {
				test_module.onObjectBatch(Stream.of(Tuples._2T(0L, new TestBatchRecord())), Optional.empty(), Optional.empty());
			}
			catch (Exception e) {
				fail("Shouldn't have thrown");
			}
			
			test_module.onStageComplete(true);		
		}
	}
	
}
