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

import static org.junit.Assert.*;

import java.io.File;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule.ProcessingStage;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
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
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

public class TestDefaultDedupEnrichmentService {
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
			final EnrichmentControlMetadataBean control = BeanTemplateUtils.build(EnrichmentControlMetadataBean.class).done().get();
			final IEnrichmentModuleContext enrich_context = Mockito.mock(IEnrichmentModuleContext.class);
			Mockito.when(enrich_context.getServiceContext()).thenReturn(_service_context);
			
			
			// simple version (default temporal field)
			{
				final DataBucketBean test_bucket = getDocBucket("/test/simple",
						BeanTemplateUtils.build(DocumentSchemaBean.class)
						.done().get()
						);
				
				final DefaultDedupEnrichmentService test_module = new DefaultDedupEnrichmentService();
				
				test_module.onStageInitialize(enrich_context, test_bucket, control, Tuples._2T(ProcessingStage.input, ProcessingStage.output), Optional.empty());
				
				assertEquals(test_module._doc_schema.get(), test_bucket.data_schema().document_schema());
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
												.with(EnrichmentControlMetadataBean::entry_point, TestDedupEnrichmentModule.class.getName())
											.done().get()										
											)
									)
						.done().get()
						));
				
				final DefaultDedupEnrichmentService test_module = new DefaultDedupEnrichmentService();
				
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
				
				final DefaultDedupEnrichmentService test_module = new DefaultDedupEnrichmentService();
				
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
	
	//TODO: test all the static utils
	
	//TODO: putting it all together
	
	////////////////////////////////////////////////////
	
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
	
	public static class TestDedupEnrichmentModule implements IEnrichmentBatchModule {

		@Override
		public void onStageInitialize(IEnrichmentModuleContext context,
				DataBucketBean bucket, EnrichmentControlMetadataBean control,
				Tuple2<ProcessingStage, ProcessingStage> previous_next,
				Optional<List<String>> next_grouping_fields) {
		}

		@Override
		public void onObjectBatch(Stream<Tuple2<Long, IBatchRecord>> batch,
				Optional<Integer> batch_size, Optional<JsonNode> grouping_key) {
		}

		@Override
		public void onStageComplete(boolean is_original) {
		}
		
	}
	
	
}
