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
package com.ikanow.aleph2.data_import_manager.governance.actors;

import static org.junit.Assert.*;

import java.io.File;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;

import scala.Tuple2;
import akka.actor.Props;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_import_manager.services.DataImportActorContext;
import com.ikanow.aleph2.data_import_manager.services.GeneralInformationService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.SearchIndexSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.TemporalSchemaBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.distributed_services.data_model.DistributedServicesPropertyBean;
import com.ikanow.aleph2.management_db.services.ManagementDbActorContext;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

public class TestDataAgeOutSupervisor {

	public static class TestAgeOutSearchIndexSettings implements ISearchIndexService {

		public boolean handled1 = false;
		public boolean handled2 = false;
		public boolean handled3 = false;
		public boolean handled4 = false; // (this one _shouldn't_ be handled)
		
		public TestAgeOutSearchIndexSettings() {			
		}
		
		@Override
		public Collection<Object> getUnderlyingArtefacts() {
			return null;
		}

		@Override
		public <T> Optional<T> getUnderlyingPlatformDriver(
				Class<T> driver_class, Optional<String> driver_options) {
			return null;
		}

		@Override
		public Tuple2<String, List<BasicMessageBean>> validateSchema(
				SearchIndexSchemaBean schema, DataBucketBean bucket) {
			return null;
		}

		@Override
		public Optional<IGenericDataService> getDataService() {
			return Optional.of(_test);
		}

		public IGenericDataService _test = new IGenericDataService() {

			@Override
			public <O> Optional<IDataWriteService<O>> getWritableDataService(
					Class<O> clazz, DataBucketBean bucket,
					Optional<String> options, Optional<String> secondary_buffer) {
				return null;
			}

			@Override
			public <O> Optional<ICrudService<O>> getReadableCrudService(
					Class<O> clazz, Collection<DataBucketBean> buckets,
					Optional<String> options) {
				return null;
			}

			@Override
			public Set<String> getSecondaryBuffers(
					DataBucketBean bucket, Optional<String> intermediate_step) {
				return null;
			}

			@Override
			public CompletableFuture<BasicMessageBean> switchCrudServiceToPrimaryBuffer(
					DataBucketBean bucket, Optional<String> secondary_buffer, final Optional<String> new_name_for_ex_primary, Optional<String> intermediate_step) {
				return null;
			}

			@Override
			public CompletableFuture<BasicMessageBean> handleAgeOutRequest(
					DataBucketBean bucket) {
				
				final String test_name = bucket.full_name();
				if (test_name.equals("/test/1")) {
					final BasicMessageBean msg = ErrorUtils.buildErrorMessage("test1", "test1", "test1");
					handled1 = true;
					return CompletableFuture.completedFuture(msg);
				}
				else if (test_name.equals("/test/2")) {
					final BasicMessageBean msg = ErrorUtils.buildSuccessMessage("test2", "test2", "test2");
					handled2 = true;
					return CompletableFuture.completedFuture(msg);
				}
				else if (test_name.equals("/test/3")) {
					final BasicMessageBean msg = ErrorUtils.buildSuccessMessage("test3", "test3", "test3");
					
					final BasicMessageBean loggable = BeanTemplateUtils.clone(msg).with(BasicMessageBean::details,
							ImmutableMap.builder().put("loggable", "anything").build()
							).done();
					
					handled3 = true;
					return CompletableFuture.completedFuture(loggable);
				}
				else { //if (test_name.equals("/test/4")) {
					handled4 = true;
					final BasicMessageBean msg = ErrorUtils.buildErrorMessage("test4", "test4", "test4");
					return CompletableFuture.completedFuture(msg);
				}
			}

			@Override
			public CompletableFuture<BasicMessageBean> handleBucketDeletionRequest(
					DataBucketBean bucket, Optional<String> secondary_buffer,
					boolean bucket_getting_deleted) {
				return null;
			}

			@Override
			public Optional<String> getPrimaryBufferName(DataBucketBean bucket, Optional<String> intermediate_step) {
				return null;
			}
			
		};

	}
	
	TestAgeOutSearchIndexSettings _test_results;	
	
	@Inject 
	protected IServiceContext _service_context = null;
	
	protected DataImportActorContext _actor_context;
	protected ManagementDbActorContext _db_actor_context;
	
	@SuppressWarnings("deprecation")
	@Before
	public void setup() throws Exception {
		
		if (null != _service_context) {
			return;
		}
		
		final String temp_dir = System.getProperty("java.io.tmpdir") + File.separator;
		
		ManagementDbActorContext.unsetSingleton();
		
		// OK we're going to use guice, it was too painful doing this by hand...				
		Config config = ConfigFactory.parseReader(new InputStreamReader(this.getClass().getResourceAsStream("test_data_age_out.properties")))
							.withValue("globals.local_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
							.withValue("globals.local_cached_jar_dir", ConfigValueFactory.fromAnyRef(temp_dir))
							.withValue("globals.distributed_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
							.withValue("globals.local_yarn_config_dir", ConfigValueFactory.fromAnyRef(temp_dir));
		
		Injector app_injector = ModuleUtils.createTestInjector(Arrays.asList(), Optional.of(config));	
		app_injector.injectMembers(this);
		
		_actor_context = new DataImportActorContext(_service_context, new GeneralInformationService(), null, null); 
		app_injector.injectMembers(_actor_context);
		
		// Have to do this in order for the underlying management db to live...		
		_service_context.getCoreManagementDbService();
		
		_test_results = (TestAgeOutSearchIndexSettings) _service_context.getSearchIndexService().get();
		
	}
	
	@Test
	public void test_dataAgeOutSupervisor() throws InterruptedException, ExecutionException {

		// Get the bucket DB set up the way we want:
		
		final DataBucketBean test1 = BeanTemplateUtils.build(DataBucketBean.class)
											.with(DataBucketBean::full_name, "/test/1")
											.with(DataBucketBean::data_schema,
													BeanTemplateUtils.build(DataSchemaBean.class)
													.with(DataSchemaBean::temporal_schema,
															BeanTemplateUtils.build(TemporalSchemaBean.class)
															.with(TemporalSchemaBean::exist_age_max, "1 day")
															.done().get()
															)
													.done().get())
										.done().get();
		
		final DataBucketBean test2 = BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::full_name, "/test/2")
				.with(DataBucketBean::data_schema,
						BeanTemplateUtils.build(DataSchemaBean.class)
						.with(DataSchemaBean::temporal_schema,
								BeanTemplateUtils.build(TemporalSchemaBean.class)
								.with(TemporalSchemaBean::exist_age_max, "1 day")
								.done().get()
								)
						.done().get())
			.done().get();

		final DataBucketBean test3 = BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::full_name, "/test/3")
				.with(DataBucketBean::data_schema,
						BeanTemplateUtils.build(DataSchemaBean.class)
						.with(DataSchemaBean::temporal_schema,
								BeanTemplateUtils.build(TemporalSchemaBean.class)
								.with(TemporalSchemaBean::exist_age_max, "1 day")
								.done().get()
								)
						.done().get())
			.done().get();

		final DataBucketBean test4 = BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::full_name, "/test/3")
				.with(DataBucketBean::data_schema,
						BeanTemplateUtils.build(DataSchemaBean.class)
						.done().get())
			.done().get();
		
		final ICrudService<DataBucketBean> dbc = _service_context.getService(IManagementDbService.class, Optional.empty()).get()
				.getDataBucketStore();
		
		dbc.deleteDatastore().get();
		assertEquals(0, dbc.countObjects().get().intValue());
		
		dbc.storeObjects(Arrays.asList(test1, test2, test3, test4)).get();
		assertEquals(4, dbc.countObjects().get().intValue());
		
		// Emulate the start up code in data import manager:
		
		_actor_context.getDistributedServices().createSingletonActor("TEST" + ".governance.actors.DataAgeOutSupervisor", 
				ImmutableSet.<String>builder().add(DistributedServicesPropertyBean.ApplicationNames.DataImportManager.toString()).build(), 
				Props.create(DataAgeOutSupervisor.class));		
		
		// Wait for it to run
		Thread.sleep(3000L);
		
		// Check results
		
		assertEquals(true, _test_results.handled1);
		assertEquals(true, _test_results.handled2);
		assertEquals(true, _test_results.handled3);
		assertEquals(false, _test_results.handled4);
	}
}
