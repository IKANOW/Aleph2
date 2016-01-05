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
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.analytics.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.MockSecurityService;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean.AnalyticThreadJobInputBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean.MasterEnrichmentType;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import fj.data.Either;
import fj.data.Validation;

public class TestAnalyticsContext_FileSystemChecks {

	static final Logger _logger = LogManager.getLogger(); 

	protected ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());
	protected Injector _app_injector;
	
	@Inject
	protected IServiceContext _service_context;
	
	@Before
	public void injectModules() throws Exception {
		_logger.info("run injectModules");
		
		final String temp_dir = System.getProperty("java.io.tmpdir") + File.separator;		
		
		final Config config = ConfigFactory.parseFile(new File("./example_config_files/context_local_test.properties"))
												.withValue("globals.local_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
												.withValue("globals.local_cached_jar_dir", ConfigValueFactory.fromAnyRef(temp_dir))
												.withValue("globals.distributed_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
												.withValue("globals.local_yarn_config_dir", ConfigValueFactory.fromAnyRef(temp_dir));
		
		try {
			_app_injector = ModuleUtils.createTestInjector(Arrays.asList(), Optional.of(config));
			_app_injector.injectMembers(this);
		}
		catch (Exception e) {
			try {
				e.printStackTrace();
			}
			catch (Exception ee) {
				System.out.println(ErrorUtils.getLongForm("{0}", e));
			}
		}
	}
	
	// This seems broken - look into it but ignore for now to check nothing else is a problem
	@org.junit.Ignore
	@Test
	public void test_storageService_timedInputPaths() throws InterruptedException, ExecutionException {
		
		final AnalyticsContext test_context = _app_injector.getInstance(AnalyticsContext.class);		

		File f = new File(_service_context.getStorageService().getBucketRootPath() + "/this_bucket" + IStorageService.STORED_DATA_SUFFIX_PROCESSED);
		FileUtils.deleteQuietly(f);
		
		final AnalyticThreadJobBean.AnalyticThreadJobInputBean analytic_input1 =  BeanTemplateUtils.build(AnalyticThreadJobBean.AnalyticThreadJobInputBean.class)
				.with(AnalyticThreadJobBean.AnalyticThreadJobInputBean::data_service, "storage_service")
				.with(AnalyticThreadJobBean.AnalyticThreadJobInputBean::resource_name_or_id, "/this_bucket") //(just avoids DB check)
				.with(AnalyticThreadJobBean.AnalyticThreadJobInputBean::config,
						BeanTemplateUtils.build(AnalyticThreadJobBean.AnalyticThreadJobInputConfigBean.class)
							.with(AnalyticThreadJobBean.AnalyticThreadJobInputConfigBean::time_min, "1 year")
						.done().get()
						)
				.done().get();
		
		final AnalyticThreadJobBean analytic_job1 = BeanTemplateUtils.build(AnalyticThreadJobBean.class)
				.with(AnalyticThreadJobBean::name, "test_name1")
				.with(AnalyticThreadJobBean::analytic_technology_name_or_id, "test_analytic_tech_id")
				.with(AnalyticThreadJobBean::inputs, Arrays.asList(analytic_input1))
				.with(AnalyticThreadJobBean::library_names_or_ids, Arrays.asList("id1", "name2"))
				.done().get();		
		
		final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::_id, "this_bucket")
				.with(DataBucketBean::full_name, "/this_bucket")
				.with(DataBucketBean::analytic_thread, 
						BeanTemplateUtils.build(AnalyticThreadBean.class)
						.with(AnalyticThreadBean::jobs, Arrays.asList(analytic_job1)
								)
								.done().get()
						)
						.done().get();
		
		test_context._service_context.getService(IManagementDbService.class, Optional.empty()).get().getDataBucketStore().storeObject(test_bucket).get();		
		
		// Check falls back to current if dirs don't exist
		
		assertTrue("Falls back to full storage", 
				test_context.getInputPaths(Optional.of(test_bucket), analytic_job1, analytic_input1).get(0).endsWith("/this_bucket/managed_bucket/import/stored/processed/current/**/*"));
		
		@SuppressWarnings("deprecation")
		final int year = 1900 + new Date().getYear(); 
		
		createDirs(f, Arrays.asList("test_" + (year-3), "test_" + (year-2), "test_" + (year-1), "test_" + (year)));
		
		final List<String> res = test_context.getInputPaths(Optional.of(test_bucket), analytic_job1, analytic_input1);
		
		assertEquals("Timed slices: " + res.stream().collect(Collectors.joining(";")),
				Arrays.asList("/current/test_2014/*", "/current/test_2015/*"),
				res.stream().map(s -> s.substring(s.indexOf("/current/"))).sorted().collect(Collectors.toList())
				);
	}

	@Test
	public void test_externalEmit() throws JsonProcessingException, IOException, InterruptedException {
		test_externalEmit_worker(false);
	}
	@Test
	public void test_externalEmit_testMode() throws JsonProcessingException, IOException, InterruptedException {
		test_externalEmit_worker(true);
	}
		
	public void test_externalEmit_worker(boolean is_test) throws JsonProcessingException, IOException, InterruptedException {
		
		final MockSecurityService mock_security = (MockSecurityService) _service_context.getSecurityService();
		
		// Create some buckets:

		// 0) My bucket
		
		final AnalyticsContext test_context = _app_injector.getInstance(AnalyticsContext.class);
		
		final AnalyticThreadJobInputBean input =
				BeanTemplateUtils.build(AnalyticThreadJobInputBean.class)
					.with(AnalyticThreadJobInputBean::resource_name_or_id, "/test/analytics/batch")
				.done().get();
		
		final AnalyticThreadJobBean job = 
				BeanTemplateUtils.build(AnalyticThreadJobBean.class)
					.with(AnalyticThreadJobBean::name, "test")
					.with(AnalyticThreadJobBean::analytic_type, MasterEnrichmentType.batch)
					.with(AnalyticThreadJobBean::inputs, Arrays.asList(input))
				.done().get();
		
		final DataBucketBean my_bucket = 
				BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::full_name, is_test ? "/aleph2_testing/useriid/test/me" : "/test/me")
				.with(DataBucketBean::owner_id, "me")
			.done().get();		
		
		test_context.setBucket(my_bucket);
				
		// 2) Batch analytic bucket
		
		//(see TestAnalyticsContext_FileSystemChecks)
		
		final DataBucketBean analytic_bucket_batch = 
				BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::full_name, "/test/analytics/batch")
					.with(DataBucketBean::analytic_thread,
							BeanTemplateUtils.build(AnalyticThreadBean.class)
								.with(AnalyticThreadBean::jobs, Arrays.asList(job))
							.done().get()
							)
				.done().get();
		test_context._service_context.getService(IManagementDbService.class, Optional.empty()).get().getDataBucketStore().storeObject(analytic_bucket_batch, true).join();
		mock_security.setUserMockRole("me", analytic_bucket_batch.full_name(), ISecurityService.ACTION_READ_WRITE, true);
		
		File f_tmp = new File(_service_context.getStorageService().getBucketRootPath() + analytic_bucket_batch.full_name() + IStorageService.TEMP_DATA_SUFFIX);
		File f_import = new File(_service_context.getStorageService().getBucketRootPath() + analytic_bucket_batch.full_name() + IStorageService.TO_IMPORT_DATA_SUFFIX);
		FileUtils.deleteQuietly(f_tmp);		
		FileUtils.deleteQuietly(f_import);						
		createDirs(f_tmp, Arrays.asList(""));
		createDirs(f_import, Arrays.asList(""));		
		assertTrue("Should exist:" + f_tmp, f_tmp.exists());
		assertTrue("Should exist:" + f_import, f_import.exists());
		
		// create some directories
		
		// emit the objects
		
		final Validation<BasicMessageBean, JsonNode> ret_val_1 =
				test_context.emitObject(Optional.of(analytic_bucket_batch), job, Either.left((ObjectNode)_mapper.readTree("{\"test\":\"batch_succeed\"}")), Optional.empty());

		assertTrue("Should work: " + ret_val_1.validation(f -> f.message(), s -> s.toString()), ret_val_1.isSuccess());
		assertTrue(test_context._mutable_state.external_buckets.get(analytic_bucket_batch.full_name()).isLeft());

		// no files to start with (because the output hasn't been flushed)
		Thread.sleep(500L); //(safety)
		
		assertEquals(0, f_import.list().length);
		if (is_test) assertTrue(0 == f_tmp.list().length);
		else assertFalse(0 == f_tmp.list().length);
		
		test_context.flushBatchOutput(Optional.of(my_bucket), job);
		
		// now check again (after a "safety sleep")
		Thread.sleep(500L);

		assertEquals(0, f_tmp.list().length);
		if (is_test) assertTrue(0 == f_import.list().length);
		else assertFalse(0 == f_import.list().length);
	}
	
	//////////////////////////////////////////////////////////////
	
	//UTILS
	
	public void createDirs(File f, List<String> dirs) {
		dirs.stream().forEach(dir -> {
			try {
				FileUtils.forceMkdir(new File(f.toString() + "/" + dir));
				
				//DEBUG
				//System.out.println("CREATED " + new File(f.toString() + "/" + dir));
			}
			catch (Exception e) {
				//DEBUG
				//e.printStackTrace();
			}
		});
	}

}
