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
package com.ikanow.aleph2.logging.service;

import static org.junit.Assert.*;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IBucketLogger;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.ManagementSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.SearchIndexSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.ManagementSchemaBean.LoggingSchemaBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.BucketUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.logging.data_model.LoggingServiceConfigBean;
import com.ikanow.aleph2.logging.module.LoggingServiceModule;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class TestLoggingService {
	private static final Logger _logger = LogManager.getLogger();
	private static ISearchIndexService search_index_service;
	private static LoggingService logging_service;
	protected ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());
	protected Injector _app_injector;
	
	// All the services
	@Inject IServiceContext _service_context;
	@Inject LoggingServiceConfigBean _config;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}
	
	public void getServices() {		
		_logger.info("run injectModules");		
		final File config_file = new File("./resources/context_local_test.properties");
		final Config config = ConfigFactory.parseFile(config_file);
		
		try {
			_app_injector = ModuleUtils.createTestInjector(Arrays.asList(new LoggingServiceModule()), Optional.of(config));
		}
		catch (Exception e) {
			try {
				e.printStackTrace();
			}
			catch (Exception ee) {
				System.out.println(ErrorUtils.getLongForm("{0}", e));
			}
		}
		
		
		_app_injector.injectMembers(this);
		search_index_service = _service_context.getSearchIndexService().get();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		getServices();		
		logging_service = new LoggingService(_config, _service_context);
	}

	@After
	public void tearDown() throws Exception {
	}

	/**
	 * Tests writing messages as user, system, external and checks all the messages were stored.
	 * 
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	@Test
	public void testLogBucket() throws InterruptedException, ExecutionException {
		final String subsystem_name = "logging_test1";
		final long num_messages_to_log = 50;
		final DataBucketBean test_bucket = getTestBucket("test1", Level.ALL); 
		final IBucketLogger user_logger = logging_service.getLogger(test_bucket);
		final IBucketLogger system_logger = logging_service.getSystemLogger(test_bucket);
		final IBucketLogger external_logger = logging_service.getExternalLogger(subsystem_name);
		//log a few messages
		for ( int i = 0; i < num_messages_to_log; i++ ) {
			user_logger.log(Level.ERROR, ErrorUtils.buildMessage(true, subsystem_name, "test_message " + i, "no error")).get();
			system_logger.log(Level.ERROR, ErrorUtils.buildMessage(true, subsystem_name, "test_message " + i, "no error")).get();
			external_logger.log(Level.ERROR, ErrorUtils.buildMessage(true, subsystem_name, "test_message " + i, "no error")).get();
		}
		
		//check its in ES, wait 10s max for the index to refresh
		final DataBucketBean logging_test_bucket = BucketUtils.convertDataBucketBeanToLogging(test_bucket);
		final IDataWriteService<BasicMessageBean> logging_crud = search_index_service.getDataService().get().getWritableDataService(BasicMessageBean.class, logging_test_bucket, Optional.empty(), Optional.empty()).get();
		waitForResults(logging_crud, 10);
		assertEquals(num_messages_to_log*2, logging_crud.countObjects().get().longValue());
		
		final DataBucketBean logging_external_test_bucket = BucketUtils.convertDataBucketBeanToLogging(BeanTemplateUtils.clone(test_bucket).with(DataBucketBean::full_name, "/external/"+ subsystem_name+"/").done());
		final IDataWriteService<BasicMessageBean> logging_crud_external = search_index_service.getDataService().get().getWritableDataService(BasicMessageBean.class, logging_external_test_bucket, Optional.empty(), Optional.empty()).get();
		waitForResults(logging_crud_external, 10);
		assertEquals(num_messages_to_log, logging_crud_external.countObjects().get().longValue());

		//cleanup
		logging_crud.deleteDatastore().get();
	}
	
	/**
	 * Tests writing messages as user, system, external at 3 different log levels and verifies
	 * the too low of level messages were filtered out (not written to storage).
	 * 
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	@Test
	public void testLogFilter() throws InterruptedException, ExecutionException {
		final String subsystem_name = "logging_test2";
		final long num_messages_to_log_each_type = 5;
		final List<Level> levels = Arrays.asList(Level.DEBUG, Level.INFO, Level.ERROR);
		final DataBucketBean test_bucket = getTestBucket("test2", Level.ERROR); 
		final IBucketLogger user_logger = logging_service.getLogger(test_bucket);
		final IBucketLogger system_logger = logging_service.getSystemLogger(test_bucket);
		final IBucketLogger external_logger = logging_service.getExternalLogger(subsystem_name);
		//log a few messages
		for ( int i = 0; i < num_messages_to_log_each_type; i++ ) {
			for ( Level level : levels) {
				user_logger.log(level, ErrorUtils.buildMessage(true, subsystem_name, "test_message " + i, "no error")).get();
				system_logger.log(level, ErrorUtils.buildMessage(true, subsystem_name, "test_message " + i, "no error")).get();
				external_logger.log(level, ErrorUtils.buildMessage(true, subsystem_name, "test_message " + i, "no error")).get();
			}
		}
		
		//check its in ES, wait 10s max for the index to refresh
		final DataBucketBean logging_test_bucket = BucketUtils.convertDataBucketBeanToLogging(test_bucket);
		final IDataWriteService<BasicMessageBean> logging_crud = search_index_service.getDataService().get().getWritableDataService(BasicMessageBean.class, logging_test_bucket, Optional.empty(), Optional.empty()).get();
		waitForResults(logging_crud, 10);
		assertEquals(10, logging_crud.countObjects().get().longValue()); //should only have logged ERROR messages

		final DataBucketBean logging_external_test_bucket = BucketUtils.convertDataBucketBeanToLogging(BeanTemplateUtils.clone(test_bucket).with(DataBucketBean::full_name, "/external/"+ subsystem_name+"/").done());
		final IDataWriteService<BasicMessageBean> logging_crud_external = search_index_service.getDataService().get().getWritableDataService(BasicMessageBean.class, logging_external_test_bucket, Optional.empty(), Optional.empty()).get();
		waitForResults(logging_crud_external, 10);
		assertEquals(15, logging_crud_external.countObjects().get().longValue());
		
		//cleanup
		logging_crud.deleteDatastore().get();
	}
	
	/**
	 * Tests and empty management schema falls back to defaults and filters items by the
	 * defaults used in the config file.
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	@Test
	public void testLogEmptyManagement() throws InterruptedException, ExecutionException {
		//if no logging schema is supplied, falls back to defaults in config file (if any)
		//config file is set to:
		//SYSTEM: DEBUG
		//USER: ERROR
		//therefore we should see 
		//5 messages of each type to make it through via the system calls (15)
		//5 messages of DEBUG making it through via user calls (5)
		//5 messages of each type to make it through via the external calls (15)
		final String subsystem_name = "logging_test3";
		final long num_messages_to_log_each_type = 5;
		final List<Level> levels = Arrays.asList(Level.DEBUG, Level.INFO, Level.ERROR);
		final DataBucketBean test_bucket = getEmptyTestBucket("test3"); 
		final IBucketLogger user_logger = logging_service.getLogger(test_bucket);
		final IBucketLogger system_logger = logging_service.getSystemLogger(test_bucket);
		final IBucketLogger external_logger = logging_service.getExternalLogger(subsystem_name);
		//log a few messages
		for ( int i = 0; i < num_messages_to_log_each_type; i++ ) {
			for ( Level level : levels) {
				user_logger.log(level, ErrorUtils.buildMessage(true, subsystem_name, "test_message " + i, "no error")).get();
				system_logger.log(level, ErrorUtils.buildMessage(true, subsystem_name, "test_message " + i, "no error")).get();
				external_logger.log(level, ErrorUtils.buildMessage(true, subsystem_name, "test_message " + i, "no error")).get();
			}
		}
		
		//check its in ES, wait 10s max for the index to refresh
		//USER + SYSTEM
		final DataBucketBean logging_test_bucket = BucketUtils.convertDataBucketBeanToLogging(test_bucket);
		final IDataWriteService<BasicMessageBean> logging_crud = search_index_service.getDataService().get().getWritableDataService(BasicMessageBean.class, logging_test_bucket, Optional.empty(), Optional.empty()).get();
		waitForResults(logging_crud, 10);
		assertEquals(20, logging_crud.countObjects().get().longValue()); //should only have logged ERROR messages

		//EXTERNAL
		final DataBucketBean logging_external_test_bucket = BucketUtils.convertDataBucketBeanToLogging(BeanTemplateUtils.clone(test_bucket).with(DataBucketBean::full_name, "/external/"+ subsystem_name+"/").done());
		final IDataWriteService<BasicMessageBean> logging_crud_external = search_index_service.getDataService().get().getWritableDataService(BasicMessageBean.class, logging_external_test_bucket, Optional.empty(), Optional.empty()).get();
		waitForResults(logging_crud_external, 10);
		assertEquals(15, logging_crud_external.countObjects().get().longValue());
		
		//cleanup
		logging_crud.deleteDatastore().get();
	}
	
	/**
	 * Waits for the crud service count objects to return some amount of objects w/in the given
	 * timeframe, returns as soon as we find any results.  Useful for waiting for ES to flush/update the index. 
	 * 
	 * @param crud_service
	 * @param max_wait_time_s
	 */
	private static void waitForResults(final IDataWriteService<?> crud_service, final long max_wait_time_s) {
		for (int ii = 0; ii < max_wait_time_s; ++ii) {
			try { Thread.sleep(1000L); } catch (Exception e) {}
			if (crud_service.countObjects().join().intValue() > 0) break;
		}
	}

	/**
	 * Creates a sample bucket with search index enabled and the given full_name located at /test/logtest/<name>/
	 * 
	 * @param name
	 * @param min_log_level 
	 * @return
	 */
	private DataBucketBean getTestBucket(final String name, Level min_log_level) {
		return BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::full_name, "/test/logtest/" + name + "/")
				.with(DataBucketBean::data_schema, BeanTemplateUtils.build(DataSchemaBean.class)
						.with(DataSchemaBean::search_index_schema, BeanTemplateUtils.build(SearchIndexSchemaBean.class)
								.with(SearchIndexSchemaBean::enabled, true)
								.done().get())
						.done().get())	
				.with(DataBucketBean::management_schema, BeanTemplateUtils.build(ManagementSchemaBean.class)
						.with(ManagementSchemaBean::logging_schema, BeanTemplateUtils.build(LoggingSchemaBean.class)
								.with(LoggingSchemaBean::log_level, min_log_level)
								.done().get())
						.done().get())
				.done().get();
	}
	
	/**
	 * Creates a sample bucket without a mangement schema.
	 * 
	 * @param name
	 * @return
	 */
	private DataBucketBean getEmptyTestBucket(final String name) {
		return BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::full_name, "/test/logtest/" + name + "/")
				.with(DataBucketBean::data_schema, BeanTemplateUtils.build(DataSchemaBean.class)
						.with(DataSchemaBean::search_index_schema, BeanTemplateUtils.build(SearchIndexSchemaBean.class)
								.with(SearchIndexSchemaBean::enabled, true)
								.done().get())
						.done().get())					
				.done().get();
	}
}
