/*******************************************************************************
* Copyright 2015, The IKANOW Open Source Project.
* 
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License, version 3,
* as published by the Free Software Foundation.
* 
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Affero General Public License for more details.
* 
* You should have received a copy of the GNU Affero General Public License
* along with this program. If not, see <http://www.gnu.org/licenses/>.
******************************************************************************/
package com.ikanow.aleph2.analytics.services;

import static org.junit.Assert.*;

import java.io.File;
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.analytics.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

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
				res.stream().map(s -> s.substring(s.indexOf("/current/"))).collect(Collectors.toList())
				);
	}

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
