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
******************************************************************************/
package com.ikanow.aleph2.data_import_manager.stream_enrichment.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import backtype.storm.generated.TopologyInfo;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_import.context.stream_enrichment.utils.ErrorUtils;
import com.ikanow.aleph2.data_import.services.StreamingEnrichmentContext;
import com.ikanow.aleph2.data_import_manager.stream_enrichment.services.IStormController;
import com.ikanow.aleph2.data_import_manager.stream_enrichment.storm_samples.SampleStormStreamTopology1;
import com.ikanow.aleph2.data_import_manager.stream_enrichment.utils.StormControllerUtil;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentStreamingTopology;
import com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class TestStormControllerUtil {
	static final Logger _logger = LogManager.getLogger(); 
	private static IStormController storm_cluster;
	protected Injector _app_injector;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		storm_cluster = StormControllerUtil.getLocalStormController();
	}
	
	@Before
	public void injectModules() throws Exception {
		final Config config = ConfigFactory.parseFile(new File("./example_config_files/context_local_test.properties"));
		
		try {
			_app_injector = ModuleUtils.createInjector(Arrays.asList(), Optional.of(config));
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
	public void test_createTopologyName() {
		assertEquals("path-to-bucket__", StormControllerUtil.bucketPathToTopologyName("/path/to/bucket"));
		assertEquals("path-to-bucket-more__", StormControllerUtil.bucketPathToTopologyName("/path/to/bucket/more"));
		assertEquals("path-_----more__", StormControllerUtil.bucketPathToTopologyName("/path/+-__////more"));
	}
	
	/**
	 * Tests that caching doesn't cause jobs to fail because they restart too fast
	 * https://github.com/IKANOW/Aleph2/issues/26
	 * @throws Exception 
	 */
	@Test
	public void testQuickCache() throws Exception {
		//Submit a job, causes jar to cache
		final DataBucketBean bucket = createBucket();
		final SharedLibraryBean library = BeanTemplateUtils.build(SharedLibraryBean.class)
				.with(SharedLibraryBean::path_name, "/test/lib")
				.done().get();
		final StreamingEnrichmentContext context = _app_injector.getInstance(StreamingEnrichmentContext.class);	
		context.setBucket(bucket);
		context.setLibraryConfig(library);			
		context.setUserTopologyEntryPoint("com.ikanow.aleph2.data_import_manager.stream_enrichment.storm_samples.SampleStormStreamTopology1");
		context.getEnrichmentContextSignature(Optional.empty(), Optional.empty());
		context.overrideSavedContext(); // (THIS IS NEEDED WHEN TESTING THE KAFKA SPOUT)
		final IEnrichmentStreamingTopology enrichment_topology = new SampleStormStreamTopology1();
		final String cached_jar_dir = System.getProperty("java.io.tmpdir");
		final ISearchIndexService index_service = context.getServiceContext().getService(ISearchIndexService.class, Optional.empty()).get();
		final ICrudService<JsonNode> crud_service = index_service.getCrudService(JsonNode.class, bucket).get();
		crud_service.deleteDatastore().get();
		StormControllerUtil.startJob(storm_cluster, bucket, context, new ArrayList<String>(), enrichment_topology, cached_jar_dir);
		
		//debug only, let's the job finish
		//Thread.sleep(5000);
		final TopologyInfo info = StormControllerUtil.getJobStats(storm_cluster, StormControllerUtil.bucketPathToTopologyName(bucket.full_name()));
		_logger.debug("Status is: " + info.get_status());
		assertTrue(info.get_status().equals("ACTIVE"));
		
		//Restart same job (should use cached jar)
		StormControllerUtil.restartJob(storm_cluster, bucket, context, new ArrayList<String>(), enrichment_topology, cached_jar_dir);
		
		final TopologyInfo info1 = StormControllerUtil.getJobStats(storm_cluster, StormControllerUtil.bucketPathToTopologyName(bucket.full_name()));
		_logger.debug("Status is: " + info.get_status());
		assertTrue(info1.get_status().equals("ACTIVE"));	
	}

	protected DataBucketBean createBucket() {		
		return BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::_id, "test_quickcache")
				.with(DataBucketBean::modified, new Date())
				.with(DataBucketBean::full_name, "/test/quickcache")
				.with("data_schema", BeanTemplateUtils.build(DataSchemaBean.class)
						.with("search_index_schema", BeanTemplateUtils.build(DataSchemaBean.SearchIndexSchemaBean.class)
								.done().get())
						.done().get())
				.done().get();
	}
	
	
	
}
