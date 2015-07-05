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
package com.ikanow.aleph2.data_import.streaming_enrichment.storm;

import java.io.File;
import java.util.Arrays;
import java.util.Date;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import com.google.inject.Injector;
import com.ikanow.aleph2.data_import.context.stream_enrichment.utils.ErrorUtils;
import com.ikanow.aleph2.data_import.services.StreamingEnrichmentContext;
import com.ikanow.aleph2.data_import.stream_enrichment.storm.PassthroughTopology;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.distributed_services.utils.KafkaUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;

public class TestPassthroughTopology {

	LocalCluster _local_cluster;
	
	protected Injector _app_injector;
	
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
		_local_cluster = new LocalCluster();
	}
	
	@Test
	public void test_passthroughTopology() throws InterruptedException {
		// PHASE 1: GET AN IN-TECHNOLOGY CONTEXT
		// Bucket
		final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::_id, "test_passthroughtopology")
				.with(DataBucketBean::modified, new Date())
				.with(DataBucketBean::full_name, "/test/passthrough")
				.with("data_schema", BeanTemplateUtils.build(DataSchemaBean.class)
						.with("search_index_schema", BeanTemplateUtils.build(DataSchemaBean.SearchIndexSchemaBean.class)
								.done().get())
						.done().get())
				.done().get();
		
		// Context		
		final StreamingEnrichmentContext test_context = _app_injector.getInstance(StreamingEnrichmentContext.class);
		test_context.setBucket(test_bucket);
		test_context.setUserTopologyEntryPoint("com.ikanow.aleph2.data_import.stream_enrichment.storm.PassthroughTopology");
		test_context.getEnrichmentContextSignature(Optional.empty(), Optional.empty());
		
		//PHASE 2: CREATE TOPOLOGY AND SUBMit		
		final ICoreDistributedServices cds = test_context.getService(ICoreDistributedServices.class, Optional.empty()).get();
		KafkaUtils.createTopic(KafkaUtils.bucketPathToTopicName(test_bucket.full_name()));
		final StormTopology topology = (StormTopology) new PassthroughTopology()
											.getTopologyAndConfiguration(test_bucket, test_context)
											._1();
		
		final backtype.storm.Config config = new backtype.storm.Config();
		config.setDebug(true);
		_local_cluster.submitTopology("test_passthroughTopology", config, topology);		
		Thread.sleep(5000L);
		
		//PHASE3 : WRITE TO KAFKA
		cds.produce(KafkaUtils.bucketPathToTopicName(test_bucket.full_name()), "{\"test\":\"test1\"}");
		
		//TODO wait
		
		//TODO check they appear in the appropriate CRUD service
		
	}
	
}
