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
package com.ikanow.aleph2.management_db.controllers.actors;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.stream.Collectors;

import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean.MasterEnrichmentType;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;

public class TestBucketActionSupervisor_Analytics {

	// Simple test on some utility code
	
	@Test
	public void test_hasStreamingEnrichment() throws IOException {
		
		final String bucket_in_str = Resources.toString(Resources.getResource("com/ikanow/aleph2/management_db/utils/analytic_test_bucket.json"), Charsets.UTF_8);
		
		DataBucketBean base_bucket = BeanTemplateUtils.from(bucket_in_str, DataBucketBean.class).get();
		
		assertTrue(BucketActionSupervisor.bucketHasAnalytics(base_bucket));
		
		// Check some true negative cases:
		{
			DataBucketBean enrich_bucket = BeanTemplateUtils.clone(base_bucket).with(DataBucketBean::master_enrichment_type, MasterEnrichmentType.streaming).done();
			assertFalse(BucketActionSupervisor.bucketHasAnalytics(enrich_bucket));
		}
		{
			DataBucketBean nothing_bucket = BeanTemplateUtils.clone(base_bucket).with(DataBucketBean::analytic_thread, null).done();
			assertFalse(BucketActionSupervisor.bucketHasAnalytics(nothing_bucket));
		}
		// Remove all the streaming jobs
		{
			DataBucketBean no_streaming_bucket = BeanTemplateUtils.clone(base_bucket)
													.with(DataBucketBean::analytic_thread, 
															BeanTemplateUtils.clone(base_bucket.analytic_thread())
																.with(AnalyticThreadBean::jobs,
																		base_bucket.analytic_thread().jobs().stream()
																			.filter(job -> job.analytic_type() != MasterEnrichmentType.streaming_and_batch 
																						&& job.analytic_type() != MasterEnrichmentType.batch
																						&& job.analytic_type() != MasterEnrichmentType.streaming)
																			.collect(Collectors.toList())
																		)
															.done()
															)
												.done();
			assertFalse(BucketActionSupervisor.bucketHasAnalytics(no_streaming_bucket));
			
		}
	}
}
