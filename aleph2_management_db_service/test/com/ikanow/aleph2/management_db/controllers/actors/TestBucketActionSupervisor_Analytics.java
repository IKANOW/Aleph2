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
		
		assertTrue(BucketActionSupervisor.bucketHasStreamingAnalytics(base_bucket));
		
		// Check some true negative cases:
		{
			DataBucketBean enrich_bucket = BeanTemplateUtils.clone(base_bucket).with(DataBucketBean::master_enrichment_type, MasterEnrichmentType.streaming).done();
			assertFalse(BucketActionSupervisor.bucketHasStreamingAnalytics(enrich_bucket));
		}
		{
			DataBucketBean nothing_bucket = BeanTemplateUtils.clone(base_bucket).with(DataBucketBean::analytic_thread, null).done();
			assertFalse(BucketActionSupervisor.bucketHasStreamingAnalytics(nothing_bucket));
		}
		// Remove all the streaming jobs
		{
			DataBucketBean no_streaming_bucket = BeanTemplateUtils.clone(base_bucket)
													.with(DataBucketBean::analytic_thread, 
															BeanTemplateUtils.clone(base_bucket.analytic_thread())
																.with(AnalyticThreadBean::jobs,
																		base_bucket.analytic_thread().jobs().stream()
																			.filter(job -> job.analytic_type() != MasterEnrichmentType.streaming_and_batch 
																						&& job.analytic_type() != MasterEnrichmentType.streaming)
																			.collect(Collectors.toList())
																		)
															.done()
															)
												.done();
			assertFalse(BucketActionSupervisor.bucketHasStreamingAnalytics(no_streaming_bucket));
			
		}
	}
}
