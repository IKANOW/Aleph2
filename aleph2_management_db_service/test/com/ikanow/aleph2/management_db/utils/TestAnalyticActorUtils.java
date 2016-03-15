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
package com.ikanow.aleph2.management_db.utils;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.Test;

import scala.Tuple3;

import com.codepoetics.protonpack.StreamUtils;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean.MasterEnrichmentType;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;

public class TestAnalyticActorUtils {

	@Test
	public void test_bucketSplitting() throws IOException {
		final String bucket_in_str = Resources.toString(Resources.getResource("com/ikanow/aleph2/management_db/utils/analytic_test_bucket.json"), Charsets.UTF_8);
		
		final DataBucketBean test_bucket = BeanTemplateUtils.from(bucket_in_str, DataBucketBean.class).get();
			
		// No diff
		{
			final Map<Tuple3<String, String, MasterEnrichmentType>, DataBucketBean> test_results = AnalyticActorUtils.splitAnalyticBuckets(test_bucket, Optional.empty());
			
			assertEquals(5, test_results.size());
			
			String[] res_names = { "StreamingEnrichmentService", "StreamingEnrichmentService", "StreamingEnrichmentService", "BatchEnrichmentService", "BatchEnrichmentService" };
			String[] res_epoints = { null, "test", null, null, "something_different" };
			MasterEnrichmentType[] res_types = { MasterEnrichmentType.streaming, MasterEnrichmentType.streaming, MasterEnrichmentType.streaming_and_batch, MasterEnrichmentType.streaming_and_batch, MasterEnrichmentType.batch };
			Integer[] res_numjobs = { 2, 1, 1, 2, 1 };
			
			StreamUtils.zipWithIndex(test_results.entrySet().stream())
				.forEach(i_bucket -> {
					final int ii = (int) i_bucket.getIndex();
					final DataBucketBean bucket = i_bucket.getValue().getValue();
					assertEquals("" + ii, Tuples._3T(res_names[ii], res_epoints[ii], res_types[ii]), i_bucket.getValue().getKey());
					assertEquals("" + ii, res_numjobs[ii].intValue(), bucket.analytic_thread().jobs().size());
					assertTrue("All have the right name: "+ bucket.analytic_thread().jobs().stream().map(j->j.name()).collect(Collectors.joining(";")), 
							bucket.analytic_thread().jobs().stream().allMatch(j -> j.name().endsWith("_group" + (1+ii))));
					assertEquals("All the same apart from jobs",
							BeanTemplateUtils.toJson(removeJobsFromBucket(test_bucket)).toString(),
							BeanTemplateUtils.toJson(removeJobsFromBucket(bucket)).toString()
							);
					//assertTrue(test_results."group_" + (1 + i_bucket.getIndex()));
				})
			;
		}
		// Performing a diff against the old bucket
		
		final DataBucketBean new_bucket = BeanTemplateUtils.clone(test_bucket)
											.with(DataBucketBean::analytic_thread,
													BeanTemplateUtils.clone(test_bucket.analytic_thread())
														.with(AnalyticThreadBean::jobs, Arrays.asList(test_bucket.analytic_thread().jobs().get(0)))
													.done()
													)
											.done();
		{
			final Map<Tuple3<String, String, MasterEnrichmentType>, DataBucketBean> test_results = AnalyticActorUtils.splitAnalyticBuckets(new_bucket, Optional.of(test_bucket));
			
			assertEquals("Failed? " + test_results, 5, test_results.size()); // 1x streaming enrichment + 4 empty lists
			assertFalse("First list has elements", test_results.values().iterator().next().analytic_thread().jobs().isEmpty());
			assertFalse("The other 4 don't", test_results.values().stream().skip(1).filter(b -> !b.analytic_thread().jobs().isEmpty()).findAny().isPresent());
		}
	}
	
	private DataBucketBean removeJobsFromBucket(DataBucketBean in) {
		return BeanTemplateUtils.clone(in)
				.with(DataBucketBean::analytic_thread, 
						BeanTemplateUtils.clone(in.analytic_thread())
							.with(AnalyticThreadBean::jobs, null)
						.done()
						)
				.done();
	}
	
}
