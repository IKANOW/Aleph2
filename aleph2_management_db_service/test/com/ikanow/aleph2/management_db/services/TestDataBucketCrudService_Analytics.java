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
package com.ikanow.aleph2.management_db.services;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.junit.Test;

import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean.AnalyticThreadJobInputBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean.AnalyticThreadJobInputConfigBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean.AnalyticThreadJobOutputBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean.AnalyticThreadComplexTriggerBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean.AnalyticThreadComplexTriggerBean.TriggerOperator;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean.AnalyticThreadComplexTriggerBean.TriggerType;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.HarvestControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean.MasterEnrichmentType;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.management_db.utils.BucketValidationUtils;

public class TestDataBucketCrudService_Analytics {

	
	@Test
	public void test_basicAnalyticsValidation() {

		// (Base check:)		
		List<String> res0 = BucketValidationUtils.validateAnalyticBucket(getBaseBucket("/tb0", null, null));
		assertTrue("Simple bucket validates: " + res0.stream().collect(Collectors.joining(";")), res0.isEmpty());

		// 1) Combined enrichment and analytics
		
		{
			final DataBucketBean tb = BeanTemplateUtils.clone(getBaseBucket("/tb1", null, null))
										.with(DataBucketBean::master_enrichment_type, MasterEnrichmentType.streaming)
										.done();

			List<BasicMessageBean> res = BucketValidationUtils.staticValidation(tb, false);
			assertEquals("Wrong number of errors: " + res.stream().map(b->b.message()).collect(Collectors.joining(";")), 2, res.size());
		}		
		{
			final DataBucketBean tb = BeanTemplateUtils.clone(getBaseBucket("/tb1b", null, null))
										.with(DataBucketBean::master_enrichment_type, MasterEnrichmentType.none)
										.done();

			List<BasicMessageBean> res = BucketValidationUtils.staticValidation(tb, false);
			assertEquals(0, res.size());
		}		
		
		// 2) Jobs

		// Job should have valid name		

		{
			final AnalyticThreadJobBean job = getBaseJob("/invalid/name", null, null, null);

			final DataBucketBean tb = BeanTemplateUtils.clone(getBaseBucket("/tb2a", job, null))
					.done();

			List<BasicMessageBean> res = BucketValidationUtils.staticValidation(tb, false);
			assertEquals(1, res.size());
		}		

		// Jobs should have a a valid analytic_technology_name_or_id

		{
			final AnalyticThreadJobBean job = BeanTemplateUtils.clone(getBaseJob("name", null, null, null))
					.with(AnalyticThreadJobBean::analytic_technology_name_or_id, null)
					.done();

			final DataBucketBean tb = BeanTemplateUtils.clone(getBaseBucket("/tb2a", job, null))
					.done();

			List<BasicMessageBean> res = BucketValidationUtils.staticValidation(tb, false);
			System.out.println("validation errs = " + res.stream().map(m->m.message()).collect(Collectors.joining(" ; ")));
			assertEquals(1, res.size());
		}		

		// Jobs should have a valid analytic type

		{
			final AnalyticThreadJobBean job = BeanTemplateUtils.clone(getBaseJob("name", null, null, null))
					.with(AnalyticThreadJobBean::analytic_type, null)
					.done();

			final DataBucketBean tb = BeanTemplateUtils.clone(getBaseBucket("/tb2a", job, null))
					.done();

			List<BasicMessageBean> res = BucketValidationUtils.staticValidation(tb, false);
			System.out.println("validation errs = " + res.stream().map(m->m.message()).collect(Collectors.joining(" ; ")));
			assertEquals(1, res.size());
		}		
		{
			final AnalyticThreadJobBean job = BeanTemplateUtils.clone(getBaseJob("name", null, null, null))
					.with(AnalyticThreadJobBean::analytic_type, MasterEnrichmentType.none)
					.done();

			final DataBucketBean tb = BeanTemplateUtils.clone(getBaseBucket("/tb2a", job, null))
					.done();

			List<BasicMessageBean> res = BucketValidationUtils.staticValidation(tb, false);
			System.out.println("validation errs = " + res.stream().map(m->m.message()).collect(Collectors.joining(" ; ")));
			assertEquals(1, res.size());
		}		
		{
			final AnalyticThreadJobBean job = BeanTemplateUtils.clone(getBaseJob("name", null, null, null))
					.with(AnalyticThreadJobBean::analytic_type, MasterEnrichmentType.streaming_and_batch)
					.done();

			final DataBucketBean tb = BeanTemplateUtils.clone(getBaseBucket("/tb2a", job, null))
					.done();

			List<BasicMessageBean> res = BucketValidationUtils.staticValidation(tb, false);
			System.out.println("validation errs = " + res.stream().map(m->m.message()).collect(Collectors.joining(" ; ")));
			assertEquals(1, res.size());			
		}

		// 3) Inputs

		// Valid data service
		{
			final AnalyticThreadJobInputBean input = BeanTemplateUtils.clone(getBaseInput(null))
					.with(AnalyticThreadJobInputBean::data_service, null)
					.done();

			final AnalyticThreadJobBean job = BeanTemplateUtils.clone(getBaseJob("name", input, null, null))
					.done();

			final DataBucketBean tb = BeanTemplateUtils.clone(getBaseBucket("/tb3a", job, null))
					.done();

			List<BasicMessageBean> res = BucketValidationUtils.staticValidation(tb, false);
			System.out.println("validation errs = " + res.stream().map(m->m.message()).collect(Collectors.joining(" ; ")));
			assertEquals(1, res.size());
		}
		// random
		{
			final AnalyticThreadJobInputBean input = BeanTemplateUtils.clone(getBaseInput(null))
					.with(AnalyticThreadJobInputBean::data_service, "banana")
					.done();

			final AnalyticThreadJobBean job = BeanTemplateUtils.clone(getBaseJob("name", input, null, null))
					.done();

			final DataBucketBean tb = BeanTemplateUtils.clone(getBaseBucket("/tb3b", job, null))
					.done();

			List<BasicMessageBean> res = BucketValidationUtils.staticValidation(tb, false);
			System.out.println("validation errs = " + res.stream().map(m->m.message()).collect(Collectors.joining(" ; ")));
			assertEquals(1, res.size());
		}		
		// batch/streaming with alternate
		{
			final AnalyticThreadJobInputBean input = BeanTemplateUtils.clone(getBaseInput(null))
					.with(AnalyticThreadJobInputBean::data_service, "streaming.alternate")
					.done();

			final AnalyticThreadJobBean job = BeanTemplateUtils.clone(getBaseJob("name", input, null, null))
					.done();

			final DataBucketBean tb = BeanTemplateUtils.clone(getBaseBucket("/tb3b", job, null))
					.done();

			List<BasicMessageBean> res = BucketValidationUtils.staticValidation(tb, false);
			System.out.println("validation errs = " + res.stream().map(m->m.message()).collect(Collectors.joining(" ; ")));
			assertEquals(1, res.size());
		}		
		//(check some non-default valid ones)
		{
			final AnalyticThreadJobInputBean input = BeanTemplateUtils.clone(getBaseInput(null))
					.with(AnalyticThreadJobInputBean::data_service, "search_index_service")
					.done();

			final AnalyticThreadJobBean job = BeanTemplateUtils.clone(getBaseJob("name", input, null, null))
					.done();

			final DataBucketBean tb = BeanTemplateUtils.clone(getBaseBucket("/tb3c", job, null))
					.done();

			List<BasicMessageBean> res = BucketValidationUtils.staticValidation(tb, false);
			System.out.println("validation errs = " + res.stream().map(m->m.message()).collect(Collectors.joining(" ; ")));
			assertEquals(0, res.size());
		}		
		{
			final AnalyticThreadJobInputBean input = BeanTemplateUtils.clone(getBaseInput(null))
					.with(AnalyticThreadJobInputBean::data_service, "document_service.alternate")
					.done();

			final AnalyticThreadJobBean job = BeanTemplateUtils.clone(getBaseJob("name", input, null, null))
					.done();

			final DataBucketBean tb = BeanTemplateUtils.clone(getBaseBucket("/tb3c", job, null))
					.done();

			List<BasicMessageBean> res = BucketValidationUtils.staticValidation(tb, false);
			System.out.println("validation errs = " + res.stream().map(m->m.message()).collect(Collectors.joining(" ; ")));
			assertEquals(0, res.size());
		}		
		{
			final AnalyticThreadJobInputBean input = BeanTemplateUtils.clone(getBaseInput(null))
					.with(AnalyticThreadJobInputBean::data_service, "storage_service")
					.done();

			final AnalyticThreadJobBean job = BeanTemplateUtils.clone(getBaseJob("name", input, null, null))
					.done();

			final DataBucketBean tb = BeanTemplateUtils.clone(getBaseBucket("/tb3d", job, null))
					.done();

			List<BasicMessageBean> res = BucketValidationUtils.staticValidation(tb, false);
			System.out.println("validation errs = " + res.stream().map(m->m.message()).collect(Collectors.joining(" ; ")));
			assertEquals(0, res.size());
		}		
		{
			final AnalyticThreadJobInputBean input = BeanTemplateUtils.clone(getBaseInput(null))
					.with(AnalyticThreadJobInputBean::data_service, "batch")
					.done();

			final AnalyticThreadJobBean job = BeanTemplateUtils.clone(getBaseJob("name", input, null, null))
					.done();

			final DataBucketBean tb = BeanTemplateUtils.clone(getBaseBucket("/tb3e", job, null))
					.done();

			List<BasicMessageBean> res = BucketValidationUtils.staticValidation(tb, false);
			System.out.println("validation errs = " + res.stream().map(m->m.message()).collect(Collectors.joining(" ; ")));
			assertEquals(0, res.size());
		}		
		
		// Valid resource name
		// (invalid ones)
		{
			final AnalyticThreadJobInputBean input = BeanTemplateUtils.clone(getBaseInput(null))
					.with(AnalyticThreadJobInputBean::resource_name_or_id, null)
					.done();

			final AnalyticThreadJobBean job = BeanTemplateUtils.clone(getBaseJob("name", input, null, null))
					.done();

			final DataBucketBean tb = BeanTemplateUtils.clone(getBaseBucket("/tb3f", job, null))
					.done();

			List<BasicMessageBean> res = BucketValidationUtils.staticValidation(tb, false);
			System.out.println("validation errs = " + res.stream().map(m->m.message()).collect(Collectors.joining(" ; ")));
			assertEquals(1, res.size());
		}		
		{
			final AnalyticThreadJobInputBean input = BeanTemplateUtils.clone(getBaseInput(null))
					.with(AnalyticThreadJobInputBean::resource_name_or_id, "::")
					.done();

			final AnalyticThreadJobBean job = BeanTemplateUtils.clone(getBaseJob("name", input, null, null))
					.done();

			final DataBucketBean tb = BeanTemplateUtils.clone(getBaseBucket("/tb3g", job, null))
					.done();

			List<BasicMessageBean> res = BucketValidationUtils.staticValidation(tb, false);
			System.out.println("validation errs = " + res.stream().map(m->m.message()).collect(Collectors.joining(" ; ")));
			assertEquals(1, res.size());
		}		
		//(check some valid ones)
		{
			final AnalyticThreadJobInputBean input = BeanTemplateUtils.clone(getBaseInput(null))
					.with(AnalyticThreadJobInputBean::resource_name_or_id, "test1")
					.done();

			final AnalyticThreadJobBean job = BeanTemplateUtils.clone(getBaseJob("name", input, null, null))
					.done();

			final DataBucketBean tb = BeanTemplateUtils.clone(getBaseBucket("/tb3h", job, null))
					.done();

			List<BasicMessageBean> res = BucketValidationUtils.staticValidation(tb, false);
			System.out.println("validation errs = " + res.stream().map(m->m.message()).collect(Collectors.joining(" ; ")));
			assertEquals(0, res.size());
		}		
		{
			final AnalyticThreadJobInputBean input = BeanTemplateUtils.clone(getBaseInput(null))
					.with(AnalyticThreadJobInputBean::resource_name_or_id, ":test1")
					.done();

			final AnalyticThreadJobBean job = BeanTemplateUtils.clone(getBaseJob("name", input, null, null))
					.done();

			final DataBucketBean tb = BeanTemplateUtils.clone(getBaseBucket("/tb3i", job, null))
					.done();

			List<BasicMessageBean> res = BucketValidationUtils.staticValidation(tb, false);
			System.out.println("validation errs = " + res.stream().map(m->m.message()).collect(Collectors.joining(" ; ")));
			assertEquals(0, res.size());
		}		
		{
			final AnalyticThreadJobInputBean input = BeanTemplateUtils.clone(getBaseInput(null))
					.with(AnalyticThreadJobInputBean::resource_name_or_id, "/valid:test1")
					.done();

			final AnalyticThreadJobBean job = BeanTemplateUtils.clone(getBaseJob("name", input, null, null))
					.done();

			final DataBucketBean tb = BeanTemplateUtils.clone(getBaseBucket("/tb3j", job, null))
					.done();

			List<BasicMessageBean> res = BucketValidationUtils.staticValidation(tb, false);
			System.out.println("validation errs = " + res.stream().map(m->m.message()).collect(Collectors.joining(" ; ")));
			assertEquals(0, res.size());
		}		
		{
			final AnalyticThreadJobInputBean input = BeanTemplateUtils.clone(getBaseInput(null))
					.with(AnalyticThreadJobInputBean::resource_name_or_id, "/valid")
					.done();

			final AnalyticThreadJobBean job = BeanTemplateUtils.clone(getBaseJob("name", input, null, null))
					.done();

			final DataBucketBean tb = BeanTemplateUtils.clone(getBaseBucket("/tb3k", job, null))
					.done();

			List<BasicMessageBean> res = BucketValidationUtils.staticValidation(tb, false);
			System.out.println("validation errs = " + res.stream().map(m->m.message()).collect(Collectors.joining(" ; ")));
			assertEquals(0, res.size());
		}		
		
		// Valid times:
		// (invalid)
		{
			final AnalyticThreadJobInputConfigBean in_cfg = getBaseInputConfig("rabbit", "banana");
			
			final AnalyticThreadJobInputBean input = BeanTemplateUtils.clone(getBaseInput(in_cfg))
					.done();

			final AnalyticThreadJobBean job = BeanTemplateUtils.clone(getBaseJob("name", input, null, null))
					.done();

			final DataBucketBean tb = BeanTemplateUtils.clone(getBaseBucket("/tb3l", job, null))
					.done();

			List<BasicMessageBean> res = BucketValidationUtils.staticValidation(tb, false);
			System.out.println("validation errs = " + res.stream().map(m->m.message()).collect(Collectors.joining(" ; ")));
			assertEquals(2, res.size());
			
		}
		// (invalid pulled from global)
		{
			final AnalyticThreadJobInputConfigBean in_cfg = getBaseInputConfig("rabbit", "banana");
			
			final AnalyticThreadJobInputBean input = BeanTemplateUtils.clone(getBaseInput(null))
					.done();

			final AnalyticThreadJobBean job = BeanTemplateUtils.clone(getBaseJob("name", input, null, in_cfg))
					.done();

			final DataBucketBean tb = BeanTemplateUtils.clone(getBaseBucket("/tb3m", job, null))
					.done();

			List<BasicMessageBean> res = BucketValidationUtils.staticValidation(tb, false);
			System.out.println("validation errs = " + res.stream().map(m->m.message()).collect(Collectors.joining(" ; ")));
			assertEquals(2, res.size());
			
		}
		// (some valid)
		{
			final AnalyticThreadJobInputConfigBean in_cfg = getBaseInputConfig("1 week ago", "now");
			
			final AnalyticThreadJobInputBean input = BeanTemplateUtils.clone(getBaseInput(in_cfg))
					.done();

			final AnalyticThreadJobBean job = BeanTemplateUtils.clone(getBaseJob("name", input, null, null))
					.done();

			final DataBucketBean tb = BeanTemplateUtils.clone(getBaseBucket("/tb3n", job, null))
					.done();

			List<BasicMessageBean> res = BucketValidationUtils.staticValidation(tb, false);
			System.out.println("validation errs = " + res.stream().map(m->m.message()).collect(Collectors.joining(" ; ")));
			assertEquals(0, res.size());			
		}
		
		// 4) Output
		
		//invalid
		{
			final AnalyticThreadJobOutputBean output = getBaseOutput(true, null, "invalid");
			
			final AnalyticThreadJobBean job = BeanTemplateUtils.clone(getBaseJob("name", null, output, null))
					.done();
			
			final DataBucketBean tb = BeanTemplateUtils.clone(getBaseBucket("/tb4a", job, null))
					.done();

			List<BasicMessageBean> res = BucketValidationUtils.staticValidation(tb, false);
			System.out.println("validation errs = " + res.stream().map(m->m.message()).collect(Collectors.joining(" ; ")));
			assertEquals(2, res.size());
		}
		{
			final AnalyticThreadJobOutputBean output = getBaseOutput(true, MasterEnrichmentType.none, null);
			
			final AnalyticThreadJobBean job = BeanTemplateUtils.clone(getBaseJob("name", null, output, null))
					.done();
			
			final DataBucketBean tb = BeanTemplateUtils.clone(getBaseBucket("/tb4b", job, null))
					.done();

			List<BasicMessageBean> res = BucketValidationUtils.staticValidation(tb, false);
			System.out.println("validation errs = " + res.stream().map(m->m.message()).collect(Collectors.joining(" ; ")));
			assertEquals(1, res.size());
		}
		//valid
		{
			final AnalyticThreadJobOutputBean output = getBaseOutput(true, MasterEnrichmentType.streaming, null);
			
			final AnalyticThreadJobBean job = BeanTemplateUtils.clone(getBaseJob("name", null, output, null))
					.done();
			
			final DataBucketBean tb = BeanTemplateUtils.clone(getBaseBucket("/tb4c", job, null))
					.done();

			List<BasicMessageBean> res = BucketValidationUtils.staticValidation(tb, false);
			System.out.println("validation errs = " + res.stream().map(m->m.message()).collect(Collectors.joining(" ; ")));
			assertEquals(0, res.size());
		}
		{
			final AnalyticThreadJobOutputBean output = getBaseOutput(false, null, "invalid");
			
			final AnalyticThreadJobBean job = BeanTemplateUtils.clone(getBaseJob("name", null, output, null))
					.done();
			
			final DataBucketBean tb = BeanTemplateUtils.clone(getBaseBucket("/tb4d", job, null))
					.done();

			List<BasicMessageBean> res = BucketValidationUtils.staticValidation(tb, false);
			System.out.println("validation errs = " + res.stream().map(m->m.message()).collect(Collectors.joining(" ; ")));
			assertEquals(0, res.size());
		}
	}

	@Test
	public void test_bucketDistributionRestrictions() {

		final DataBucketBean base_bucket_pure = getBaseBucket("/tb0", null, null);
		final DataBucketBean base_bucket_harvest = 
				BeanTemplateUtils.clone(getBaseBucket("/tb0", null, null))
					.with(DataBucketBean::harvest_technology_name_or_id, "/test")
				.done()
				;
		
		final AnalyticThreadJobBean job1 = BeanTemplateUtils.clone(getBaseJob("name1", null, null, null))
				.with(AnalyticThreadJobBean::lock_to_nodes, true)
				.done();
		final AnalyticThreadJobBean job2 = BeanTemplateUtils.clone(getBaseJob("name2", null, null, null))
				.with(AnalyticThreadJobBean::lock_to_nodes, false)
				.done();
		final AnalyticThreadJobBean job3 = BeanTemplateUtils.clone(getBaseJob("name3", null, null, null))
				.with(AnalyticThreadJobBean::lock_to_nodes, true)
				.done();
		final AnalyticThreadJobBean job4 = BeanTemplateUtils.clone(getBaseJob("name3", null, null, null))
				.with(AnalyticThreadJobBean::multi_node_enabled, true)
				.done();
				
		final BiFunction<DataBucketBean, List<AnalyticThreadJobBean>, DataBucketBean> fn = (bucket, jobs) -> 
			BeanTemplateUtils.clone(bucket)
					.with(DataBucketBean::analytic_thread, 
							BeanTemplateUtils.clone(bucket.analytic_thread())
								.with(AnalyticThreadBean::jobs, jobs)
							.done())
				.done();
		
		// Quick check that validation works
		{
			List<String> res0 = BucketValidationUtils.validateAnalyticBucket(base_bucket_pure);
			assertTrue(res0.isEmpty());
			List<String> res1 = BucketValidationUtils.validateAnalyticBucket(base_bucket_harvest);
			assertTrue(res1.isEmpty());
		}
		// Multi node enabled
		{
			final DataBucketBean b1 = fn.apply(base_bucket_pure, Arrays.asList(job4));
			List<String> res0 = BucketValidationUtils.validateAnalyticBucket(b1);
			assertEquals(1, res0.size());
		}
		// Lock to nodes enabled - pure (works)
		{
			final DataBucketBean b1 = fn.apply(base_bucket_pure, Arrays.asList(job1));
			List<String> res0 = BucketValidationUtils.validateAnalyticBucket(b1);
			assertEquals(0, res0.size());
		}
		// Lock to nodes enabled - hybrid (false)
		{
			final DataBucketBean b1 = fn.apply(base_bucket_harvest, Arrays.asList(job1));
			List<String> res0 = BucketValidationUtils.validateAnalyticBucket(b1);
			assertEquals(1, res0.size());
		}
		// Lock to nodes enabled - pure but two are different
		{
			final DataBucketBean b1 = fn.apply(base_bucket_pure, Arrays.asList(job1, job2, job3));
			List<String> res0 = BucketValidationUtils.validateAnalyticBucket(b1);
			assertEquals(1, res0.size());
		}
		// Lock to nodes enabled - pure and all the same
		{
			final DataBucketBean b1 = fn.apply(base_bucket_pure, Arrays.asList(job1, job3));
			List<String> res0 = BucketValidationUtils.validateAnalyticBucket(b1);
			assertEquals(0, res0.size());
		}
		{
			final DataBucketBean b1 = fn.apply(base_bucket_pure, Arrays.asList(job2));
			List<String> res0 = BucketValidationUtils.validateAnalyticBucket(b1);
			assertEquals(0, res0.size());
		}
	}
	
	@Test
	public void test_analyticsValidation_triggers() {
		
		// 5) Top level trigger test
		
		// (Base check:)		
		List<String> res0 = BucketValidationUtils.validateAnalyticBucket(getBaseBucket("/tb5/a", null, getBaseTriggerBean("hourly", getComplexTriggerBean(0))));
		assertTrue("Simple bucket validates: " + res0.stream().collect(Collectors.joining(";")), res0.isEmpty());

		// Invalid time
		{
			final DataBucketBean tb = BeanTemplateUtils.clone(getBaseBucket("/tb4d", null, getBaseTriggerBean("rabbit", getComplexTriggerBean(0))))
					.done();
			
			List<BasicMessageBean> res = BucketValidationUtils.staticValidation(tb, false);
			System.out.println("validation errs = " + res.stream().map(m->m.message()).collect(Collectors.joining(" ; ")));
			assertEquals(1, res.size());			
		}
		
		// 6) Complex trigger tests
		
		{
			final DataBucketBean tb = BeanTemplateUtils.clone(getBaseBucket("/tb4e/1", null, getBaseTriggerBean(null, getComplexTriggerBean(1))))
					.done();
			
			List<BasicMessageBean> res = BucketValidationUtils.staticValidation(tb, false);
			System.out.println("validation errs = " + res.stream().map(m->m.message()).collect(Collectors.joining(" ; ")));
			assertEquals(1, res.size());						
		}
		{
			final DataBucketBean tb = BeanTemplateUtils.clone(getBaseBucket("/tb4e/2", null, getBaseTriggerBean(null, getComplexTriggerBean(2))))
					.done();
			
			List<BasicMessageBean> res = BucketValidationUtils.staticValidation(tb, false);
			System.out.println("validation errs = " + res.stream().map(m->m.message()).collect(Collectors.joining(" ; ")));
			assertEquals(2, res.size());						
		}
		{
			final DataBucketBean tb = BeanTemplateUtils.clone(getBaseBucket("/tb4e/3", null, getBaseTriggerBean(null, getComplexTriggerBean(3))))
					.done();
			
			List<BasicMessageBean> res = BucketValidationUtils.staticValidation(tb, false);
			System.out.println("validation errs = " + res.stream().map(m->m.message()).collect(Collectors.joining(" ; ")));
			assertEquals(1, res.size());						
		}
		{
			final DataBucketBean tb = BeanTemplateUtils.clone(getBaseBucket("/tb4e/31", null, getBaseTriggerBean(null, getComplexTriggerBean(31))))
					.done();
			
			List<BasicMessageBean> res = BucketValidationUtils.staticValidation(tb, false);
			System.out.println("validation errs = " + res.stream().map(m->m.message()).collect(Collectors.joining(" ; ")));
			assertEquals(0, res.size());
		}
		{
			final DataBucketBean tb = BeanTemplateUtils.clone(getBaseBucket("/tb4e/4", null, getBaseTriggerBean(null, getComplexTriggerBean(4))))
					.done();
			
			List<BasicMessageBean> res = BucketValidationUtils.staticValidation(tb, false);
			System.out.println("validation errs = " + res.stream().map(m->m.message()).collect(Collectors.joining(" ; ")));
			assertEquals(1, res.size());						
		}
		{
			final DataBucketBean tb = BeanTemplateUtils.clone(getBaseBucket("/tb4e/5/1", null, getBaseTriggerBean(null, getComplexTriggerBean(51))))
					.done();
			
			List<BasicMessageBean> res = BucketValidationUtils.staticValidation(tb, false);
			System.out.println("validation errs = " + res.stream().map(m->m.message()).collect(Collectors.joining(" ; ")));
			assertEquals(2, res.size());						
		}
		{
			final DataBucketBean tb = BeanTemplateUtils.clone(getBaseBucket("/tb4e/5/2", null, getBaseTriggerBean(null, getComplexTriggerBean(52))))
					.done();
			
			List<BasicMessageBean> res = BucketValidationUtils.staticValidation(tb, false);
			System.out.println("validation errs = " + res.stream().map(m->m.message()).collect(Collectors.joining(" ; ")));
			assertEquals(1, res.size());						
		}
	}

	@Test	
	public void test_lockToNodes() {
		
		// 3 cases:
		
		final AnalyticThreadJobInputBean input = BeanTemplateUtils.clone(getBaseInput(null))
				.with(AnalyticThreadJobInputBean::data_service, "search_index_service")
				.done();

		// 1) Valid lock to nodes
		{
			final AnalyticThreadJobBean job = BeanTemplateUtils.clone(getBaseJob("name", input, null, null))
					.done();

			final DataBucketBean tb = BeanTemplateUtils.clone(getBaseBucket("/tb3c", job, null))
					.with(DataBucketBean::lock_to_nodes, true)
					.done();

			List<BasicMessageBean> res = BucketValidationUtils.staticValidation(tb, false);
			System.out.println("validation errs = " + res.stream().map(m->m.message()).collect(Collectors.joining(" ; ")));
			assertEquals(0, res.size());						
		}
		
		// 2) Invalid: too many technologies
		{
			final AnalyticThreadJobBean job = BeanTemplateUtils.clone(getBaseJob("name", input, null, null))
					.done();

			final AnalyticThreadJobBean job_other = BeanTemplateUtils.clone(job)
					.with(AnalyticThreadJobBean::analytic_technology_name_or_id, "/other_test")
					.done();
			
			final DataBucketBean tb_pre = BeanTemplateUtils.clone(getBaseBucket("/tb3c", job, null))
					.with(DataBucketBean::lock_to_nodes, true)
					.done();			
			
			final DataBucketBean tb = BeanTemplateUtils.clone(tb_pre)
					.with(DataBucketBean::lock_to_nodes, true)
					.with(DataBucketBean::analytic_thread, 
							BeanTemplateUtils.clone(tb_pre.analytic_thread())
								.with(AnalyticThreadBean::jobs,
										Arrays.asList(job, job_other)
										)
							.done()
						)
					.done();

			{
				List<BasicMessageBean> res = BucketValidationUtils.staticValidation(tb, false);
				System.out.println("validation errs = " + res.stream().map(m->m.message()).collect(Collectors.joining(" ; ")));
				assertEquals(1, res.size());
			}
			
			// 3) >1 techs _but_ not a pure analytic thread so is ignored anyway
			{
				final DataBucketBean tb_harvest = BeanTemplateUtils.clone(tb)
						.with(DataBucketBean::harvest_technology_name_or_id, "/harvest")
						.with(DataBucketBean::harvest_configs,
								Arrays.asList(
										BeanTemplateUtils.build(HarvestControlMetadataBean.class)
										.done().get()
										)
								)						
						.done();
				
				List<BasicMessageBean> res = BucketValidationUtils.staticValidation(tb_harvest, false);
				System.out.println("validation errs = " + res.stream().map(m->m.message()).collect(Collectors.joining(" ; ")));
				assertEquals(0, res.size());						
			}
			
		}
	}
	
	//////////////////////////////////////////
	
	protected AnalyticThreadComplexTriggerBean getComplexTriggerBean(int type) {
		
		if (0 == type) {
			return BeanTemplateUtils.build(AnalyticThreadComplexTriggerBean.class)
					.with(AnalyticThreadComplexTriggerBean::resource_name_or_id, "/test")
					.with(AnalyticThreadComplexTriggerBean::type, TriggerType.bucket)
					.done().get();
		}
		else if (1 == type) {
			return BeanTemplateUtils.build(AnalyticThreadComplexTriggerBean.class)
					//(both op and resource id null)
					.done().get();			
		}
		else if (2 == type) {
			return BeanTemplateUtils.build(AnalyticThreadComplexTriggerBean.class)
					//(both op and resource id non null)
					.with(AnalyticThreadComplexTriggerBean::resource_name_or_id, "/test")
					.with(AnalyticThreadComplexTriggerBean::type, TriggerType.file)
					.with(AnalyticThreadComplexTriggerBean::op, TriggerOperator.and)
					.with(AnalyticThreadComplexTriggerBean::dependency_list, Arrays.asList(getComplexTriggerBean(1)))
					.done().get();			
		}
		else if (3 == type) {
			return BeanTemplateUtils.build(AnalyticThreadComplexTriggerBean.class)
					//(custom type with no analytic tech specified)
					.with(AnalyticThreadComplexTriggerBean::resource_name_or_id, "/test")
					.with(AnalyticThreadComplexTriggerBean::type, TriggerType.custom)
					.done().get();			
		}
		else if (31 == type) {
			return BeanTemplateUtils.build(AnalyticThreadComplexTriggerBean.class)
					//(custom type with no analytic tech specified)
					.with(AnalyticThreadComplexTriggerBean::custom_analytic_technology_name_or_id, "/test")
					.with(AnalyticThreadComplexTriggerBean::type, TriggerType.custom)
					.with(AnalyticThreadComplexTriggerBean::resource_name_or_id, "/test")
					.done().get();			
		}
		else if (4 == type) {
			return BeanTemplateUtils.build(AnalyticThreadComplexTriggerBean.class)
					//(operator with empty list)
					.with(AnalyticThreadComplexTriggerBean::op, TriggerOperator.and)
					.done().get();			
		}
		else if (51 == type) {
			return BeanTemplateUtils.build(AnalyticThreadComplexTriggerBean.class)
					//(type no resource id)
					.with(AnalyticThreadComplexTriggerBean::type, TriggerType.file)
					.done().get();			
		}
		else if (52 == type) {
			return BeanTemplateUtils.build(AnalyticThreadComplexTriggerBean.class)
					//(resource id no type)
					.with(AnalyticThreadComplexTriggerBean::resource_name_or_id, "/test")
					.done().get();			
		}
		else return null;
	}
	
	//////////////////////////////////////////

	protected AnalyticThreadTriggerBean getBaseTriggerBean(String schedule, AnalyticThreadComplexTriggerBean complex_trigger) {
		
		return BeanTemplateUtils.build(AnalyticThreadTriggerBean.class)
				.with(AnalyticThreadTriggerBean::schedule, schedule)
				.with(AnalyticThreadTriggerBean::trigger, complex_trigger)
				.done().get();
	}
	
	protected AnalyticThreadJobInputConfigBean getBaseInputConfig(String tmin, String tmax) {		
		return BeanTemplateUtils.build(AnalyticThreadJobInputConfigBean.class)
				.with(AnalyticThreadJobInputConfigBean::time_min, tmin)
				.with(AnalyticThreadJobInputConfigBean::time_max, tmax)
				.done().get();
	}

	protected AnalyticThreadJobInputBean getBaseInput(AnalyticThreadJobInputConfigBean config) {

		return BeanTemplateUtils.build(AnalyticThreadJobInputBean.class)
				.with(AnalyticThreadJobInputBean::data_service, "stream")
				.with(AnalyticThreadJobInputBean::resource_name_or_id, "stream")
				.with(AnalyticThreadJobInputBean::config, config)
				.done().get();
	}

	protected AnalyticThreadJobOutputBean getBaseOutput(Boolean is_transient, MasterEnrichmentType transient_type, String sub_bucket_patch) {
		
		return BeanTemplateUtils.build(AnalyticThreadJobOutputBean.class)
				.with(AnalyticThreadJobOutputBean::is_transient, is_transient)
				.with(AnalyticThreadJobOutputBean::transient_type, transient_type)
				.with(AnalyticThreadJobOutputBean::sub_bucket_path, sub_bucket_patch)
				.done().get();
	}
	
	
	protected AnalyticThreadJobBean getBaseJob(final String name, AnalyticThreadJobInputBean input, AnalyticThreadJobOutputBean output, AnalyticThreadJobInputConfigBean global_config) {
		return BeanTemplateUtils.build(AnalyticThreadJobBean.class)
				.with(AnalyticThreadJobBean::analytic_technology_name_or_id, "/test")
				.with(AnalyticThreadJobBean::analytic_type, MasterEnrichmentType.streaming)
				.with(AnalyticThreadJobBean::name, name)
				.with(AnalyticThreadJobBean::inputs, input == null ? Collections.emptyList() : Arrays.asList(input))
				.with(AnalyticThreadJobBean::global_input_config, global_config)
				.with(AnalyticThreadJobBean::output, output)
				.done().get();		

	}

	protected DataBucketBean getBaseBucket(final String test_name, final AnalyticThreadJobBean job, final AnalyticThreadTriggerBean trigger) {
		return BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::_id, "id1")
				.with(DataBucketBean::full_name, test_name)
				.with(DataBucketBean::display_name, test_name)
				.with(DataBucketBean::created, new Date())
				.with(DataBucketBean::modified, new Date())
				.with(DataBucketBean::analytic_thread,
						BeanTemplateUtils.build(AnalyticThreadBean.class)
						.with(AnalyticThreadBean::jobs, job == null ? Collections.emptyList() : Arrays.asList(job))
						.with(AnalyticThreadBean::trigger_config, trigger)
						.done().get()
						)
						.with(DataBucketBean::owner_id, "owner1")
						.done().get();
	}
}
