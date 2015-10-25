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
package com.ikanow.aleph2.data_import_manager.analytics.utils;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;

import scala.Tuple2;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean.AnalyticThreadComplexTriggerBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean.AnalyticThreadComplexTriggerBean.TriggerOperator;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean.AnalyticThreadComplexTriggerBean.TriggerType;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerStateBean;

public class TestAnalyticTriggerBeanUtils {

	@Test
	public void test_automaticTriggers() {		
		final DataBucketBean auto_trigger_bucket = TestAnalyticTriggerCrudUtils.buildBucket("/test/trigger", false);
		
		final Stream<AnalyticTriggerStateBean> test_stream = AnalyticTriggerBeanUtils.generateTriggerStateStream(auto_trigger_bucket, false, Optional.empty());
		final List<AnalyticTriggerStateBean> test_list = test_stream.collect(Collectors.toList());
		
		System.out.println("Resources = \n" + 
				test_list.stream().map(t -> BeanTemplateUtils.toJson(t).toString()).collect(Collectors.joining("\n")));
		
		assertEquals(7, test_list.size()); // (3 inputs + 4 job deps:)
		assertEquals(3, test_list.stream().filter(trigger -> null == trigger.job_name()).count());
		assertEquals(4, test_list.stream().filter(trigger -> null != trigger.job_name()).count());
	}
	
	@Test
	public void test_enrichmentBucket() throws IOException {
		final String json_bucket = Resources.toString(Resources.getResource("com/ikanow/aleph2/data_import_manager/analytics/actors/real_test_case_batch_2.json"), Charsets.UTF_8);		
		final DataBucketBean enrichment_bucket = BeanTemplateUtils.from(json_bucket, DataBucketBean.class).get();
		
		final Stream<AnalyticTriggerStateBean> test_stream = AnalyticTriggerBeanUtils.generateTriggerStateStream(enrichment_bucket, false, Optional.empty());
		final List<AnalyticTriggerStateBean> test_list = test_stream.collect(Collectors.toList());
		
		System.out.println("Resources = \n" + 
				test_list.stream().map(t -> BeanTemplateUtils.toJson(t).toString()).collect(Collectors.joining("\n")));
		
		assertEquals(1, test_list.size());
		AnalyticTriggerStateBean trigger = test_list.get(0);
		assertEquals(enrichment_bucket.full_name(), trigger.input_resource_name_or_id());
		assertEquals(TriggerType.file, trigger.trigger_type());
	}
	
	@Test
	public void test_buildAutomaticTrigger() {		
		final DataBucketBean auto_trigger_bucket = TestAnalyticTriggerCrudUtils.buildBucket("/test/trigger", false);
		Optional<AnalyticThreadComplexTriggerBean> trigger = AnalyticTriggerBeanUtils.getManualOrAutomatedTrigger(auto_trigger_bucket);
		assertTrue("Auto trigger present", trigger.isPresent());
		assertEquals("Auto trigger has an 'and' operator", TriggerOperator.and, trigger.get().op());
		assertEquals("Auto trigger length is correct", 3, trigger.get().dependency_list().size());		
		assertTrue("Auto trigger is enabled", Optional.ofNullable(trigger.get().enabled()).orElse(true));
		// Perform some quick checks of the trigger fields
		trigger.get().dependency_list().stream()
			.forEach(tr -> {
				assertTrue("Auto trigger element is enabled", Optional.ofNullable(tr.enabled()).orElse(true));
				assertTrue("Auto trigger element has resource", null != tr.resource_name_or_id());
				assertTrue("Auto trigger element has data service", null != tr.data_service());				
			});		
	}
	
	@Test
	public void test_buildManualTrigger() {
		final DataBucketBean manual_trigger_bucket = TestAnalyticTriggerCrudUtils.buildBucket("/test/trigger", true);
		Optional<AnalyticThreadComplexTriggerBean> trigger = AnalyticTriggerBeanUtils.getManualOrAutomatedTrigger(manual_trigger_bucket);
		assertTrue("Manual trigger present", trigger.isPresent());
		assertEquals("Manual trigger returns itself", manual_trigger_bucket.analytic_thread().trigger_config().trigger(), trigger.get());
		
	}
	
	@Test
	public void test_checkAutoTrigger() {
		final DataBucketBean auto_trigger_bucket = TestAnalyticTriggerCrudUtils.buildBucket("/test/trigger", false);
		Optional<AnalyticThreadComplexTriggerBean> trigger = AnalyticTriggerBeanUtils.getManualOrAutomatedTrigger(auto_trigger_bucket);
		// this generates the following trigger:
		// and: [ "/test_job1a_input_1":storage_service, "/test_job1a_input:temp":batch, "/test_job3_input_1":search_index_service ]
		
		// 1) fail (not enough terms)
		{
			final Set<Tuple2<String, String>> resources_dataservices
			= Stream.of(
					Tuples._2T( "/test_job1a_input_1", "storage_service"),
					Tuples._2T( "/test_job1a_input:temp", "batch")
					)
					.collect(Collectors.toSet());
		
			assertFalse("Should fail (not enough terms)", AnalyticTriggerBeanUtils.checkTrigger(trigger.get(), resources_dataservices, true));			
		}
		// 2) succeed (multiple terms)
		{
			final Set<Tuple2<String, String>> resources_dataservices
				= Stream.of(
						Tuples._2T( "/test_job1a_input_1", "storage_service"),
						Tuples._2T( "/test_job1a_input:temp", "batch"),
						Tuples._2T( "/test_job3_input_1", "search_index_service")						
					)
					.collect(Collectors.toSet());
		
			assertTrue("Should match", AnalyticTriggerBeanUtils.checkTrigger(trigger.get(), resources_dataservices, true));			
		}
	}
	
	@Test
	public void test_checkManualTrigger() {
		
		// Manual trigger for this bucket is
		// and: [ or: and: [ "/input/test/1/1/1":search_index_service, and: [ "/input/test/1/1/2":storage_service , {"/input/test/1/1/3":(file)}, "/input/test/1/1/1":search_index_service] ] 
		//        not: [ "/input/test/1/1/1":bucket, "/input/test/2/1:test":bucket]
		//        {stuff}]
		
		final DataBucketBean manual_trigger_bucket = TestAnalyticTriggerCrudUtils.buildBucket("/test/trigger", true);
		Optional<AnalyticThreadComplexTriggerBean> trigger = AnalyticTriggerBeanUtils.getManualOrAutomatedTrigger(manual_trigger_bucket);

		// 1) fail (not enough terms)
		{
			final Set<Tuple2<String, String>> resources_dataservices
				= Stream.of(
						Tuples._2T( "/input/test/1/1/1", "search_index_service")
						)
						.collect(Collectors.toSet());
			
			assertFalse("Should fail (not enough terms)", AnalyticTriggerBeanUtils.checkTrigger(trigger.get(), resources_dataservices, true));
		}
		// 2) fail (not enough terms)
		{
			final Set<Tuple2<String, String>> resources_dataservices
				= Stream.of(
						Tuples._2T( "/input/test/1/1/2", "storage_service")
						)
						.collect(Collectors.toSet());
			
			assertFalse("Should fail (not enough terms)", AnalyticTriggerBeanUtils.checkTrigger(trigger.get(), resources_dataservices, true));
		}
		// 3) succeed (multiple terms)
		{
			final Set<Tuple2<String, String>> resources_dataservices
				= Stream.of(
						Tuples._2T( "/input/test/1/1/2", "storage_service"),
						Tuples._2T( "/input/test/1/1/1", "search_index_service")						
					)
					.collect(Collectors.toSet());
		
			assertTrue("Should match", AnalyticTriggerBeanUtils.checkTrigger(trigger.get(), resources_dataservices, true));			
		}
		// 4) failure vs not
		{
			final Set<Tuple2<String, String>> resources_dataservices
				= Stream.of(
					Tuples._2T( "/input/test/1/1/2", "storage_service"),
					Tuples._2T( "/input/test/2/1:test", "bucket")
					)
					.collect(Collectors.toSet());
		
			assertFalse("Should fail (not)", AnalyticTriggerBeanUtils.checkTrigger(trigger.get(), resources_dataservices, true));
		}
	}
	
	@Test
	public void test_getNextCheckTime() {
		//TODO (ALEPH-12): check the 3 different cases (2 locs + default)
	}
	
}
