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
package com.ikanow.aleph2.data_import_manager.analytics.utils;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;

import scala.Tuple2;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import com.ikanow.aleph2.data_model.interfaces.shared_services.MockManagementCrudService;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean.AnalyticThreadComplexTriggerBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean.AnalyticThreadComplexTriggerBean.TriggerOperator;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean.AnalyticThreadComplexTriggerBean.TriggerType;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.BucketUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerStateBean;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage.BucketActionAnalyticJobMessage.JobMessageType;
import com.ikanow.aleph2.management_db.data_model.BucketMgmtMessage.BucketTimeoutMessage;

public class TestAnalyticTriggerBeanUtils {

	@Test 
	public void test_internalMessageBuilding_lockWorkaround() {
		MockManagementCrudService<DataBucketStatusBean> test_status = new MockManagementCrudService<>();
		MockManagementCrudService<BucketTimeoutMessage> test_test_status = new MockManagementCrudService<>();
		
		// Empty ie won't find
		test_status.setMockValues(Arrays.asList());
		test_test_status.setMockValues(Arrays.asList());
		
		final AnalyticThreadJobBean job1 = 
				BeanTemplateUtils.build(AnalyticThreadJobBean.class)
				.with(AnalyticThreadJobBean::lock_to_nodes, true)
			.done().get();
		
		final AnalyticThreadJobBean job2 = 
				BeanTemplateUtils.build(AnalyticThreadJobBean.class)
			.done().get();
		
		final DataBucketBean bucket1 = 
				BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::full_name, "/test")
					.with(DataBucketBean::owner_id, "owner")
					.with(DataBucketBean::analytic_thread, 
							BeanTemplateUtils.build(AnalyticThreadBean.class)
								.with(AnalyticThreadBean::jobs,
										Arrays.asList(job1)
										)
							.done().get()
							)
				.done().get()
				;

		final DataBucketBean bucket2 = 
				BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::analytic_thread, 
							BeanTemplateUtils.build(AnalyticThreadBean.class)
								.with(AnalyticThreadBean::jobs,
										Arrays.asList(job2)
										)
							.done().get()
							)
				.done().get()
				;
		
		final DataBucketStatusBean bucket_status1 = 				
				BeanTemplateUtils.build(DataBucketStatusBean.class)
					.with(DataBucketStatusBean::node_affinity, Arrays.asList("test1"))
				.done().get()
				;

		{
			final Collection<String> res1 = AnalyticTriggerBeanUtils.sendInternalEventMessage_internal(
					new BucketActionMessage.BucketActionAnalyticJobMessage(bucket1, Arrays.asList(job1), JobMessageType.check_completion),
					test_status,
					test_test_status
					).join()
					;
			
			assertTrue(res1.isEmpty());
		}
		
		test_status.setMockValues(Arrays.asList(bucket_status1));
		
		{
			final Collection<String> res1 = AnalyticTriggerBeanUtils.sendInternalEventMessage_internal(
					new BucketActionMessage.BucketActionAnalyticJobMessage(bucket1, Arrays.asList(job1), JobMessageType.check_completion),
					test_status,
					test_test_status
					).join()
					;
			
			assertEquals(Arrays.asList("test1"), res1);
		}
		{
			final Collection<String> res1 = AnalyticTriggerBeanUtils.sendInternalEventMessage_internal(
					new BucketActionMessage.BucketActionAnalyticJobMessage(bucket2, Arrays.asList(job2), JobMessageType.check_completion),
					test_status,test_test_status
					).join()
					;
			
			assertTrue(res1.isEmpty());
		}
		
		// OK now test test mode:
		
		final BucketTimeoutMessage timeout_msg = 
				BeanTemplateUtils.build(BucketTimeoutMessage.class)
				.with(BucketTimeoutMessage::handling_clients, ImmutableSet.of("test_test1"))
				.done().get();
		
		test_status.setMockValues(Arrays.asList());
		test_test_status.setMockValues(Arrays.asList(timeout_msg));
		
		final DataBucketBean test_bucket1 = BucketUtils.convertDataBucketBeanToTest(bucket1, "owner2");
		
		{
			final Collection<String> res1 = AnalyticTriggerBeanUtils.sendInternalEventMessage_internal(
					new BucketActionMessage.BucketActionAnalyticJobMessage(test_bucket1, Arrays.asList(job1), JobMessageType.check_completion),
					test_status,
					test_test_status
					).join()
					;
			
			assertEquals(ImmutableSet.of("test_test1"), res1);
		}		
	}
	
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
	public void test_timedTrigger() {
		final DataBucketBean auto_trigger_bucket = TestAnalyticTriggerCrudUtils.buildBucket("/test/trigger", false);
		final DataBucketBean timed_trigger_bucket = 
				BeanTemplateUtils.clone(auto_trigger_bucket)
					.with(DataBucketBean::analytic_thread,
							BeanTemplateUtils.clone(auto_trigger_bucket.analytic_thread())
								.with(AnalyticThreadBean::jobs, Arrays.asList())
								.with(AnalyticThreadBean::trigger_config,
										BeanTemplateUtils.build(AnalyticThreadTriggerBean.class)
											.with(AnalyticThreadTriggerBean::auto_calculate, false)
											.with(AnalyticThreadTriggerBean::schedule, "10 minutes")
										.done().get()
										)
							.done()
							)
				.done();
		
		final Stream<AnalyticTriggerStateBean> test_stream = AnalyticTriggerBeanUtils.generateTriggerStateStream(timed_trigger_bucket, false, Optional.empty());
		final List<AnalyticTriggerStateBean> test_list = test_stream.collect(Collectors.toList());
		
		System.out.println("Resources = \n" + 
				test_list.stream().map(t -> BeanTemplateUtils.toJson(t).toString()).collect(Collectors.joining("\n")));
		
		assertEquals(1, test_list.size());
		AnalyticTriggerStateBean trigger = test_list.get(0);
		assertEquals(TriggerType.time, trigger.trigger_type());
		
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
		
		final Date now = new Date();

		final DataBucketBean bucket0 = BeanTemplateUtils.build(DataBucketBean.class)
			.done().get();		
		
		final DataBucketBean bucket1 = BeanTemplateUtils.build(DataBucketBean.class)
											.with(DataBucketBean::poll_frequency, "1 hour")
										.done().get();
		
		final DataBucketBean bucket2 = BeanTemplateUtils.build(DataBucketBean.class)
											.with(DataBucketBean::analytic_thread,
													BeanTemplateUtils.build(AnalyticThreadBean.class)
														.with(AnalyticThreadBean::trigger_config,
																BeanTemplateUtils.build(AnalyticThreadTriggerBean.class)
																	.with(AnalyticThreadTriggerBean::schedule, "2 hours")
																.done().get()
																)
													.done().get()
													)
											.with(DataBucketBean::poll_frequency, "1 day") // this will be ignored
										.done().get();
		
		assertEquals(now.toInstant().plusSeconds(600L), AnalyticTriggerBeanUtils.getNextCheckTime(now, bucket0).toInstant());
		assertEquals(now.toInstant().plusSeconds(3600L), AnalyticTriggerBeanUtils.getNextCheckTime(now, bucket1).toInstant());
		assertEquals(now.toInstant().plusSeconds(7200L), AnalyticTriggerBeanUtils.getNextCheckTime(now,bucket2).toInstant());
	}
	
	@Test
	public void test_testRelativeTime() {
		
		final Date now = new Date();

		final DataBucketBean bucket0 = BeanTemplateUtils.build(DataBucketBean.class)
											.with(DataBucketBean::poll_frequency, "3pm")
										.done().get();		
		
		final DataBucketBean bucket1 = BeanTemplateUtils.build(DataBucketBean.class)
											.with(DataBucketBean::poll_frequency, "1 hour")
										.done().get();
		
		{
			final Date next_0 = AnalyticTriggerBeanUtils.getNextCheckTime(now, bucket0);
			assertFalse(AnalyticTriggerBeanUtils.scheduleIsRelative(now, next_0, bucket0));
		}
		{
			final Date next_1 = AnalyticTriggerBeanUtils.getNextCheckTime(now, bucket1);
			assertTrue(AnalyticTriggerBeanUtils.scheduleIsRelative(now, next_1, bucket1));
		}
	}
	
}
