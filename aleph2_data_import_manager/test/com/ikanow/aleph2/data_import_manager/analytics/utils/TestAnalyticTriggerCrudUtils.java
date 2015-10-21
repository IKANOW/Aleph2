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

import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;

import scala.Tuple2;

import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean.AnalyticThreadComplexTriggerBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean.AnalyticThreadComplexTriggerBean.TriggerOperator;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean.AnalyticThreadComplexTriggerBean.TriggerType;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerStateBean;
import com.ikanow.aleph2.shared.crud.mongodb.services.MockMongoDbCrudServiceFactory;

//TODO (ALEPH-12): in bucket change actor need to activate the bucket if any jobs manually triggered...

//TODO (ALEPH-12): now copy internal job deps into resource_name_or_id, so need to sort that out

public class TestAnalyticTriggerCrudUtils {

	ICrudService<AnalyticTriggerStateBean> _test_crud;
	
	@Before
	public void setup() throws InterruptedException, ExecutionException {
		
		MockMongoDbCrudServiceFactory factory = new MockMongoDbCrudServiceFactory();
		_test_crud = factory.getMongoDbCrudService(AnalyticTriggerStateBean.class, String.class, factory.getMongoDbCollection("test.trigger_crud"), Optional.empty(), Optional.empty(), Optional.empty());
		_test_crud.deleteDatastore().get();
	}	
	
	@Test
	public void test_storeOrUpdateTriggerStage_updateActivation() throws InterruptedException {
		assertEquals(0, _test_crud.countObjects().join().intValue());
		
		final DataBucketBean bucket = buildBucket("/test/store/trigger", true);

		// Save a bucket
		{		
			//TODO: add tests in TestBeanUtils for other cases?
			final Stream<AnalyticTriggerStateBean> test_stream = AnalyticTriggerBeanUtils.generateTriggerStateStream(bucket, false, Optional.empty());
			final List<AnalyticTriggerStateBean> test_list = test_stream.collect(Collectors.toList());
			
			System.out.println("Resources = \n" + 
					test_list.stream().map(t -> BeanTemplateUtils.toJson(t).toString()).collect(Collectors.joining("\n")));
			
			assertEquals(8L, test_list.size()); //(8 not 7 cos haven't dedup'd yet)
	
			// 4 internal dependencies
			assertEquals(4L, test_list.stream().filter(t -> null != t.job_name()).count());
			// 4 external dependencies
			assertEquals(4L, test_list.stream().filter(t -> null == t.job_name()).count());
			
			final Map<Tuple2<String, String>, List<AnalyticTriggerStateBean>> grouped_triggers
				= test_list.stream().collect(
						Collectors.groupingBy(t -> Tuples._2T(t.bucket_name(), null)));
			
			AnalyticTriggerCrudUtils.storeOrUpdateTriggerStage(_test_crud, grouped_triggers).join();
			
			assertEquals(7L, _test_crud.countObjects().join().intValue());
		}		
		
		//DEBUG
		//this.printTriggerDatabase();
		
		// Sleep to change times
		Thread.sleep(100L); 
		
		// 2) Modify and update
		final DataBucketBean mod_bucket = 
				BeanTemplateUtils.clone(bucket)
					.with(DataBucketBean::analytic_thread, 
							BeanTemplateUtils.clone(bucket.analytic_thread())
								.with(AnalyticThreadBean::jobs,
										bucket.analytic_thread().jobs().stream()
											.map(j -> 
												BeanTemplateUtils.clone(j)
													.with(AnalyticThreadJobBean::name, "test_" + j.name())
												.done()
											)
											.collect(Collectors.toList())
										)
							.done()
							)
				.done();
		{
			
			final Stream<AnalyticTriggerStateBean> test_stream = AnalyticTriggerBeanUtils.generateTriggerStateStream(mod_bucket, false, Optional.empty());
			final List<AnalyticTriggerStateBean> test_list = test_stream.collect(Collectors.toList());

			final Map<Tuple2<String, String>, List<AnalyticTriggerStateBean>> grouped_triggers
				= test_list.stream().collect(
						Collectors.groupingBy(t -> Tuples._2T(t.bucket_name(), null)));
		
			AnalyticTriggerCrudUtils.storeOrUpdateTriggerStage(_test_crud, grouped_triggers).join();
		
			//DEBUG
			//this.printTriggerDatabase();
			
			assertEquals(7L, _test_crud.countObjects().join().intValue());
		
			assertEquals(4L, Optionals.streamOf(
					_test_crud.getObjectsBySpec(CrudUtils.allOf(AnalyticTriggerStateBean.class)).join().iterator()
					,
					false)
					.filter(t -> null != t.job_name())
					.filter(t -> t.job_name().startsWith("test_")).count());
		}
		
		// 3) Since we're here might as well try activating...
		{
			final Stream<AnalyticTriggerStateBean> test_stream = Optionals.streamOf(
					_test_crud.getObjectsBySpec(CrudUtils.allOf(AnalyticTriggerStateBean.class)).join().iterator()
					,
					false);
					
			AnalyticTriggerCrudUtils.updateTriggerStatuses(_test_crud, test_stream, new Date(), Optional.of(true)).join();
			
			assertEquals(7L, _test_crud.countObjects().join().intValue());
			assertEquals(7L, Optionals.streamOf(
					_test_crud.getObjectsBySpec(CrudUtils.allOf(AnalyticTriggerStateBean.class)).join().iterator()
					,
					false)
					.filter(t -> t.is_job_active())
					.filter(t -> 100 != Optional.ofNullable(t.last_resource_size()).orElse(-1L))
					.count());
		}		
		// 4) ... and then de-activating...
		{
			final Stream<AnalyticTriggerStateBean> test_stream = Optionals.streamOf(
					_test_crud.getObjectsBySpec(CrudUtils.allOf(AnalyticTriggerStateBean.class)).join().iterator()
					,
					false);
			
			AnalyticTriggerCrudUtils.updateTriggerStatuses(_test_crud, test_stream, new Date(), Optional.of(false)).join();
			
			assertEquals(7L, _test_crud.countObjects().join().intValue());
			assertEquals(7L, Optionals.streamOf(
					_test_crud.getObjectsBySpec(CrudUtils.allOf(AnalyticTriggerStateBean.class)).join().iterator()
					,
					false)
					.filter(t -> !t.is_job_active())
					.filter(t -> 100 != Optional.ofNullable(t.last_resource_size()).orElse(-1L))
					.count());
		}		
		// 5) ... finally re-activate 
		{
			final Stream<AnalyticTriggerStateBean> test_stream = Optionals.streamOf(
					_test_crud.getObjectsBySpec(CrudUtils.allOf(AnalyticTriggerStateBean.class)).join().iterator()
					,
					false)
					.map(t -> BeanTemplateUtils.clone(t)
							.with(AnalyticTriggerStateBean::curr_resource_size, 100L).done())
					;
			
			AnalyticTriggerCrudUtils.updateTriggerStatuses(_test_crud, test_stream, new Date(), Optional.of(true)).join();
			
			assertEquals(7L, _test_crud.countObjects().join().intValue());
			assertEquals(7L, Optionals.streamOf(
					_test_crud.getObjectsBySpec(CrudUtils.allOf(AnalyticTriggerStateBean.class)).join().iterator()
					,
					false)
					.filter(t -> t.is_job_active())
					.filter(t -> 100 == t.last_resource_size())
					.count());
			
		}
	}
	
	//////////////////////////////////////////////////////////////////

	@Test
	public void test_activateUpdateTimesAndSuspend() throws InterruptedException
	{
		assertEquals(0, _test_crud.countObjects().join().intValue());
		
		final DataBucketBean bucket = buildBucket("/test/active/trigger", true);		
		
		// 1) Store as above
		{
			final Stream<AnalyticTriggerStateBean> test_stream = AnalyticTriggerBeanUtils.generateTriggerStateStream(bucket, false, Optional.of("test_host"));
			final List<AnalyticTriggerStateBean> test_list = test_stream.collect(Collectors.toList());
			
			System.out.println("Resources = \n" + 
					test_list.stream().map(t -> BeanTemplateUtils.toJson(t).toString()).collect(Collectors.joining("\n")));
			
			assertEquals(8L, test_list.size());//(8 not 7 because we only dedup at the DB)
	
			// 4 internal dependencies
			assertEquals(4L, test_list.stream().filter(t -> null != t.job_name()).count());
			// 5 external dependencies
			assertEquals(4L, test_list.stream().filter(t -> null == t.job_name()).count());
			
			final Map<Tuple2<String, String>, List<AnalyticTriggerStateBean>> grouped_triggers
				= test_list.stream().collect(
						Collectors.groupingBy(t -> Tuples._2T(t.bucket_name(), null)));
			
			AnalyticTriggerCrudUtils.storeOrUpdateTriggerStage(_test_crud, grouped_triggers).join();
			
			assertEquals(7L, _test_crud.countObjects().join().intValue());
			
		}
		
		//DEBUG
		//printTriggerDatabase();
		
		// Sleep to change times
		Thread.sleep(100L);
		
		//(activate)
		bucket.analytic_thread().jobs().forEach(job -> {
			AnalyticTriggerCrudUtils.createActiveJobRecord(_test_crud, bucket, job, Optional.of("test_host"));
		});
		
		// 2) Activate then save suspended - check suspended goes to pending 		
		{
			final Stream<AnalyticTriggerStateBean> test_stream = AnalyticTriggerBeanUtils.generateTriggerStateStream(bucket, true, Optional.of("test_host"));
			final List<AnalyticTriggerStateBean> test_list = test_stream.collect(Collectors.toList());
			
			assertTrue("All suspended", test_list.stream().filter(t -> t.is_bucket_suspended()).findFirst().isPresent());
			
			final Map<Tuple2<String, String>, List<AnalyticTriggerStateBean>> grouped_triggers
			= test_list.stream().collect(
					Collectors.groupingBy(t -> Tuples._2T(t.bucket_name(), null)));
			
			//DEBUG
			//printTriggerDatabase();
			
			assertEquals(13L, _test_crud.countObjects().join().intValue()); // ie 5 active jobs + 1 active bucket, 4 job dependencies, 3 external triggers 			
			
			AnalyticTriggerCrudUtils.storeOrUpdateTriggerStage(_test_crud, grouped_triggers).join();

			//DEBUG
			//printTriggerDatabase();
			
			assertEquals(17L, _test_crud.countObjects().join().intValue()); // ie 5 active jobs, + 1 active bucket, 4 job dependencies x2 (pending/non-pending), the 3 external triggers get overwritten			
			assertEquals(7L, _test_crud.countObjectsBySpec(
					CrudUtils.allOf(AnalyticTriggerStateBean.class)
						.when(AnalyticTriggerStateBean::is_bucket_suspended, true)
					).join().intValue());			
		}
		
		// Sleep to change times
		Thread.sleep(100L);
		
		// 3) De-activate and check reverts to pending
		{
			AnalyticTriggerCrudUtils.deleteActiveJobEntries(_test_crud, bucket, bucket.analytic_thread().jobs(), Optional.of("test_host")).join();
			
			bucket.analytic_thread().jobs().stream().forEach(job -> {
				//System.out.println("BEFORE: " + job.name() + ": " + _test_crud.countObjects().join().intValue());
				
				AnalyticTriggerCrudUtils.updateCompletedJob(_test_crud, bucket.full_name(), job.name(), Optional.of("test_host")).join();
				
				//System.out.println(" AFTER: " + job.name() + ": " + _test_crud.countObjects().join().intValue());
			});										

			assertEquals(8L, _test_crud.countObjects().join().intValue());			

			assertEquals(7L, _test_crud.countObjectsBySpec(
					CrudUtils.allOf(AnalyticTriggerStateBean.class)
						.when(AnalyticTriggerStateBean::is_pending, false)
					).join().intValue());												
			
			AnalyticTriggerCrudUtils.deleteActiveBucketRecord(_test_crud, bucket.full_name(), Optional.of("test_host")).join();
			
			assertEquals(7L, _test_crud.countObjects().join().intValue());						
		}
	}
	
	//TODO (ALEPH-12): test 2 different locked_to_host, check they don't interfere...

	//TODO: test list
	// (simple)
	// - updateActiveJobTriggerStatus ... Updates active job records' next check times
	// (more complex)
	// - updateTriggerInputsWhenJobOrBucketCompletes
	
	@Test
	public void test_getTriggersToCheck() throws InterruptedException {
		assertEquals(0, _test_crud.countObjects().join().intValue());
		
		final DataBucketBean bucket = buildBucket("/test/check/triggers", true);
		
		// Just set the test up:
		{
			final Stream<AnalyticTriggerStateBean> test_stream = AnalyticTriggerBeanUtils.generateTriggerStateStream(bucket, false, Optional.empty());
			final List<AnalyticTriggerStateBean> test_list = test_stream.collect(Collectors.toList());
			
			assertEquals(8L, test_list.size());//(8 not 7 because we only dedup at the DB)
	
			final Map<Tuple2<String, String>, List<AnalyticTriggerStateBean>> grouped_triggers
				= test_list.stream().collect(
					Collectors.groupingBy(t -> Tuples._2T(t.bucket_name(), null)));
		
			AnalyticTriggerCrudUtils.storeOrUpdateTriggerStage(_test_crud, grouped_triggers).join();
		
			assertEquals(7L, _test_crud.countObjects().join().intValue());
		}		
		
		// Check the triggers:
		{
			final Map<Tuple2<String, String>, List<AnalyticTriggerStateBean>> res = 
					AnalyticTriggerCrudUtils.getTriggersToCheck(_test_crud).join();
	
			assertEquals("Just one bucket", 1, res.keySet().size());
			
			final List<AnalyticTriggerStateBean> triggers = res.values().stream().findFirst().get();
			assertEquals("One trigger for each resource", 3, triggers.size());
			
			assertTrue("External triggers", triggers.stream().allMatch(trigger -> null != trigger.input_resource_combined()));
			
			// Save the triggers
			
			//DEBUG
			//this.printTriggerDatabase();
			
			AnalyticTriggerCrudUtils.updateTriggerStatuses(_test_crud, triggers.stream(), 
					Date.from(Instant.now().plusSeconds(2)), Optional.empty()
					).join();

			//DEBUG
			//this.printTriggerDatabase();			
		}
		
		// Try again
		{
			final Map<Tuple2<String, String>, List<AnalyticTriggerStateBean>> res = 
					AnalyticTriggerCrudUtils.getTriggersToCheck(_test_crud).join();
	
			assertEquals("None this time", 0, res.keySet().size());
		}		
		
		// Activate the internal jobs and the external triggers, and set the times back
		// (this time will get the job deps but not the triggers)
		
		{
			// activates external with bucket_active
			AnalyticTriggerCrudUtils.updateTriggersWithBucketOrJobActivation(
					_test_crud, bucket, Optional.empty(), Optional.empty()).join();
					
			// activate internal with bucket and job active
			AnalyticTriggerCrudUtils.updateTriggersWithBucketOrJobActivation(
					_test_crud, bucket, Optional.of(bucket.analytic_thread().jobs()), Optional.empty())
					.join();

			//(just update the next trigger time)
			_test_crud.updateObjectsBySpec(CrudUtils.allOf(AnalyticTriggerStateBean.class), 
					Optional.empty(), 
						CrudUtils.update(AnalyticTriggerStateBean.class)
									.set(AnalyticTriggerStateBean::next_check, Date.from(Instant.now().minusSeconds(2)))
					).join();
			
			final Map<Tuple2<String, String>, List<AnalyticTriggerStateBean>> res = 
					AnalyticTriggerCrudUtils.getTriggersToCheck(_test_crud).join();
	
			final List<AnalyticTriggerStateBean> triggers = res.values().stream().findFirst().get();
			assertEquals("One trigger for each job dep", 4, triggers.size());
			
			assertFalse("Should be external triggers", triggers.stream().allMatch(trigger -> null == trigger.job_name()));
			
			AnalyticTriggerCrudUtils.updateTriggerStatuses(_test_crud, triggers.stream(), 
					Date.from(Instant.now().plusSeconds(2)), Optional.empty()
					).join();			
		}
		
		// Try again
		{
			final Map<Tuple2<String, String>, List<AnalyticTriggerStateBean>> res = 
					AnalyticTriggerCrudUtils.getTriggersToCheck(_test_crud).join();
	
			assertEquals("None this time", 0, res.keySet().size());			
		}		
		
		// Activate the jobs "properly"
		
		{
			AnalyticTriggerCrudUtils.createActiveJobRecord(
					_test_crud, 
					bucket, 
					bucket.analytic_thread().jobs().stream().findFirst().get(), 
					Optional.empty()
					).join();
			
			final Map<Tuple2<String, String>, List<AnalyticTriggerStateBean>> res = 
					AnalyticTriggerCrudUtils.getTriggersToCheck(_test_crud).join();
	
			assertEquals("None this time", 0, res.keySet().size());			
			
			//DEBUG
			//this.printTriggerDatabase();
			
			// Reduce the date and try again:
			_test_crud.updateObjectsBySpec(
					CrudUtils.allOf(AnalyticTriggerStateBean.class)
							.when(AnalyticTriggerStateBean::trigger_type, TriggerType.none)
							, 
					Optional.empty(), 
						CrudUtils.update(AnalyticTriggerStateBean.class)
									.set(AnalyticTriggerStateBean::next_check, Date.from(Instant.now().minusSeconds(2)))
					).join();
			
			//DEBUG
			//this.printTriggerDatabase();
			
			final Map<Tuple2<String, String>, List<AnalyticTriggerStateBean>> res2 = 
					AnalyticTriggerCrudUtils.getTriggersToCheck(_test_crud).join();
	
			assertEquals("Got the one active bucket", 1, res2.keySet().size());			
			
			final List<AnalyticTriggerStateBean> triggers = res2.values().stream().findFirst().get();
			assertEquals("One trigger for the one active job + 1 for the bucket", 2, triggers.size());

		}		
	}
	
	//////////////////////////////////////////////////////////////////
	
	//TODO (ALEPH-12): delete bucket, check clears the DB

	//////////////////////////////////////////////////////////////////
	
	public void printTriggerDatabase()
	{
		List<com.fasterxml.jackson.databind.JsonNode> ll = Optionals.streamOf(_test_crud.getRawService().getObjectsBySpec(CrudUtils.allOf())
				.join().iterator(), true)
				.collect(Collectors.toList())
				;
			System.out.println("DB_Resources = \n" + 
					ll.stream().map(t -> t.toString()).collect(Collectors.joining("\n")));		
	}
	
	/** Generates 4 job->job dependency
	 * @param bucket_path
	 * @param trigger
	 * @return
	 */
	public static DataBucketBean buildBucket(final String bucket_path, boolean trigger) {
		
		final AnalyticThreadJobBean job0 = 
				BeanTemplateUtils.build(AnalyticThreadJobBean.class)
					.with(AnalyticThreadJobBean::name, "job0")
				.done().get();
		
		//#INT1: job1a -> job0
		final AnalyticThreadJobBean job1a = 
				BeanTemplateUtils.build(AnalyticThreadJobBean.class)
					.with(AnalyticThreadJobBean::name, "job1a")
					.with(AnalyticThreadJobBean::dependencies, Arrays.asList("job0"))
				.done().get();
		
		//#INT2: job1b -> job0
		final AnalyticThreadJobBean job1b = 
				BeanTemplateUtils.build(AnalyticThreadJobBean.class)
					.with(AnalyticThreadJobBean::name, "job1b")
					.with(AnalyticThreadJobBean::dependencies, Arrays.asList("job0"))
				.done().get();
		
		//#INT3: job2 -> job1a
		//#INT4: job2 -> job1b
		final AnalyticThreadJobBean job2 = 
				BeanTemplateUtils.build(AnalyticThreadJobBean.class)
					.with(AnalyticThreadJobBean::name, "job2")
					.with(AnalyticThreadJobBean::dependencies, Arrays.asList("job1a", "job1b"))
				.done().get();
		
		//Will be ignored as has no deps:
		final AnalyticThreadJobBean job3 = 
				BeanTemplateUtils.build(AnalyticThreadJobBean.class)
					.with(AnalyticThreadJobBean::name, "job3")
				.done().get();

		final AnalyticThreadTriggerBean trigger_info =
			BeanTemplateUtils.build(AnalyticThreadTriggerBean.class)
				.with(AnalyticThreadTriggerBean::auto_calculate, trigger ? null : true)
				.with(AnalyticThreadTriggerBean::trigger, trigger ? buildTrigger() : null)
			.done().get();
		
		final AnalyticThreadBean thread = 
				BeanTemplateUtils.build(AnalyticThreadBean.class)
					.with(AnalyticThreadBean::jobs, Arrays.asList(job0, job1a, job1b, job2, job3))
					.with(AnalyticThreadBean::trigger_config, trigger_info)
				.done().get();
		
		final DataBucketBean bucket =
				BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::_id, bucket_path.replace("/", "_"))
					.with(DataBucketBean::full_name, bucket_path)
					.with(DataBucketBean::analytic_thread, thread)
				.done().get();				
		
		return bucket;
	}
	
	/** Generates 3 triggers (add a 4th)
	 * @return
	 */
	public static AnalyticThreadComplexTriggerBean buildTrigger() {
		
		final AnalyticThreadComplexTriggerBean complex_trigger =
				BeanTemplateUtils.build(AnalyticThreadComplexTriggerBean.class)
					.with(AnalyticThreadComplexTriggerBean::op, TriggerOperator.and)
					.with(AnalyticThreadComplexTriggerBean::dependency_list, Arrays.asList(
						//1
						BeanTemplateUtils.build(AnalyticThreadComplexTriggerBean.class)
							.with(AnalyticThreadComplexTriggerBean::op, TriggerOperator.or)							
							.with(AnalyticThreadComplexTriggerBean::dependency_list, Arrays.asList(
									// OR#1
									BeanTemplateUtils.build(AnalyticThreadComplexTriggerBean.class)
										.with(AnalyticThreadComplexTriggerBean::op, TriggerOperator.and)
										.with(AnalyticThreadComplexTriggerBean::dependency_list, Arrays.asList(
												// #EXT1: input_resource_name_or_id:"/input/test/1/1/1", input_data_service:"search_index_service"
												BeanTemplateUtils.build(AnalyticThreadComplexTriggerBean.class)
													.with(AnalyticThreadComplexTriggerBean::enabled, true)
													.with(AnalyticThreadComplexTriggerBean::type, TriggerType.bucket)
													.with(AnalyticThreadComplexTriggerBean::data_service, "search_index_service")
													.with(AnalyticThreadComplexTriggerBean::resource_name_or_id, "/input/test/1/1/1")
													.with(AnalyticThreadComplexTriggerBean::resource_trigger_limit, 1000L)
												.done().get()
												,
												BeanTemplateUtils.build(AnalyticThreadComplexTriggerBean.class)
													.with(AnalyticThreadComplexTriggerBean::enabled, true)
													.with(AnalyticThreadComplexTriggerBean::op, TriggerOperator.and)
													.with(AnalyticThreadComplexTriggerBean::dependency_list, Arrays.asList(
															// #EXT2: input_resource_name_or_id:"/input/test/1/1/2", input_data_service:"storage_service"
															BeanTemplateUtils.build(AnalyticThreadComplexTriggerBean.class)
																.with(AnalyticThreadComplexTriggerBean::enabled, true)
																.with(AnalyticThreadComplexTriggerBean::type, TriggerType.bucket)
																.with(AnalyticThreadComplexTriggerBean::data_service, "storage_service")
																.with(AnalyticThreadComplexTriggerBean::resource_name_or_id, "/input/test/1/1/2")
																.with(AnalyticThreadComplexTriggerBean::resource_trigger_limit, 1000L)
															.done().get()
															,
															BeanTemplateUtils.build(AnalyticThreadComplexTriggerBean.class)
																.with(AnalyticThreadComplexTriggerBean::enabled, false)
																.with(AnalyticThreadComplexTriggerBean::type, TriggerType.file)
																.with(AnalyticThreadComplexTriggerBean::resource_name_or_id, "/input/test/1/1/3")
																.with(AnalyticThreadComplexTriggerBean::resource_trigger_limit, 1000L)
															.done().get()
															,
															BeanTemplateUtils.build(AnalyticThreadComplexTriggerBean.class) //////////PURE DUP
																.with(AnalyticThreadComplexTriggerBean::enabled, true)
																.with(AnalyticThreadComplexTriggerBean::type, TriggerType.bucket)
																.with(AnalyticThreadComplexTriggerBean::data_service, "search_index_service")
																.with(AnalyticThreadComplexTriggerBean::resource_name_or_id, "/input/test/1/1/1")
																.with(AnalyticThreadComplexTriggerBean::resource_trigger_limit, 1000L)
															.done().get()
													))
												.done().get()
										))
									.done().get()
//Add another one here									
//									,
//									BeanTemplateUtils.build(AnalyticThreadComplexTriggerBean.class)
//									.done().get()
							))
						.done().get()
						,
						//2
						BeanTemplateUtils.build(AnalyticThreadComplexTriggerBean.class)
							.with(AnalyticThreadComplexTriggerBean::enabled, true)
							.with(AnalyticThreadComplexTriggerBean::op, TriggerOperator.not)
							.with(AnalyticThreadComplexTriggerBean::dependency_list, Arrays.asList(
									// #EXT3: "input_resource_name_or_id":"/input/test/1/1/1", no data_service
									BeanTemplateUtils.build(AnalyticThreadComplexTriggerBean.class)////////////DUP WITH DIFF DATA SERVICE
										.with(AnalyticThreadComplexTriggerBean::enabled, true)
										.with(AnalyticThreadComplexTriggerBean::type, TriggerType.bucket)
										.with(AnalyticThreadComplexTriggerBean::resource_name_or_id, "/input/test/1/1/1")
									.done().get()
									,
									BeanTemplateUtils.build(AnalyticThreadComplexTriggerBean.class)
										.with(AnalyticThreadComplexTriggerBean::enabled, false)
										.with(AnalyticThreadComplexTriggerBean::type, TriggerType.bucket)
										.with(AnalyticThreadComplexTriggerBean::resource_name_or_id, "/input/test/2/1:test")
									.done().get()
							))
						.done().get()
						,
						// 3
						BeanTemplateUtils.build(AnalyticThreadComplexTriggerBean.class)
							.with(AnalyticThreadComplexTriggerBean::enabled, false)///////////DISABLED
							.with(AnalyticThreadComplexTriggerBean::op, TriggerOperator.and)
							.with(AnalyticThreadComplexTriggerBean::dependency_list, Arrays.asList(
									BeanTemplateUtils.build(AnalyticThreadComplexTriggerBean.class)
										.with(AnalyticThreadComplexTriggerBean::enabled, true)
										.with(AnalyticThreadComplexTriggerBean::type, TriggerType.file)
										.with(AnalyticThreadComplexTriggerBean::data_service, "storage_service")
										.with(AnalyticThreadComplexTriggerBean::resource_name_or_id, "/input/test/3/1")
										.with(AnalyticThreadComplexTriggerBean::resource_trigger_limit, 1000L)
									.done().get()
									,
									BeanTemplateUtils.build(AnalyticThreadComplexTriggerBean.class)
										.with(AnalyticThreadComplexTriggerBean::enabled, true)
										.with(AnalyticThreadComplexTriggerBean::op, TriggerOperator.and)
										.with(AnalyticThreadComplexTriggerBean::dependency_list, Arrays.asList(
												BeanTemplateUtils.build(AnalyticThreadComplexTriggerBean.class)
													.with(AnalyticThreadComplexTriggerBean::enabled, true)
													.with(AnalyticThreadComplexTriggerBean::type, TriggerType.file)
													.with(AnalyticThreadComplexTriggerBean::data_service, "storage_service")
													.with(AnalyticThreadComplexTriggerBean::resource_name_or_id, "/input/test/3/2")
													.with(AnalyticThreadComplexTriggerBean::resource_trigger_limit, 1000L)
												.done().get()
										))
									.done().get()
							))
						.done().get()
						))
				.done().get();
		
		return complex_trigger;
	}
	
}
