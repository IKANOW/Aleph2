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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
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

//TODO (ALEPH-12): looks like i'm not taking is_pending into account in the UUID

public class TestAnalyticTriggerCrudUtils {

	ICrudService<AnalyticTriggerStateBean> _test_crud;
	
	@Before
	public void setup() throws InterruptedException, ExecutionException {
		
		MockMongoDbCrudServiceFactory factory = new MockMongoDbCrudServiceFactory();
		_test_crud = factory.getMongoDbCrudService(AnalyticTriggerStateBean.class, String.class, factory.getMongoDbCollection("test.trigger_crud"), Optional.empty(), Optional.empty(), Optional.empty());
		_test_crud.deleteDatastore().get();
	}	
	
	//TODO (ALEPH-12) testing
	
	@Test
	public void test_storeOrUpdateTriggerStage() throws InterruptedException {
		assertEquals(0, _test_crud.countObjects().join().intValue());
		
		final DataBucketBean bucket = buildBucket("/test/store/trigger", true);

		// Save a bucket
		{		
			//TODO: add tests in TestBeanUtils for other cases?
			final Stream<AnalyticTriggerStateBean> test_stream = AnalyticTriggerBeanUtils.generateTriggerStateStream(bucket, false, Optional.empty());
			final List<AnalyticTriggerStateBean> test_list = test_stream.collect(Collectors.toList());
			
			System.out.println("Resources = \n" + 
					test_list.stream().map(t -> BeanTemplateUtils.toJson(t).toString()).collect(Collectors.joining("\n")));
			
			assertEquals(8L, test_list.size()); //(8 noy 7 cos haven't dedup'd yet)
	
			// 4 internal dependencies
			assertEquals(4L, test_list.stream().filter(t -> null != t.job_name()).count());
			// 5 external dependencies
			assertEquals(4L, test_list.stream().filter(t -> null != t.input_resource_combined()).count());
			
			final Map<Tuple2<String, String>, List<AnalyticTriggerStateBean>> grouped_triggers
				= test_list.stream().collect(
						Collectors.groupingBy(t -> Tuples._2T(t.bucket_name(), null)));
			
			AnalyticTriggerCrudUtils.storeOrUpdateTriggerStage(_test_crud, grouped_triggers).join();
			
			assertEquals(7L, _test_crud.countObjects().join().intValue());
		}		
		
		// Sleep to change times
		Thread.sleep(100L);
		
		// 2) Modify and update
		{
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
			
			final Stream<AnalyticTriggerStateBean> test_stream = AnalyticTriggerBeanUtils.generateTriggerStateStream(mod_bucket, false, Optional.empty());
			final List<AnalyticTriggerStateBean> test_list = test_stream.collect(Collectors.toList());

			final Map<Tuple2<String, String>, List<AnalyticTriggerStateBean>> grouped_triggers
				= test_list.stream().collect(
						Collectors.groupingBy(t -> Tuples._2T(t.bucket_name(), null)));
		
			AnalyticTriggerCrudUtils.storeOrUpdateTriggerStage(_test_crud, grouped_triggers).join();
		
			assertEquals(7L, _test_crud.countObjects().join().intValue());
		
			assertEquals(4L, Optionals.streamOf(
					_test_crud.getObjectsBySpec(CrudUtils.allOf(AnalyticTriggerStateBean.class)).join().iterator()
					,
					false)
					.filter(t -> null != t.job_name())
					.filter(t -> t.job_name().startsWith("test_")).count());
		}
		
		//TODO (ALEPH-12): more validation
		
		//TODO (ALEPH-12): more cases
	}
	
	//////////////////////////////////////////////////////////////////
	
	//TODO (ALEPH-12): this doesn't currently work
	//@Test
	public void test_activateUpdateAndSuspend() throws InterruptedException
	{
		assertEquals(0, _test_crud.countObjects().join().intValue());
		
		final DataBucketBean bucket = buildBucket("/test/active/trigger", true);
		
		
		// 3) Store as above
		{
			final Stream<AnalyticTriggerStateBean> test_stream = AnalyticTriggerBeanUtils.generateTriggerStateStream(bucket, false, Optional.of("test_host"));
			final List<AnalyticTriggerStateBean> test_list = test_stream.collect(Collectors.toList());
			
			System.out.println("Resources = \n" + 
					test_list.stream().map(t -> BeanTemplateUtils.toJson(t).toString()).collect(Collectors.joining("\n")));
			
			assertEquals(8L, test_list.size());//(8 not 7 because we only dedup at the DB)
	
			// 4 internal dependencies
			assertEquals(4L, test_list.stream().filter(t -> null != t.job_name()).count());
			// 5 external dependencies
			assertEquals(4L, test_list.stream().filter(t -> null != t.input_resource_combined()).count());
			
			final Map<Tuple2<String, String>, List<AnalyticTriggerStateBean>> grouped_triggers
				= test_list.stream().collect(
						Collectors.groupingBy(t -> Tuples._2T(t.bucket_name(), null)));
			
			AnalyticTriggerCrudUtils.storeOrUpdateTriggerStage(_test_crud, grouped_triggers).join();
			
			assertEquals(7L, _test_crud.countObjects().join().intValue());
			
		}
		
		// Sleep to change times
		Thread.sleep(100L);
		
		//(activate)
		_test_crud.updateObjectsBySpec(CrudUtils.allOf(AnalyticTriggerStateBean.class), 
				Optional.empty(), 
				CrudUtils.update(AnalyticTriggerStateBean.class)
					.set(AnalyticTriggerStateBean::is_bucket_active, true)
					.set(AnalyticTriggerStateBean::is_job_active, true)
				).join()
				;
		
		// 4) Activate then save suspended - check suspended goes to pending 		
		{
			final Stream<AnalyticTriggerStateBean> test_stream = AnalyticTriggerBeanUtils.generateTriggerStateStream(bucket, true, Optional.of("test_host"));
			final List<AnalyticTriggerStateBean> test_list = test_stream.collect(Collectors.toList());
			
			assertTrue("All suspended", test_list.stream().filter(t -> t.is_bucket_suspended()).findFirst().isPresent());
			
			final Map<Tuple2<String, String>, List<AnalyticTriggerStateBean>> grouped_triggers
			= test_list.stream().collect(
					Collectors.groupingBy(t -> Tuples._2T(t.bucket_name(), null)));
			
			AnalyticTriggerCrudUtils.storeOrUpdateTriggerStage(_test_crud, grouped_triggers).join();
			
			assertEquals(18L, _test_crud.countObjects().join().intValue());			
			assertEquals(9L, _test_crud.countObjectsBySpec(
					CrudUtils.allOf(AnalyticTriggerStateBean.class)
						.when(AnalyticTriggerStateBean::is_bucket_suspended, true)
					).join().intValue());			
		}
		
		// Sleep to change times
		Thread.sleep(100L);
		
		// 5) De-activate and check reverts to pending
		{
			bucket.analytic_thread().jobs().stream().forEach(job -> {
				
				/**/
				System.out.println("BEFORE: " + job.name() + ": " + _test_crud.countObjects().join().intValue());
				AnalyticTriggerCrudUtils.updateCompletedJob(_test_crud, bucket.full_name(), job.name(), Optional.empty()).join();							
				/**/
				System.out.println(" AFTER: " + job.name() + ": " + _test_crud.countObjects().join().intValue());
			});										
			
/**/			
			List<JsonNode> ll = Optionals.streamOf(_test_crud.getRawService().getObjectsBySpec(CrudUtils.allOf())
				.join().iterator(), true)
				.collect(Collectors.toList())
				;
			System.out.println("Resources = \n" + 
					ll.stream().map(t -> t.toString()).collect(Collectors.joining("\n")));
			
			assertEquals(9L, _test_crud.countObjects().join().intValue());			
			
		}
	}
	
	//TODO (ALEPH-12): not building _ids....
	
	//////////////////////////////////////////////////////////////////
	
	//TODO (ALEPH-12): delete bucket, check clears the DB

	//////////////////////////////////////////////////////////////////
	
	/** Generates 4 job->job dependency
	 * @param bucket_path
	 * @param trigger
	 * @return
	 */
	public DataBucketBean buildBucket(final String bucket_path, boolean trigger) {
		
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

		final AnalyticThreadTriggerBean trigger_info =
			BeanTemplateUtils.build(AnalyticThreadTriggerBean.class)
				.with(AnalyticThreadTriggerBean::auto_calculate, trigger ? null : true)
				.with(AnalyticThreadTriggerBean::trigger, trigger ? buildTrigger() : null)
			.done().get();
		
		final AnalyticThreadBean thread = 
				BeanTemplateUtils.build(AnalyticThreadBean.class)
					.with(AnalyticThreadBean::jobs, Arrays.asList(job0, job1a, job1b, job2))
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
	AnalyticThreadComplexTriggerBean buildTrigger() {
		
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
