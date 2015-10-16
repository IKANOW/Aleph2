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
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;

import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean.AnalyticThreadComplexTriggerBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean.AnalyticThreadComplexTriggerBean.TriggerOperator;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean.AnalyticThreadComplexTriggerBean.TriggerType;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerStateBean;
import com.ikanow.aleph2.shared.crud.mongodb.services.MockMongoDbCrudServiceFactory;

//TODO (ALEPH-12): in bucket change actor need to activate the bucket if any jobs manually triggered...

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
	public void test_storeOrUpdateTriggerStage() {
		assertEquals(0, _test_crud.countObjects().join().intValue());
		
		final DataBucketBean bucket = buildBucket("/test/store/trigger", true);
		
		//TODO: add tests in TestBeanUtils for other cases?
		final Stream<AnalyticTriggerStateBean> test_stream = AnalyticTriggerBeanUtils.generateTriggerStateStream(bucket, false, Optional.empty());
		final List<AnalyticTriggerStateBean> test_list = test_stream.collect(Collectors.toList());
		
		System.out.println("Resources = " + test_list.stream().map(t -> t.input_resource_combined() + "/" + t.input_data_service() + "/j=" + t.job_name()).collect(Collectors.joining(";")));
		
		assertEquals(9L, test_list.size());

		// 4 internal dependencies
		assertEquals(4L, test_list.stream().filter(t -> null != t.job_name()).count());
		// 5 external dependencies
		assertEquals(5L, test_list.stream().filter(t -> null != t.input_resource_name_or_id()).count());
		
		//TODO (ALEPH-12): Save to mongodb and validate
	}
	
	//////////////////////////////////////////////////////////////////
	
	public DataBucketBean buildBucket(final String bucket_path, boolean trigger) {
		
		final AnalyticThreadJobBean job0 = 
				BeanTemplateUtils.build(AnalyticThreadJobBean.class)
					.with(AnalyticThreadJobBean::name, "job0")
				.done().get();
		
		final AnalyticThreadJobBean job1a = 
				BeanTemplateUtils.build(AnalyticThreadJobBean.class)
					.with(AnalyticThreadJobBean::name, "job1a")
					.with(AnalyticThreadJobBean::dependencies, Arrays.asList("job0"))
				.done().get();
		
		final AnalyticThreadJobBean job1b = 
				BeanTemplateUtils.build(AnalyticThreadJobBean.class)
					.with(AnalyticThreadJobBean::name, "job1b")
					.with(AnalyticThreadJobBean::dependencies, Arrays.asList("job0"))
				.done().get();
		
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
	
	AnalyticThreadComplexTriggerBean buildTrigger() {
		
		final AnalyticThreadComplexTriggerBean complex_trigger =
				BeanTemplateUtils.build(AnalyticThreadComplexTriggerBean.class)
					.with(AnalyticThreadComplexTriggerBean::op, TriggerOperator.and)
					.with(AnalyticThreadComplexTriggerBean::dependency_list, Arrays.asList(
						//1
						BeanTemplateUtils.build(AnalyticThreadComplexTriggerBean.class)
							.with(AnalyticThreadComplexTriggerBean::op, TriggerOperator.or)
							.with(AnalyticThreadComplexTriggerBean::dependency_list, Arrays.asList(
									BeanTemplateUtils.build(AnalyticThreadComplexTriggerBean.class)
										.with(AnalyticThreadComplexTriggerBean::op, TriggerOperator.and)
										.with(AnalyticThreadComplexTriggerBean::dependency_list, Arrays.asList(
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
									,
									BeanTemplateUtils.build(AnalyticThreadComplexTriggerBean.class)
									.done().get()
							))
						.done().get()
						,
						//2
						BeanTemplateUtils.build(AnalyticThreadComplexTriggerBean.class)
							.with(AnalyticThreadComplexTriggerBean::enabled, true)
							.with(AnalyticThreadComplexTriggerBean::op, TriggerOperator.not)
							.with(AnalyticThreadComplexTriggerBean::dependency_list, Arrays.asList(
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
