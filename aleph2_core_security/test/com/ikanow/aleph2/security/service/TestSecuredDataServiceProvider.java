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
 ******************************************************************************/
package com.ikanow.aleph2.security.service;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.junit.Before;
import org.mockito.Mockito;

import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.MockSecurityService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.MockServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.FutureUtils;
import com.ikanow.aleph2.data_model.utils.Optionals;

public class TestSecuredDataServiceProvider {

	protected MockServiceContext _service_context;
	protected MockSecurityService _security_service;
	
	public static class MutableIterator implements Iterator<DataBucketBean> {
		protected Iterator<DataBucketBean> _mutable_delegate;
		
		public void resetAndFill(List<DataBucketBean> buckets) {
			_mutable_delegate = buckets.iterator();
		}
		
		@Override
		public boolean hasNext() {
			return _mutable_delegate.hasNext();
		}

		@Override
		public DataBucketBean next() {
			return _mutable_delegate.next();
		}
		
	}
	protected MutableIterator _mutable_iterator = new MutableIterator();
	
	@Before
	public void setup() {
		
		@SuppressWarnings("unchecked")
		ICrudService.Cursor<DataBucketBean> cursor = (ICrudService.Cursor<DataBucketBean>) Mockito.mock(ICrudService.Cursor.class);
		Mockito.when(cursor.iterator()).thenReturn(_mutable_iterator);
		
		@SuppressWarnings("unchecked")
		IManagementCrudService<DataBucketBean> dummy_bucket_db = (IManagementCrudService<DataBucketBean>) Mockito.mock(IManagementCrudService.class);
		Mockito.when(dummy_bucket_db.getObjectsBySpec(Mockito.any())).thenReturn(FutureUtils.createManagementFuture(CompletableFuture.completedFuture(cursor)));
		Mockito.when(dummy_bucket_db.secured(Mockito.any(), Mockito.any())).thenReturn(dummy_bucket_db);
		
		final IManagementDbService dummy_management_db = Mockito.mock(IManagementDbService.class);
		Mockito.when(dummy_management_db.getDataBucketStore()).thenReturn(dummy_bucket_db);
		Mockito.when(dummy_management_db.readOnlyVersion()).thenReturn(dummy_management_db);
		
		_security_service = new MockSecurityService();		
		_security_service.setGlobalMockRole("/test/allowed", true);
		_security_service.setGlobalMockRole("/test/not_allowed", false);
		_security_service.setGlobalMockRole("/test/missing", false);
		_security_service.setGlobalMockRole("/fixed/1", true);
		_security_service.setGlobalMockRole("/fixed/1b", true);
		_security_service.setGlobalMockRole("/fixed/2", false);
		_security_service.setGlobalMockRole("/multi/1", true);
		_security_service.setGlobalMockRole("/multi/1b", true);
		_security_service.setGlobalMockRole("/multi/only/disallowed", true);
		_security_service.setGlobalMockRole("/multi/2", false);
		_security_service.setGlobalMockRole("/allowed/1", true);
		
		_service_context = new MockServiceContext();
		_service_context.addService(ISecurityService.class, Optional.empty(), _security_service);
		_service_context.addService(IManagementDbService.class, Optional.empty(), dummy_management_db);
		_service_context.addService(IManagementDbService.class, IManagementDbService.CORE_MANAGEMENT_DB, dummy_management_db);		
	}
	
	@org.junit.Test
	public void test_MockitoSetup() {
		
		_mutable_iterator.resetAndFill(Arrays.asList(
				BeanTemplateUtils.build(DataBucketBean.class)				
					.with(DataBucketBean::full_name, "/test")
				.done().get()
				));
		
		final Iterator<DataBucketBean> it = _service_context.getCoreManagementDbService().getDataBucketStore().getObjectsBySpec(CrudUtils.allOf(DataBucketBean.class)).join().iterator();
		
		assertEquals(Arrays.asList("/test"),
				Optionals.streamOf(it, false)
						.map(b -> b.full_name())
						.collect(Collectors.toList())						
				);
		
		final Iterator<DataBucketBean> it2 = _service_context.getCoreManagementDbService().getDataBucketStore().getObjectsBySpec(CrudUtils.allOf(DataBucketBean.class)).join().iterator();
		
		assertEquals(Arrays.asList(),
				Optionals.streamOf(it2, false)
						.map(b -> b.full_name())
						.collect(Collectors.toList())						
				);

		
		_mutable_iterator.resetAndFill(Arrays.asList(
				BeanTemplateUtils.build(DataBucketBean.class)				
					.with(DataBucketBean::full_name, "/test/2")
				.done().get()
				));
		
		final Iterator<DataBucketBean> it3 = _service_context.getCoreManagementDbService().getDataBucketStore().secured(_service_context, null).getObjectsBySpec(CrudUtils.allOf(DataBucketBean.class)).join().iterator();
		
		assertEquals(Arrays.asList("/test/2"),
				Optionals.streamOf(it3, false)
						.map(b -> b.full_name())
						.collect(Collectors.toList())						
				);		
	}

	@org.junit.Test
	public void test_miscCalls() {
		// Tests code that hasn't been plumbed in yet for coverage
		
		// Services
		
		final AuthorizationBean auth_bean = new AuthorizationBean("test_user");
		final DummyGenericDataService test_harness = new DummyGenericDataService();
		
		final IDataServiceProvider provider = Mockito.mock(IDataServiceProvider.class);
		Mockito.when(provider.getDataService()).thenReturn(Optional.of(test_harness));
		Mockito.when(provider.secured(Mockito.any(), Mockito.any())).thenReturn(new SecuredDataServiceProvider(_service_context, provider, auth_bean));

		final IDataServiceProvider secure_provider = provider.secured(_service_context, auth_bean);

		// Buckets
		
		final DataBucketBean bucket = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::full_name, "/test/allowed")
				.done().get()
				;
		final DataBucketBean bucket_2 = BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::full_name, "/test/not_allowed")
			.done().get()
			;
		final DataBucketBean bucket_3 = BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::full_name, "/test/missing")
			.done().get()
			;

		// Tests
		
		// No writes allowed currently
		assertEquals(Optional.empty(), secure_provider.getDataService().get().getWritableDataService(String.class, bucket, Optional.empty(), Optional.empty()));		
		assertEquals(false, secure_provider.getDataService().get().switchCrudServiceToPrimaryBuffer(bucket, Optional.empty(), Optional.empty(), Optional.empty()).join().success());
		assertEquals(false, secure_provider.getDataService().get().handleAgeOutRequest(bucket).join().success());
		assertEquals(false, secure_provider.getDataService().get().handleBucketDeletionRequest(bucket, Optional.empty(), false).join().success());

		// Reads		
		assertEquals(null, secure_provider.getDataService().get().getPrimaryBufferName(bucket, Optional.empty()));
		assertEquals(null, secure_provider.getDataService().get().getSecondaryBuffers(bucket, Optional.empty()));
		assertEquals(Optional.empty(), secure_provider.getDataService().get().getPrimaryBufferName(bucket_2, Optional.empty()));
		assertEquals(Collections.emptySet(), secure_provider.getDataService().get().getSecondaryBuffers(bucket_2, Optional.empty()));
		assertEquals(Optional.empty(), secure_provider.getDataService().get().getPrimaryBufferName(bucket_3, Optional.empty()));
		assertEquals(Collections.emptySet(), secure_provider.getDataService().get().getSecondaryBuffers(bucket_3, Optional.empty()));
	}
	
	@org.junit.Test
	public void test_getReadableCrudService() {
		
		// Services
		
		final AuthorizationBean auth_bean = new AuthorizationBean("test_user");
		final DummyGenericDataService test_harness = new DummyGenericDataService();
		
		final IDataServiceProvider provider = Mockito.mock(IDataServiceProvider.class);
		Mockito.when(provider.getDataService()).thenReturn(Optional.of(test_harness));
		Mockito.when(provider.secured(Mockito.any(), Mockito.any())).thenReturn(new SecuredDataServiceProvider(_service_context, provider, auth_bean));

		final IDataServiceProvider secure_provider = provider.secured(_service_context, auth_bean);

		// Set up buckets
		
		final DataBucketBean child_bucket_allowed = BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::full_name, "/test/allowed")
			.done().get()
			;
		@SuppressWarnings("unused")
		final DataBucketBean child_bucket_not1 = BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::full_name, "/test/not_allowed")
			.done().get()
			;
		@SuppressWarnings("unused")
		final DataBucketBean child_bucket_not2 = BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::full_name, "/test/missing")
			.done().get()
			;
		
		final DataBucketBean allowed_1 = BeanTemplateUtils.build(DataBucketBean.class) //allowed but never referenced from anything allowed
				.with(DataBucketBean::full_name, "/allowed/1")
				.with(DataBucketBean::multi_bucket_children, new HashSet<String>())
			.done().get()
			;
		final DataBucketBean fixed_1 = BeanTemplateUtils.build(DataBucketBean.class) //allowed
				.with(DataBucketBean::full_name, "/fixed/1")
				.with(DataBucketBean::multi_bucket_children, new HashSet<String>())
			.done().get()
			;
		final DataBucketBean fixed_1b = BeanTemplateUtils.build(DataBucketBean.class) //allowed
				.with(DataBucketBean::full_name, "/fixed/1b")
			.done().get()
			;
		final DataBucketBean fixed_2 = BeanTemplateUtils.build(DataBucketBean.class) //not
				.with(DataBucketBean::full_name, "/fixed/2")
			.done().get()
			;
		final DataBucketBean fixed_3 = BeanTemplateUtils.build(DataBucketBean.class)//not
				.with(DataBucketBean::full_name, "/fixed/3")
			.done().get()
			;

		final DataBucketBean multi_1 = BeanTemplateUtils.build(DataBucketBean.class) //allowed
				.with(DataBucketBean::full_name, "/multi/1")
				.with(DataBucketBean::multi_bucket_children, new HashSet<String>(Arrays.asList("/test/**"))) //(would be allowed)
			.done().get()
			;
		
		final DataBucketBean multi_1b = BeanTemplateUtils.build(DataBucketBean.class) //allowed
				//(no full name - this gets treated as a dumb container ie no auth is needed)
				.with(DataBucketBean::multi_bucket_children, new HashSet<String>(Arrays.asList("/allowed/1")))
			.done().get()
			;
		final DataBucketBean multi_2 = BeanTemplateUtils.build(DataBucketBean.class) //not allowed
				.with(DataBucketBean::full_name, "/multi/2")
				.with(DataBucketBean::multi_bucket_children, new HashSet<String>(Arrays.asList("/allowed/1"))) //(would be allowed)
			.done().get()
			;
		final DataBucketBean multi_3 = BeanTemplateUtils.build(DataBucketBean.class) //not allowed
				.with(DataBucketBean::full_name, "/multi/3")
				.with(DataBucketBean::multi_bucket_children, new HashSet<String>(Arrays.asList("/allowed/1"))) //(would be allowed)
			.done().get()
			;
		final DataBucketBean multi_only_disallowed = BeanTemplateUtils.build(DataBucketBean.class) //not allowed
				.with(DataBucketBean::full_name, "/multi/only/disallowed")
				.with(DataBucketBean::multi_bucket_children, new HashSet<String>(Arrays.asList("/test/not*", "/test/missing"))) //(would be allowed)
			.done().get()
			;
		
		// Dummy return from DB
		
		_mutable_iterator.resetAndFill(Arrays.asList(
				child_bucket_allowed, allowed_1
				));

		
		// Test
		
		// Nothing in, nothing out
		secure_provider.getDataService().get().getReadableCrudService(String.class, Arrays.asList(), Optional.empty());
		assertEquals(Arrays.asList(), test_harness.normal_buckets);
		assertEquals(Arrays.asList(), test_harness.multi_buckets);
		
		// Here's the main test
		secure_provider.getDataService().get().getReadableCrudService(String.class, Arrays.asList(
				fixed_1, fixed_1b, fixed_2, fixed_3, multi_1, multi_1b, multi_2, multi_3
				), Optional.empty());
		assertEquals(Arrays.asList("/fixed/1", "/fixed/1b", "/multi/1"), test_harness.normal_buckets);
		assertEquals(Arrays.asList("/test/allowed"), test_harness.multi_buckets);

		// All multi buckets not allowed
		secure_provider.getDataService().get().getReadableCrudService(String.class, Arrays.asList(multi_only_disallowed), Optional.empty());
		assertEquals(Arrays.asList("/multi/only/disallowed"), test_harness.normal_buckets);
		assertEquals(Arrays.asList(), test_harness.multi_buckets);
		
		// Only unauth buckets
		secure_provider.getDataService().get().getReadableCrudService(String.class, Arrays.asList(multi_3, fixed_2), Optional.empty());
		assertEquals(Arrays.asList(), test_harness.normal_buckets);
		assertEquals(Arrays.asList(), test_harness.multi_buckets);
	}	
	
	////////////////////////////////////////////////
	
	public static class DummyGenericDataService implements IDataServiceProvider.IGenericDataService {

		public List<String> normal_buckets = new LinkedList<>();
		public List<String> multi_buckets = new LinkedList<>();
		
		@Override
		public <O> Optional<IDataWriteService<O>> getWritableDataService(
				Class<O> clazz, DataBucketBean bucket,
				Optional<String> options, Optional<String> secondary_buffer) {
			return null;
		}

		@Override
		public <O> Optional<ICrudService<O>> getReadableCrudService(
				Class<O> clazz, Collection<DataBucketBean> buckets,
				Optional<String> options) {
			
			normal_buckets.clear();
			multi_buckets.clear();
			
			normal_buckets.addAll(buckets.stream().map(b -> b.full_name()).collect(Collectors.toList()));
			multi_buckets.addAll(buckets.stream().flatMap(b -> Optionals.ofNullable(b.multi_bucket_children()).stream()).collect(Collectors.toList()));
			
			return null;
		}

		@Override
		public Set<String> getSecondaryBuffers(DataBucketBean bucket,
				Optional<String> intermediate_step) {
			return null;
		}

		@Override
		public Optional<String> getPrimaryBufferName(DataBucketBean bucket,
				Optional<String> intermediate_step) {
			return null;
		}

		@Override
		public CompletableFuture<BasicMessageBean> switchCrudServiceToPrimaryBuffer(
				DataBucketBean bucket, Optional<String> secondary_buffer,
				Optional<String> new_name_for_ex_primary,
				Optional<String> intermediate_step) {
			return null;
		}

		@Override
		public CompletableFuture<BasicMessageBean> handleAgeOutRequest(
				DataBucketBean bucket) {
			return null;
		}

		@Override
		public CompletableFuture<BasicMessageBean> handleBucketDeletionRequest(
				DataBucketBean bucket, Optional<String> secondary_buffer,
				boolean bucket_or_buffer_getting_deleted) {
			return null;
		}
		
	}
}
