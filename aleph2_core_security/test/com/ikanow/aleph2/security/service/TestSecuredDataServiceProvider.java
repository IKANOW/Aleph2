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
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.junit.Before;
import org.mockito.Mockito;

import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.MockSecurityService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.MockServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.FutureUtils;
import com.ikanow.aleph2.data_model.utils.Optionals;

public class TestSecuredDataServiceProvider {

	protected MockServiceContext _service_context;
	protected ISecurityService _security_service;
	
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
		
		_service_context = new MockServiceContext();
		_service_context.addService(ISecurityService.class, Optional.empty(), _security_service);
		_service_context.addService(IManagementDbService.class, Optional.empty(), dummy_management_db);
		_service_context.addService(IManagementDbService.class, IManagementDbService.CORE_MANAGEMENT_DB, dummy_management_db);		
	}
	
	@org.junit.Test
	public void testMockitoSetup() {
		
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
}
