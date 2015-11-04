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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISubject;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.BucketUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.security.utils.ErrorUtils;

/** Wraps a data service provider based on the authorizations of the user
 * @author Alex
 */
public class SecuredDataServiceProvider implements IDataServiceProvider {
	protected final IDataServiceProvider _delegate;
	protected final AuthorizationBean _bean;
	protected final IServiceContext _service_context;
	
	/** User ctor
	 * @param service_context
	 * @param delegate
	 * @param bean
	 */
	public SecuredDataServiceProvider(final IServiceContext service_context, final IDataServiceProvider delegate, final AuthorizationBean bean) {
		_service_context = service_context;
		_delegate = delegate;
		_bean = bean;
	}

	/** Wraps a generic data service based on the authorizations of the user
	 * @author Alex
	 */
	public static class SecuredDataService implements IGenericDataService {
		protected final IGenericDataService _delegate;
		protected final AuthorizationBean _bean;
		protected final IServiceContext _service_context;
		protected final ISecurityService _security_service;
		protected final IManagementCrudService<DataBucketBean> _bucket_store;
		protected final ISubject subject; // system user's subject

		
		/** User ctor
		 * @param service_context
		 * @param delegate
		 * @param bean
		 */
		public SecuredDataService(final IServiceContext service_context, final IGenericDataService delegate, final AuthorizationBean bean) {
			_service_context = service_context;
			_delegate = delegate;
			_bean = bean;
			_security_service = _service_context.getSecurityService();			
			
			_bucket_store = _service_context.getCoreManagementDbService().readOnlyVersion().getDataBucketStore().secured(_service_context, _bean);
			
			// Login:
			this.subject = _security_service.loginAsSystem();			
			_security_service.releaseRunAs(subject);		
			_security_service.runAs(subject, Arrays.asList(_bean.getPrincipalName()));			
		}
		
		/** Checks if the user has write permission on this bucket
		 * @return
		 */
		public boolean checkWritePermission(final DataBucketBean bucket) {
			//TODO (ALEPH-41): implement this
			return true;
		}
		
		/** Checks the bucket path for read permission
		 * @param bucket_path
		 * @return
		 */
		public boolean checkReadPermission(final String bucket_path) {
			return _security_service.isPermitted(subject, bucket_path);
		}
		
		/**Checks the bucket for read permission
		 * @param bucket
		 * @return
		 */
		public boolean checkReadPermission(final DataBucketBean bucket) {
			return checkReadPermission(bucket.full_name());
		}
		
		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider.IGenericDataService#getWritableDataService(java.lang.Class, com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Optional, java.util.Optional)
		 */
		@Override
		public <O> Optional<IDataWriteService<O>> getWritableDataService(
				Class<O> clazz, DataBucketBean bucket,
				Optional<String> options, Optional<String> secondary_buffer) {
			return checkWritePermission(bucket)
					? _delegate.getWritableDataService(clazz, bucket, options, secondary_buffer)
					: Optional.empty()
					;
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider.IGenericDataService#getReadableCrudService(java.lang.Class, java.util.Collection, java.util.Optional)
		 */
		@Override
		public <O> Optional<ICrudService<O>> getReadableCrudService(
				final Class<O> clazz, 
				final Collection<DataBucketBean> buckets,
				final Optional<String> options)
		{
			// This is the most complicated case - we're going to check for multi-buckets and only pass validated multi-buckets through
			// This also involves expanding the wildcard

			final Collection<DataBucketBean> verified_buckets =			
				buckets.stream()
					.filter(b -> !checkReadPermission(b))
					.map(b -> {
						return Optional.ofNullable(b.multi_bucket_children())
									.filter(m -> !m.isEmpty())
									.map(m -> {										
										final QueryComponent<DataBucketBean> query = BucketUtils.getApproxMultiBucketQuery(m);
										final Predicate<String> filter = BucketUtils.refineMultiBucketQuery(m);
										
										return Optionals.streamOf(_bucket_store.getObjectsBySpec(query).join().iterator(), false).map(mb -> mb.full_name()).filter(filter);
									})
									.map(m -> {
										return BeanTemplateUtils.clone(b)
												.with(DataBucketBean::multi_bucket_children, m)
												.done()
												;
									})
									.orElse(b)
									;
					})
					.collect(Collectors.toList())
					;
			
			return _delegate.getReadableCrudService(clazz, verified_buckets, options);
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider.IGenericDataService#getSecondaryBuffers(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Optional)
		 */
		@Override
		public Set<String> getSecondaryBuffers(DataBucketBean bucket,
				Optional<String> intermediate_step) {
			return checkReadPermission(bucket)
					? getSecondaryBuffers(bucket, intermediate_step)
					: Collections.emptySet()
					;
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider.IGenericDataService#getPrimaryBufferName(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Optional)
		 */
		@Override
		public Optional<String> getPrimaryBufferName(DataBucketBean bucket,
				Optional<String> intermediate_step) {
			return checkReadPermission(bucket)
					? getPrimaryBufferName(bucket, intermediate_step)
					: Optional.empty()
					;
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider.IGenericDataService#switchCrudServiceToPrimaryBuffer(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Optional, java.util.Optional, java.util.Optional)
		 */
		@Override
		public CompletableFuture<BasicMessageBean> switchCrudServiceToPrimaryBuffer(
				DataBucketBean bucket, Optional<String> secondary_buffer,
				Optional<String> new_name_for_ex_primary,
				Optional<String> intermediate_step) {
			
			return checkWritePermission(bucket)
					? _delegate.switchCrudServiceToPrimaryBuffer(bucket, secondary_buffer, new_name_for_ex_primary, intermediate_step)
					: CompletableFuture.completedFuture(ErrorUtils.buildErrorMessage(this.getClass().getSimpleName(), "switchCrudServiceToPrimaryBuffer", "security"))
					;
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider.IGenericDataService#handleAgeOutRequest(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean)
		 */
		@Override
		public CompletableFuture<BasicMessageBean> handleAgeOutRequest(
				DataBucketBean bucket) {
			
			return checkWritePermission(bucket)
					? _delegate.handleAgeOutRequest(bucket)
					: CompletableFuture.completedFuture(ErrorUtils.buildErrorMessage(this.getClass().getSimpleName(), "switchCrudServiceToPrimaryBuffer", "security"))
					;
		}

		@Override
		public CompletableFuture<BasicMessageBean> handleBucketDeletionRequest(
				DataBucketBean bucket, Optional<String> secondary_buffer,
				boolean bucket_or_buffer_getting_deleted) {
			
			return checkWritePermission(bucket)
					? _delegate.handleAgeOutRequest(bucket)
					: CompletableFuture.completedFuture(ErrorUtils.buildErrorMessage(this.getClass().getSimpleName(), "switchCrudServiceToPrimaryBuffer", "security"))
					;
		}
		
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider#getDataService()
	 */
	@Override
	public Optional<IGenericDataService> getDataService() {
		return _delegate.getDataService().map(service -> new SecuredDataService(_service_context, service, _bean));
	}

}
