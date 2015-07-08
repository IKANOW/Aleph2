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
package com.ikanow.aleph2.data_model.utils;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IBasicSearchService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.ProjectBean;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent;
import com.ikanow.aleph2.data_model.utils.FutureUtils.ManagementFuture;

/** Utilities for wrapping read only versions of CRUD services
 * @author Alex
 *
 */
public class CrudServiceUtils {

	/** Read only proxy for the CRUD service
	 * @author Alex
	 *
	 * @param <T>
	 */
	public static class ReadOnlyCrudService<T> implements ICrudService.IReadOnlyCrudService<T> {
		final ICrudService<T> _delegate;
		public ReadOnlyCrudService(ICrudService<T> delegate) { _delegate = delegate; }
		/**
		 * @param authorization_fieldname
		 * @param client_auth
		 * @param project_auth
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getFilteredRepo(java.lang.String, java.util.Optional, java.util.Optional)
		 */
		public ICrudService<T> getFilteredRepo(String authorization_fieldname,
				Optional<AuthorizationBean> client_auth,
				Optional<ProjectBean> project_auth) {
			return _delegate.getFilteredRepo(authorization_fieldname,
					client_auth, project_auth).readOnlyVersion();
		}
		/**
		 * @param new_object
		 * @param replace_if_present
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#storeObject(java.lang.Object, boolean)
		 */
		public CompletableFuture<Supplier<Object>> storeObject(T new_object,
				boolean replace_if_present) {
			throw new RuntimeException(ErrorUtils.READ_ONLY_CRUD_SERVICE);
		}
		/**
		 * @param new_object
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#storeObject(java.lang.Object)
		 */
		public CompletableFuture<Supplier<Object>> storeObject(T new_object) {
			throw new RuntimeException(ErrorUtils.READ_ONLY_CRUD_SERVICE);
		}
		/**
		 * @param new_objects
		 * @param replace_if_present
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#storeObjects(java.util.List, boolean)
		 */
		public CompletableFuture<Tuple2<Supplier<List<Object>>, Supplier<Long>>> storeObjects(
				List<T> new_objects, boolean replace_if_present) {
			throw new RuntimeException(ErrorUtils.READ_ONLY_CRUD_SERVICE);
		}
		/**
		 * @param new_objects
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#storeObjects(java.util.List)
		 */
		public CompletableFuture<Tuple2<Supplier<List<Object>>, Supplier<Long>>> storeObjects(
				List<T> new_objects) {
			throw new RuntimeException(ErrorUtils.READ_ONLY_CRUD_SERVICE);
		}
		/**
		 * @param ordered_field_list
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#optimizeQuery(java.util.List)
		 */
		public CompletableFuture<Boolean> optimizeQuery(
				List<String> ordered_field_list) {
			throw new RuntimeException(ErrorUtils.READ_ONLY_CRUD_SERVICE);
		}
		/**
		 * @param ordered_field_list
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deregisterOptimizedQuery(java.util.List)
		 */
		public boolean deregisterOptimizedQuery(List<String> ordered_field_list) {
			throw new RuntimeException(ErrorUtils.READ_ONLY_CRUD_SERVICE);
		}
		/**
		 * @param unique_spec
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
		 */
		public CompletableFuture<Optional<T>> getObjectBySpec(
				QueryComponent<T> unique_spec) {
			return _delegate.getObjectBySpec(unique_spec);
		}
		/**
		 * @param unique_spec
		 * @param field_list
		 * @param include
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.List, boolean)
		 */
		public CompletableFuture<Optional<T>> getObjectBySpec(
				QueryComponent<T> unique_spec, List<String> field_list,
				boolean include) {
			return _delegate.getObjectBySpec(unique_spec, field_list, include);
		}
		/**
		 * @param id
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectById(java.lang.Object)
		 */
		public CompletableFuture<Optional<T>> getObjectById(Object id) {
			return _delegate.getObjectById(id);
		}
		/**
		 * @param id
		 * @param field_list
		 * @param include
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectById(java.lang.Object, java.util.List, boolean)
		 */
		public CompletableFuture<Optional<T>> getObjectById(Object id,
				List<String> field_list, boolean include) {
			return _delegate.getObjectById(id, field_list, include);
		}
		/**
		 * @param spec
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
		 */
		public CompletableFuture<com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.Cursor<T>> getObjectsBySpec(
				QueryComponent<T> spec) {
			return _delegate.getObjectsBySpec(spec);
		}
		/**
		 * @param spec
		 * @param field_list
		 * @param include
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.List, boolean)
		 */
		public CompletableFuture<com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.Cursor<T>> getObjectsBySpec(
				QueryComponent<T> spec, List<String> field_list, boolean include) {
			return _delegate.getObjectsBySpec(spec, field_list, include);
		}
		/**
		 * @param spec
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#countObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
		 */
		public CompletableFuture<Long> countObjectsBySpec(QueryComponent<T> spec) {
			return _delegate.countObjectsBySpec(spec);
		}
		/**
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#countObjects()
		 */
		public CompletableFuture<Long> countObjects() {
			return _delegate.countObjects();
		}
		/**
		 * @param id
		 * @param update
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#updateObjectById(java.lang.Object, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent)
		 */
		public CompletableFuture<Boolean> updateObjectById(Object id,
				UpdateComponent<T> update) {
			throw new RuntimeException(ErrorUtils.READ_ONLY_CRUD_SERVICE);
		}
		/**
		 * @param unique_spec
		 * @param upsert
		 * @param update
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#updateObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.Optional, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent)
		 */
		public CompletableFuture<Boolean> updateObjectBySpec(
				QueryComponent<T> unique_spec, Optional<Boolean> upsert,
				UpdateComponent<T> update) {
			throw new RuntimeException(ErrorUtils.READ_ONLY_CRUD_SERVICE);
		}
		/**
		 * @param spec
		 * @param upsert
		 * @param update
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#updateObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.Optional, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent)
		 */
		public CompletableFuture<Long> updateObjectsBySpec(
				QueryComponent<T> spec, Optional<Boolean> upsert,
				UpdateComponent<T> update) {
			throw new RuntimeException(ErrorUtils.READ_ONLY_CRUD_SERVICE);
		}
		/**
		 * @param unique_spec
		 * @param upsert
		 * @param update
		 * @param before_updated
		 * @param field_list
		 * @param include
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#updateAndReturnObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.Optional, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent, java.util.Optional, java.util.List, boolean)
		 */
		public CompletableFuture<Optional<T>> updateAndReturnObjectBySpec(
				QueryComponent<T> unique_spec, Optional<Boolean> upsert,
				UpdateComponent<T> update, Optional<Boolean> before_updated,
				List<String> field_list, boolean include) {
			throw new RuntimeException(ErrorUtils.READ_ONLY_CRUD_SERVICE);
		}
		/**
		 * @param id
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deleteObjectById(java.lang.Object)
		 */
		public CompletableFuture<Boolean> deleteObjectById(Object id) {
			throw new RuntimeException(ErrorUtils.READ_ONLY_CRUD_SERVICE);
		}
		/**
		 * @param unique_spec
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deleteObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
		 */
		public CompletableFuture<Boolean> deleteObjectBySpec(
				QueryComponent<T> unique_spec) {
			throw new RuntimeException(ErrorUtils.READ_ONLY_CRUD_SERVICE);
		}
		/**
		 * @param spec
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deleteObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
		 */
		public CompletableFuture<Long> deleteObjectsBySpec(
				QueryComponent<T> spec) {
			throw new RuntimeException(ErrorUtils.READ_ONLY_CRUD_SERVICE);
		}
		/**
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deleteDatastore()
		 */
		public CompletableFuture<Boolean> deleteDatastore() {
			throw new RuntimeException(ErrorUtils.READ_ONLY_CRUD_SERVICE);
		}
		/**
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getRawCrudService()
		 */
		public ICrudService<JsonNode> getRawCrudService() {
			return _delegate.getRawCrudService().readOnlyVersion();
		}
		/**
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getSearchService()
		 */
		public Optional<IBasicSearchService<T>> getSearchService() {
			return _delegate.getSearchService();
		}
		/**
		 * @param driver_class
		 * @param driver_options
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getUnderlyingPlatformDriver(java.lang.Class, java.util.Optional)
		 */
		public <X> Optional<X> getUnderlyingPlatformDriver(
				Class<X> driver_class, Optional<String> driver_options) {
			return _delegate.getUnderlyingPlatformDriver(driver_class,
					driver_options);
		}
	}
	
	/** Read only proxy for the CRUD service
	 * @author Alex
	 *
	 * @param <T>
	 */
	public static class ReadOnlyManagementCrudService<T> implements IManagementCrudService.IReadOnlyManagementCrudService<T> {
		final IManagementCrudService<T> _delegate;
		public ReadOnlyManagementCrudService(IManagementCrudService<T> delegate) { _delegate = delegate; }
		/**
		 * @param authorization_fieldname
		 * @param client_auth
		 * @param project_auth
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#getFilteredRepo(java.lang.String, java.util.Optional, java.util.Optional)
		 */
		public IManagementCrudService<T> getFilteredRepo(
				String authorization_fieldname,
				Optional<AuthorizationBean> client_auth,
				Optional<ProjectBean> project_auth) {
			return _delegate.getFilteredRepo(authorization_fieldname,
					client_auth, project_auth).readOnlyVersion();
		}
		/**
		 * @param new_object
		 * @param replace_if_present
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#storeObject(java.lang.Object, boolean)
		 */
		public ManagementFuture<Supplier<Object>> storeObject(T new_object,
				boolean replace_if_present) {
			throw new RuntimeException(ErrorUtils.READ_ONLY_CRUD_SERVICE);
		}
		/**
		 * @param new_object
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#storeObject(java.lang.Object)
		 */
		public ManagementFuture<Supplier<Object>> storeObject(T new_object) {
			throw new RuntimeException(ErrorUtils.READ_ONLY_CRUD_SERVICE);
		}
		/**
		 * @param new_objects
		 * @param continue_on_error
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#storeObjects(java.util.List, boolean)
		 */
		public ManagementFuture<Tuple2<Supplier<List<Object>>, Supplier<Long>>> storeObjects(
				List<T> new_objects, boolean continue_on_error) {
			throw new RuntimeException(ErrorUtils.READ_ONLY_CRUD_SERVICE);
		}
		/**
		 * @param new_objects
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#storeObjects(java.util.List)
		 */
		public ManagementFuture<Tuple2<Supplier<List<Object>>, Supplier<Long>>> storeObjects(
				List<T> new_objects) {
			throw new RuntimeException(ErrorUtils.READ_ONLY_CRUD_SERVICE);
		}
		/**
		 * @param ordered_field_list
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#optimizeQuery(java.util.List)
		 */
		public ManagementFuture<Boolean> optimizeQuery(
				List<String> ordered_field_list) {
			throw new RuntimeException(ErrorUtils.READ_ONLY_CRUD_SERVICE);
		}
		/**
		 * @param unique_spec
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#getObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
		 */
		public ManagementFuture<Optional<T>> getObjectBySpec(
				QueryComponent<T> unique_spec) {
			throw new RuntimeException(ErrorUtils.READ_ONLY_CRUD_SERVICE);
		}
		/**
		 * @param ordered_field_list
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deregisterOptimizedQuery(java.util.List)
		 */
		public boolean deregisterOptimizedQuery(List<String> ordered_field_list) {
			throw new RuntimeException(ErrorUtils.READ_ONLY_CRUD_SERVICE);
		}
		/**
		 * @param unique_spec
		 * @param field_list
		 * @param include
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#getObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.List, boolean)
		 */
		public ManagementFuture<Optional<T>> getObjectBySpec(
				QueryComponent<T> unique_spec, List<String> field_list,
				boolean include) {
			return _delegate.getObjectBySpec(unique_spec, field_list, include);
		}
		/**
		 * @param id
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#getObjectById(java.lang.Object)
		 */
		public ManagementFuture<Optional<T>> getObjectById(Object id) {
			return _delegate.getObjectById(id);
		}
		/**
		 * @param id
		 * @param field_list
		 * @param include
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#getObjectById(java.lang.Object, java.util.List, boolean)
		 */
		public ManagementFuture<Optional<T>> getObjectById(Object id,
				List<String> field_list, boolean include) {
			return _delegate.getObjectById(id, field_list, include);
		}
		/**
		 * @param spec
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#getObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
		 */
		public ManagementFuture<com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.Cursor<T>> getObjectsBySpec(
				QueryComponent<T> spec) {
			return _delegate.getObjectsBySpec(spec);
		}
		/**
		 * @param spec
		 * @param field_list
		 * @param include
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#getObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.List, boolean)
		 */
		public ManagementFuture<com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.Cursor<T>> getObjectsBySpec(
				QueryComponent<T> spec, List<String> field_list, boolean include) {
			return _delegate.getObjectsBySpec(spec, field_list, include);
		}
		/**
		 * @param spec
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#countObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
		 */
		public ManagementFuture<Long> countObjectsBySpec(QueryComponent<T> spec) {
			return _delegate.countObjectsBySpec(spec);
		}
		/**
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#countObjects()
		 */
		public ManagementFuture<Long> countObjects() {
			return _delegate.countObjects();
		}
		/**
		 * @param id
		 * @param update
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#updateObjectById(java.lang.Object, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent)
		 */
		public ManagementFuture<Boolean> updateObjectById(Object id,
				UpdateComponent<T> update) {
			throw new RuntimeException(ErrorUtils.READ_ONLY_CRUD_SERVICE);
		}
		/**
		 * @param unique_spec
		 * @param upsert
		 * @param update
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#updateObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.Optional, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent)
		 */
		public ManagementFuture<Boolean> updateObjectBySpec(
				QueryComponent<T> unique_spec, Optional<Boolean> upsert,
				UpdateComponent<T> update) {
			throw new RuntimeException(ErrorUtils.READ_ONLY_CRUD_SERVICE);
		}
		/**
		 * @param spec
		 * @param upsert
		 * @param update
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#updateObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.Optional, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent)
		 */
		public ManagementFuture<Long> updateObjectsBySpec(
				QueryComponent<T> spec, Optional<Boolean> upsert,
				UpdateComponent<T> update) {
			throw new RuntimeException(ErrorUtils.READ_ONLY_CRUD_SERVICE);
		}
		/**
		 * @param unique_spec
		 * @param upsert
		 * @param update
		 * @param before_updated
		 * @param field_list
		 * @param include
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#updateAndReturnObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.Optional, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent, java.util.Optional, java.util.List, boolean)
		 */
		public ManagementFuture<Optional<T>> updateAndReturnObjectBySpec(
				QueryComponent<T> unique_spec, Optional<Boolean> upsert,
				UpdateComponent<T> update, Optional<Boolean> before_updated,
				List<String> field_list, boolean include) {
			throw new RuntimeException(ErrorUtils.READ_ONLY_CRUD_SERVICE);
		}
		/**
		 * @param id
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#deleteObjectById(java.lang.Object)
		 */
		public ManagementFuture<Boolean> deleteObjectById(Object id) {
			throw new RuntimeException(ErrorUtils.READ_ONLY_CRUD_SERVICE);
		}
		/**
		 * @param unique_spec
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#deleteObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
		 */
		public ManagementFuture<Boolean> deleteObjectBySpec(
				QueryComponent<T> unique_spec) {
			throw new RuntimeException(ErrorUtils.READ_ONLY_CRUD_SERVICE);
		}
		/**
		 * @param spec
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#deleteObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
		 */
		public ManagementFuture<Long> deleteObjectsBySpec(QueryComponent<T> spec) {
			throw new RuntimeException(ErrorUtils.READ_ONLY_CRUD_SERVICE);
		}
		/**
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#deleteDatastore()
		 */
		public ManagementFuture<Boolean> deleteDatastore() {
			throw new RuntimeException(ErrorUtils.READ_ONLY_CRUD_SERVICE);
		}
		/**
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#getRawCrudService()
		 */
		public IManagementCrudService<JsonNode> getRawCrudService() {
			return _delegate.getRawCrudService().readOnlyVersion();
		}
		/**
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getSearchService()
		 */
		public Optional<IBasicSearchService<T>> getSearchService() {
			return _delegate.getSearchService();
		}
		/**
		 * @param driver_class
		 * @param driver_options
		 * @return
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getUnderlyingPlatformDriver(java.lang.Class, java.util.Optional)
		 */
		public <X> Optional<X> getUnderlyingPlatformDriver(
				Class<X> driver_class, Optional<String> driver_options) {
			return _delegate.getUnderlyingPlatformDriver(driver_class,
					driver_options);
		}
	}

}
