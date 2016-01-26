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
package com.ikanow.aleph2.data_model.utils;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IBasicSearchService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.ProjectBean;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.SingleQueryComponent;
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
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getRawService()
		 */
		public ICrudService<JsonNode> getRawService() {
			return _delegate.getRawService().readOnlyVersion();
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
		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService#getCrudService()
		 */
		@Override
		public Optional<ICrudService<T>> getCrudService() {
			return Optional.of(this);
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
			return getObjectBySpec(unique_spec, Collections.emptyList(), false);
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
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#getRawService()
		 */
		public IManagementCrudService<JsonNode> getRawService() {
			return _delegate.getRawService().readOnlyVersion();
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
		@Override
		public Optional<ICrudService<T>> getCrudService() {
			return Optional.of(this);
		}
	}

	/** CRUD service proxy that optionally adds an extra term and allows the user to modify the results after they've run (eg to apply security service settings) 
	 * @author Alex
	 */
	@SuppressWarnings("unchecked")
	public static <T> ICrudService<T> intercept(final Class<T> clazz,
												final ICrudService<T> delegate, 
												final Optional<QueryComponent<T>> extra_query, 
												final Optional<Function<QueryComponent<T>, QueryComponent<T>>> query_transform,
												final Map<String, BiFunction<Object, Object[], Object>> interceptors,
												final Optional<BiFunction<Object, Object[], Object>> default_interceptor)
	{		
		InvocationHandler handler = new InvocationHandler() {
			@Override
			public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
				final Method m = delegate.getClass().getMethod(method.getName(), method.getParameterTypes());
				
				// First off, apply the extra term to any relevant args:
				final Object[] args_with_extra_query_pretransform = 						
						query_transform
							.map(q -> {
								return (null != args)
										? Arrays.stream(args)
											.map(o -> 
												(null != o) && QueryComponent.class.isAssignableFrom(o.getClass())
													? q.apply((QueryComponent<T>)o)
													: o
											)
											.collect(Collectors.toList())
											.toArray()
										: args;
							})
							.orElse(args);

				final Object[] args_with_extra_query = 						
						extra_query
							.map(q -> {
								return (null != args_with_extra_query_pretransform)
										? Arrays.stream(args_with_extra_query_pretransform)
											.map(o -> 
												(null != o) && QueryComponent.class.isAssignableFrom(o.getClass())
													? CrudUtils.allOf((QueryComponent<T>)o, q)
													: o
											)
											.collect(Collectors.toList())
											.toArray()
										: args_with_extra_query_pretransform;
							})
							.orElse(args_with_extra_query_pretransform);
				
				// Special cases for: readOnlyVersion, getFilterdRepo / countObjects / getRawService / *byId
				final Object o = Lambdas.get(() -> {
					final SingleQueryComponent<T> base_query = JsonNode.class.equals(clazz)
							? (SingleQueryComponent<T>) CrudUtils.allOf()
							: CrudUtils.allOf(clazz);
					
					try {
						if (extra_query.isPresent() && m.getName().equals("countObjects")) { // special case....change method and apply spec
							return delegate.countObjectsBySpec(extra_query.get());
						}
						else if (extra_query.isPresent() && m.getName().equals("getObjectById")) { // convert from id to spec and append extra_query
							if (1 == args.length) {
								return delegate.getObjectBySpec(CrudUtils.allOf(extra_query.get(), base_query.when("_id", args[0])));
							}
							else {
								return delegate.getObjectBySpec(CrudUtils.allOf(extra_query.get(), base_query.when("_id", args[0])), (List<String>)args[1], (Boolean)args[2]);							
							}
						}
						else if (extra_query.isPresent() && m.getName().equals("deleteDatastore")) {
							CompletableFuture<Long> l = delegate.deleteObjectsBySpec(extra_query.get());
							return l.thenApply(ll -> ll > 0);
						}
						else if (extra_query.isPresent() && m.getName().equals("deleteObjectById")) { // convert from id to spec and append extra_query
							return delegate.deleteObjectBySpec(CrudUtils.allOf(extra_query.get(), base_query.when("_id", args[0])));
						}
						else if (extra_query.isPresent() && m.getName().equals("updateObjectById")) { // convert from id to spec and append extra_query
							return delegate.updateObjectBySpec(CrudUtils.allOf(extra_query.get(), base_query.when("_id", args[0])), Optional.empty(), (UpdateComponent<T>)args[1]);
						}
						else if (m.getName().equals("getRawService")) { // special case....convert the default query to JSON, if present
							Object o_internal = m.invoke(delegate, args_with_extra_query);
							Optional<QueryComponent<JsonNode>> json_extra_query = extra_query.map(qc -> qc.toJson());
							return intercept(JsonNode.class, (ICrudService<JsonNode>)o_internal, json_extra_query, Optional.empty(), interceptors, default_interceptor);
						}
						else { // wrap any CrudService types
							Object o_internal = m.invoke(delegate, args_with_extra_query);
							return (null != o_internal) && ICrudService.class.isAssignableFrom(o_internal.getClass())
									? intercept(clazz, (ICrudService<T>)o_internal, extra_query, Optional.empty(), interceptors, default_interceptor)
									: o_internal;
						}
					}
					catch (IllegalAccessException ee) {
						throw new RuntimeException(ee);
					}
					catch (InvocationTargetException e) {
						throw new RuntimeException(e.getCause().getMessage(), e);
					}
				});				
				
				return interceptors.getOrDefault(m.getName(), 
										default_interceptor.orElse(CrudServiceUtils::identityInterceptor))
									.apply(o, args_with_extra_query);
			}
		};

		return ICrudService.IReadOnlyCrudService.class.isAssignableFrom(delegate.getClass())
				?
				(ICrudService<T>)Proxy.newProxyInstance(ICrudService.IReadOnlyCrudService.class.getClassLoader(),
								new Class[] { ICrudService.IReadOnlyCrudService.class }, handler)
				:
				(ICrudService<T>)Proxy.newProxyInstance(ICrudService.class.getClassLoader(),
								new Class[] { ICrudService.class }, handler)
				;
	}
	
	/** Utility function - just returns ret_val
	 * @param ret_val
	 * @param ignore
	 * @return
	 */
	private static Object identityInterceptor(Object ret_val, Object[] ignore) {
		return ret_val;
	}
	
}
