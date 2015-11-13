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
package com.ikanow.aleph2.data_model.interfaces.shared_services;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.ProjectBean;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.FutureUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.data_model.utils.FutureUtils.ManagementFuture;

/** Mock implementation of management CRUD (and therefore normal CRUD) for use in unit testing
 *  NOTE: not a  functional database
 * @author Alex
 *
 * @param <T>
 */
public class MockManagementCrudService<T> implements IManagementCrudService<T> {

	protected List<T> _mutable_values = new ArrayList<>();
	
	public List<T> getMockValues() { return _mutable_values; }
	public void setMockValues(List<T> new_values) {
		_mutable_values.clear();
		_mutable_values.addAll(new_values);
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deregisterOptimizedQuery(java.util.List)
	 */
	@Override
	public boolean deregisterOptimizedQuery(List<String> ordered_field_list) {
		return false;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getSearchService()
	 */
	@Override
	public Optional<IBasicSearchService<T>> getSearchService() {
		return Optional.empty();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService#getCrudService()
	 */
	@Override
	public Optional<ICrudService<T>> getCrudService() {
		return Optional.of(this);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService#getUnderlyingPlatformDriver(java.lang.Class, java.util.Optional)
	 */
	@Override
	public <X> Optional<X> getUnderlyingPlatformDriver(Class<X> driver_class,
			Optional<String> driver_options) {
		return Optional.empty();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#getFilteredRepo(java.lang.String, java.util.Optional, java.util.Optional)
	 */
	@Override
	public IManagementCrudService<T> getFilteredRepo(
			String authorization_fieldname,
			Optional<AuthorizationBean> client_auth,
			Optional<ProjectBean> project_auth) {
		return this;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#storeObject(java.lang.Object, boolean)
	 */
	@Override
	public ManagementFuture<Supplier<Object>> storeObject(T new_object,
			boolean replace_if_present) {
		_mutable_values.add(new_object);
		return FutureUtils.createManagementFuture(CompletableFuture.completedFuture(() -> "id"));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#storeObject(java.lang.Object)
	 */
	@Override
	public ManagementFuture<Supplier<Object>> storeObject(T new_object) {
		_mutable_values.add(new_object);
		return FutureUtils.createManagementFuture(CompletableFuture.completedFuture(() -> "id"));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#storeObjects(java.util.List, boolean)
	 */
	@Override
	public ManagementFuture<Tuple2<Supplier<List<Object>>, Supplier<Long>>> storeObjects(
			List<T> new_objects, boolean continue_on_error) {
		
		_mutable_values.addAll(new_objects);
		return FutureUtils.createManagementFuture(CompletableFuture.completedFuture(
				Tuples._2T(() -> new_objects.stream().map(t -> "" + t.hashCode()).collect(Collectors.toList())
						, 
						() -> (long)new_objects.size()
						)))
						;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#storeObjects(java.util.List)
	 */
	@Override
	public ManagementFuture<Tuple2<Supplier<List<Object>>, Supplier<Long>>> storeObjects(
			List<T> new_objects) {
		return this.storeObjects(new_objects, true);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#optimizeQuery(java.util.List)
	 */
	@Override
	public ManagementFuture<Boolean> optimizeQuery(
			List<String> ordered_field_list) {
		return FutureUtils.createManagementFuture(CompletableFuture.completedFuture(ordered_field_list.isEmpty()));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#getObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	@Override
	public ManagementFuture<Optional<T>> getObjectBySpec(
			QueryComponent<T> unique_spec) {
		return this.getObjectBySpec(unique_spec, Arrays.asList(), false);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#getObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.List, boolean)
	 */
	@Override
	public ManagementFuture<Optional<T>> getObjectBySpec(
			QueryComponent<T> unique_spec, List<String> field_list,
			boolean include) {
		// Get the 2nd object to diff from getObjectById
		if (_mutable_values.size() > 1) {
			return FutureUtils.createManagementFuture(CompletableFuture.completedFuture(_mutable_values.stream().skip(1).findFirst()));
		}
		else {
			return FutureUtils.createManagementFuture(CompletableFuture.completedFuture(Optional.empty()));
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#getObjectById(java.lang.Object)
	 */
	@Override
	public ManagementFuture<Optional<T>> getObjectById(Object id) {
		return FutureUtils.createManagementFuture(CompletableFuture.completedFuture(_mutable_values.stream().findFirst()));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#getObjectById(java.lang.Object, java.util.List, boolean)
	 */
	@Override
	public ManagementFuture<Optional<T>> getObjectById(Object id,
			List<String> field_list, boolean include) {
		return FutureUtils.createManagementFuture(CompletableFuture.completedFuture(_mutable_values.stream().findFirst()));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#getObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	@Override
	public ManagementFuture<com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.Cursor<T>> getObjectsBySpec(
			QueryComponent<T> spec) {
		return FutureUtils.createManagementFuture(CompletableFuture.completedFuture(new MyCursor()));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#getObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.List, boolean)
	 */
	@Override
	public ManagementFuture<com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.Cursor<T>> getObjectsBySpec(
			QueryComponent<T> spec, List<String> field_list, boolean include) {
		return FutureUtils.createManagementFuture(CompletableFuture.completedFuture(new MyCursor()));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#countObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	@Override
	public ManagementFuture<Long> countObjectsBySpec(QueryComponent<T> spec) {
		return FutureUtils.createManagementFuture(CompletableFuture.completedFuture((long)_mutable_values.size()));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#countObjects()
	 */
	@Override
	public ManagementFuture<Long> countObjects() {
		//(add 1 so can tell the difference!)
		return FutureUtils.createManagementFuture(CompletableFuture.completedFuture(1L + (long)_mutable_values.size()));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#updateObjectById(java.lang.Object, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent)
	 */
	@Override
	public ManagementFuture<Boolean> updateObjectById(Object id,
			UpdateComponent<T> update) {		
		return FutureUtils.createManagementFuture(CompletableFuture.completedFuture(null != id));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#updateObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.Optional, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent)
	 */
	@Override
	public ManagementFuture<Boolean> updateObjectBySpec(
			QueryComponent<T> unique_spec, Optional<Boolean> upsert,
			UpdateComponent<T> update) {
		return FutureUtils.createManagementFuture(CompletableFuture.completedFuture(true));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#updateObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.Optional, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent)
	 */
	@Override
	public ManagementFuture<Long> updateObjectsBySpec(QueryComponent<T> spec,
			Optional<Boolean> upsert, UpdateComponent<T> update) {
		return FutureUtils.createManagementFuture(CompletableFuture.completedFuture((long)_mutable_values.size()));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#updateAndReturnObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.Optional, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent, java.util.Optional, java.util.List, boolean)
	 */
	@Override
	public ManagementFuture<Optional<T>> updateAndReturnObjectBySpec(
			QueryComponent<T> unique_spec, Optional<Boolean> upsert,
			UpdateComponent<T> update, Optional<Boolean> before_updated,
			List<String> field_list, boolean include) {
		return FutureUtils.createManagementFuture(CompletableFuture.completedFuture(_mutable_values.stream().findFirst()));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#deleteObjectById(java.lang.Object)
	 */
	@Override
	public ManagementFuture<Boolean> deleteObjectById(Object id) {
		if (null == id) {
			return FutureUtils.createManagementFuture(CompletableFuture.completedFuture(false));			
		}
		if (_mutable_values.isEmpty()) {
			_mutable_values.remove(0);
			return FutureUtils.createManagementFuture(CompletableFuture.completedFuture(true));
		}
		else {
			return FutureUtils.createManagementFuture(CompletableFuture.completedFuture(false));			
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#deleteObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	@Override
	public ManagementFuture<Boolean> deleteObjectBySpec(QueryComponent<T> spec) {
		if (!_mutable_values.isEmpty()) {
			_mutable_values.remove(0);
			return FutureUtils.createManagementFuture(CompletableFuture.completedFuture(true));
		}
		else {
			return FutureUtils.createManagementFuture(CompletableFuture.completedFuture(false));			
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#deleteObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	@Override
	public ManagementFuture<Long> deleteObjectsBySpec(QueryComponent<T> spec) {
		final int curr_size = _mutable_values.size();
		_mutable_values.clear();
		return FutureUtils.createManagementFuture(CompletableFuture.completedFuture((long)curr_size));			
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#deleteDatastore()
	 */
	@Override
	public ManagementFuture<Boolean> deleteDatastore() {
		_mutable_values.clear();
		return FutureUtils.createManagementFuture(CompletableFuture.completedFuture(true));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#getRawService()
	 */
	@Override
	public IManagementCrudService<JsonNode> getRawService() {
		final MockManagementCrudService<JsonNode> x = new MockManagementCrudService<>();
		x._mutable_values.addAll(this._mutable_values.stream().map(t -> BeanTemplateUtils.toJson(t)).collect(Collectors.toList()));
		return x;
	}

	protected class MyCursor extends com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.Cursor<T> {

		/* (non-Javadoc)
		 * @see java.lang.Iterable#iterator()
		 */
		@Override
		public Iterator<T> iterator() {
			return _mutable_values.iterator();
		}

		/* (non-Javadoc)
		 * @see java.lang.AutoCloseable#close()
		 */
		@Override
		public void close() throws Exception {
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.Cursor#count()
		 */
		@Override
		public long count() {
			return (long)_mutable_values.size();
		}		
	}	
}
