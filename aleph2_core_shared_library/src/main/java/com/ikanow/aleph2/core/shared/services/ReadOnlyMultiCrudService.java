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
package com.ikanow.aleph2.core.shared.services;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Iterators;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IBasicSearchService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider.IGenericDataService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.ProjectBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.CrudServiceUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.MultiBucketUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent;
import com.ikanow.aleph2.data_model.utils.Lambdas;

/** A wrapper around a number of CRUD services to aggregate their results into a single return stream
 * @author Alex
 */
public class ReadOnlyMultiCrudService<T> implements ICrudService.IReadOnlyCrudService<T> {
	
	//////////////////////////////////////////////////////////////////////////////////////////////////
	
	// INITIALIZATION FOR THIS
	
	/** Returns a multi bucket crud wrapper 
	 *  DOESN'T CURRENTLY SUPPORT LIMITS OR SORTBY PROPERLY
	 * @param buckets - a list of bucket paths
	 * @param maybe_extra_query_builder - for each bucket lets the user specify an additional query to be applied to all queries
	 * @return
	 */
	public static <O> Optional<ReadOnlyMultiCrudService<O>> from(
			final Class<O> clazz,
			final List<String> buckets,
			final Optional<String> owner_id,
			final IGenericDataService data_service,
			final IManagementCrudService<DataBucketBean> bucket_store,
			final IServiceContext service_context,
			final Optional<Function<DataBucketBean, Optional<QueryComponent<O>>>> maybe_extra_query_builder) {

		final DataBucketBean dummy_bucket = 
				BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::owner_id, owner_id.orElse(null))
					.with(DataBucketBean::multi_bucket_children, buckets)
				.done().get();
		
		final List<ICrudService<O>> services =
			MultiBucketUtils.expandMultiBuckets(Arrays.asList(dummy_bucket), bucket_store, service_context)
					.values()
					.stream()
					.map(b -> Tuples._2T(b, data_service.getReadableCrudService(clazz, Arrays.asList(b), Optional.empty())
											.<ICrudService<O>>flatMap(ds -> ds.getCrudService())) 
					)
					.filter(bucket_crud -> bucket_crud._2().isPresent())
					.map(bucket_crud -> Tuples._2T(bucket_crud._1(), bucket_crud._2().get())) // because of above filter)
					.map(bucket_crud -> 
						maybe_extra_query_builder
							.flatMap(qb -> qb.apply(bucket_crud._1()))
							.map(extra_query -> CrudServiceUtils.intercept(clazz, bucket_crud._2(), Optional.of(extra_query), Optional.empty(), Collections.emptyMap(), Optional.empty()))
							.orElse(bucket_crud._2())
					)
					.collect(Collectors.toList())
					;
		
		return services.isEmpty()
				? Optional.empty()
				: Optional.of(new ReadOnlyMultiCrudService<O>(services))
				;
	}

	/** Simple builder for a read only multi crud service set
	 * @param crud_services
	 * @return
	 */
	@SafeVarargs
	public static <O> ReadOnlyMultiCrudService<O> from(ICrudService<O>... crud_services) {
		return new ReadOnlyMultiCrudService<O>(Arrays.asList(crud_services));
	}
	
	
	/** User c'tor (internal only - others use the static builders above)
	 * @param services
	 */
	protected ReadOnlyMultiCrudService(final List<ICrudService<T>> services) {
		_services = services;
	}
	
	//////////////////////////////////////////////////////////////////////////////////////////////////
	
	// STATE
	
	final List<ICrudService<T>> _services;
	
	//////////////////////////////////////////////////////////////////////////////////////////////////
	
	// THE ACTUAL INTERFACE
	
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
		throw new RuntimeException(ErrorUtils.READ_ONLY_CRUD_SERVICE);
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
		return this.getObjectBySpec(unique_spec, Collections.emptyList(), false);
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
		final Stream<CompletableFuture<Optional<T>>> intermed_res1 = _services.stream().map(s -> s.getObjectBySpec(unique_spec, field_list, include));
		
		@SuppressWarnings("unchecked")
		CompletableFuture<Optional<T>>[] intermed_res2 = (CompletableFuture<Optional<T>>[]) intermed_res1.toArray(CompletableFuture[]::new);
		
		return CompletableFuture.allOf(intermed_res2).thenApply(__ -> {
			return Arrays.stream(intermed_res2).map(res -> res.join()).filter(maybe -> maybe.isPresent()).map(maybe -> maybe.get()).findFirst();
		});
	}
	/**
	 * @param id
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectById(java.lang.Object)
	 */
	public CompletableFuture<Optional<T>> getObjectById(Object id) {
		return this.getObjectById(id, Collections.emptyList(), false);
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
		
		final Stream<CompletableFuture<Optional<T>>> intermed_res1 = _services.stream().map(s -> s.getObjectById(id, field_list, include));
		
		@SuppressWarnings("unchecked")
		CompletableFuture<Optional<T>>[] intermed_res2 = (CompletableFuture<Optional<T>>[]) intermed_res1.toArray(CompletableFuture[]::new);
		
		return CompletableFuture.allOf(intermed_res2).thenApply(__ -> {
			return Arrays.stream(intermed_res2).map(res -> res.join()).filter(maybe -> maybe.isPresent()).map(maybe -> maybe.get()).findFirst();
		});
	}
	/**
	 * @param spec
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	public CompletableFuture<com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.Cursor<T>> getObjectsBySpec(
			QueryComponent<T> spec) {
		return this.getObjectsBySpec(spec, Collections.emptyList(), false);
	}
	/**
	 * @param spec
	 * @param field_list
	 * @param include
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.List, boolean)
	 */
	public CompletableFuture<ICrudService.Cursor<T>> getObjectsBySpec(
			QueryComponent<T> spec, List<String> field_list, boolean include) {
		
		final Stream<CompletableFuture<ICrudService.Cursor<T>>> intermed_res1 = _services.stream().map(s -> s.getObjectsBySpec(spec, field_list, include));
		
		@SuppressWarnings("unchecked")
		CompletableFuture<ICrudService.Cursor<T>>[] intermed_res2 = (CompletableFuture<ICrudService.Cursor<T>>[]) intermed_res1.toArray(CompletableFuture[]::new);
		
		return CompletableFuture.allOf(intermed_res2).thenApply(__ -> {
			return new MultiCursor<T>(Arrays.stream(intermed_res2).map(res -> res.join()).collect(Collectors.toList()));
		});
	}
	/**
	 * @param spec
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#countObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	public CompletableFuture<Long> countObjectsBySpec(QueryComponent<T> spec) {		
		final Stream<CompletableFuture<Long>> intermed_res1 = _services.stream().map(s -> s.countObjectsBySpec(spec));
		
		@SuppressWarnings("unchecked")
		CompletableFuture<Long>[] intermed_res2 = (CompletableFuture<Long>[]) intermed_res1.toArray(CompletableFuture[]::new);
		
		return CompletableFuture.allOf(intermed_res2).thenApply(__ -> {
			return Arrays.stream(intermed_res2).map(res -> res.join()).reduce((a, b) -> a + b).orElse(0L);
		});
	}
	/**
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#countObjects()
	 */
	public CompletableFuture<Long> countObjects() {
		final Stream<CompletableFuture<Long>> intermed_res1 = _services.stream().map(s -> s.countObjects());
		
		@SuppressWarnings("unchecked")
		CompletableFuture<Long>[] intermed_res2 = (CompletableFuture<Long>[]) intermed_res1.toArray(CompletableFuture[]::new);
		
		return CompletableFuture.allOf(intermed_res2).thenApply(__ -> {
			return Arrays.stream(intermed_res2).map(res -> res.join()).reduce((a, b) -> a + b).orElse(0L);
		});
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
		return new ReadOnlyMultiCrudService<JsonNode>(_services.stream().map(s -> s.getRawService()).collect(Collectors.toList()));
	}
	/**
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getSearchService()
	 */
	public Optional<IBasicSearchService<T>> getSearchService() {
		return Optional.empty();
	}
	/**
	 * @param driver_class
	 * @param driver_options
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getUnderlyingPlatformDriver(java.lang.Class, java.util.Optional)
	 */
	public <X> Optional<X> getUnderlyingPlatformDriver(
			Class<X> driver_class, Optional<String> driver_options) {
		return Optional.empty();
	}
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService#getCrudService()
	 */
	@Override
	public Optional<ICrudService<T>> getCrudService() {
		return Optional.of(this);
	}	

	//////////////////////////////////////////////////////////////////////////////////////////////////
	
	// UTILITY
	
	public static class MultiCursor<O> extends ICrudService.Cursor<O> {

		/** User c'tor
		 * @param cursors
		 */
		public MultiCursor(final List<Cursor<O>> cursors) {
			_cursors = cursors;
		}
		
		final protected List<Cursor<O>> _cursors;
		
		@Override
		public Iterator<O> iterator() {
			return Iterators.concat(_cursors.stream().map(c -> c.iterator()).iterator());
		}

		@Override
		public void close() throws Exception {
			_cursors.forEach(Lambdas.wrap_consumer_u(c -> c.close()));
		}

		@Override
		public long count() {
			return _cursors.stream().map(c -> c.count()).reduce((a, b) -> a + b).orElse(0L);
		}
		
	}
}
