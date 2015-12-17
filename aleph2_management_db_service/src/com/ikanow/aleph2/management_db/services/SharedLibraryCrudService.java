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
package com.ikanow.aleph2.management_db.services;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IBasicSearchService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.MockServiceContext;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.ProjectBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.FutureUtils;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent;
import com.ikanow.aleph2.data_model.utils.FutureUtils.ManagementFuture;
import com.ikanow.aleph2.data_model.utils.SetOnce;

public class SharedLibraryCrudService implements IManagementCrudService<SharedLibraryBean> {
	@SuppressWarnings("unused")
	private static final Logger _logger = LogManager.getLogger();	

	protected final Provider<IStorageService> _storage_service;	
	protected final Provider<IManagementDbService> _underlying_management_db;
	protected final SetOnce<ICrudService<SharedLibraryBean>> _underlying_library_db = new SetOnce<>();
	
	/** Guice invoked constructor
	 */
	@Inject
	public SharedLibraryCrudService(final IServiceContext service_context) {
		_underlying_management_db = service_context.getServiceProvider(IManagementDbService.class, Optional.empty()).get();
		_storage_service = service_context.getServiceProvider(IStorageService.class, Optional.empty()).get();
		ModuleUtils.getAppInjector().thenRun(() -> {
			// (work around for guice initialization)
			_underlying_library_db.set(_underlying_management_db.get().getSharedLibraryStore());
			
			// Handle some simple optimization of the data bucket CRUD repo:
			Executors.newSingleThreadExecutor().submit(() -> {
				_underlying_library_db.get().optimizeQuery(Arrays.asList(
						BeanTemplateUtils.from(SharedLibraryBean.class).field(SharedLibraryBean::path_name)));
			});				
		});		
	}
	
	/** User constructor, for wrapping
	 */
	public SharedLibraryCrudService(final Provider<IManagementDbService> underlying_management_db, 
			final Provider<IStorageService> storage_service,
			final ICrudService<SharedLibraryBean> underlying_library_db
			)
	{
		_underlying_management_db = underlying_management_db;
		_storage_service = storage_service;
		_underlying_library_db.set(underlying_library_db);
	}
	
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deregisterOptimizedQuery(java.util.List)
	 */
	@Override
	public boolean deregisterOptimizedQuery(
			List<String> ordered_field_list) {
		return _underlying_library_db.get().deregisterOptimizedQuery(ordered_field_list);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getSearchService()
	 */
	@Override
	public Optional<IBasicSearchService<SharedLibraryBean>> getSearchService() {
		return _underlying_library_db.get().getSearchService();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService#getUnderlyingPlatformDriver(java.lang.Class, java.util.Optional)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <T> Optional<T> getUnderlyingPlatformDriver(
			Class<T> driver_class, Optional<String> driver_options) {
		if (driver_class == ICrudService.class) {
			return (Optional<T>) Optional.of(_underlying_library_db.get());
		}
		else {
			throw new RuntimeException("SharedLibraryCrudService.getUnderlyingPlatformDriver not supported");
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#getFilteredRepo(java.lang.String, java.util.Optional, java.util.Optional)
	 */
	@Override
	public IManagementCrudService<SharedLibraryBean> getFilteredRepo(
			String authorization_fieldname,
			Optional<AuthorizationBean> client_auth,
			Optional<ProjectBean> project_auth) {
		return new SharedLibraryCrudService(
				new MockServiceContext.MockProvider<IManagementDbService>(_underlying_management_db.get().getFilteredDb(client_auth, project_auth)), 
				_storage_service,
				_underlying_library_db.get().getFilteredRepo(authorization_fieldname, client_auth, project_auth));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#storeObject(java.lang.Object, boolean)
	 */
	@Override
	public ManagementFuture<Supplier<Object>> storeObject(
			SharedLibraryBean new_object, boolean replace_if_present) {
		//TODO (ALEPH-19): convert this into an update, ie get old version, compare and overwrite
		return FutureUtils.createManagementFuture(_underlying_library_db.get().storeObject(new_object, replace_if_present));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#storeObject(java.lang.Object)
	 */
	@Override
	public ManagementFuture<Supplier<Object>> storeObject(
			SharedLibraryBean new_object) {
		return this.storeObject(new_object, false);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#storeObjects(java.util.List, boolean)
	 */
	@Override
	public ManagementFuture<Tuple2<Supplier<List<Object>>, Supplier<Long>>> storeObjects(
			List<SharedLibraryBean> new_objects,
			boolean continue_on_error) {
		if (continue_on_error) {
			throw new RuntimeException("Can't call storeObjects with continue_on_error: true, use update instead");
		}
		return FutureUtils.createManagementFuture(_underlying_library_db.get().storeObjects(new_objects, continue_on_error));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#storeObjects(java.util.List)
	 */
	@Override
	public ManagementFuture<Tuple2<Supplier<List<Object>>, Supplier<Long>>> storeObjects(
			List<SharedLibraryBean> new_objects) {
		return this.storeObjects(new_objects, false);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#optimizeQuery(java.util.List)
	 */
	@Override
	public ManagementFuture<Boolean> optimizeQuery(
			List<String> ordered_field_list) {
		return FutureUtils.createManagementFuture(_underlying_library_db.get().optimizeQuery(ordered_field_list));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#getObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	@Override
	public ManagementFuture<Optional<SharedLibraryBean>> getObjectBySpec(
			QueryComponent<SharedLibraryBean> unique_spec) {
		return FutureUtils.createManagementFuture(_underlying_library_db.get().getObjectBySpec(unique_spec));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#getObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.List, boolean)
	 */
	@Override
	public ManagementFuture<Optional<SharedLibraryBean>> getObjectBySpec(
			QueryComponent<SharedLibraryBean> unique_spec,
			List<String> field_list, boolean include) {
		return FutureUtils.createManagementFuture(_underlying_library_db.get().getObjectBySpec(unique_spec, field_list, include));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#getObjectById(java.lang.Object)
	 */
	@Override
	public ManagementFuture<Optional<SharedLibraryBean>> getObjectById(
			Object id) {
		return FutureUtils.createManagementFuture(_underlying_library_db.get().getObjectById(id));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#getObjectById(java.lang.Object, java.util.List, boolean)
	 */
	@Override
	public ManagementFuture<Optional<SharedLibraryBean>> getObjectById(
			Object id, List<String> field_list,
			boolean include) {
		return FutureUtils.createManagementFuture(_underlying_library_db.get().getObjectById(id, field_list, include));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#getObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	@Override
	public ManagementFuture<Cursor<SharedLibraryBean>> getObjectsBySpec(
			QueryComponent<SharedLibraryBean> spec) {
		return FutureUtils.createManagementFuture(_underlying_library_db.get().getObjectsBySpec(spec));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#getObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.List, boolean)
	 */
	@Override
	public ManagementFuture<Cursor<SharedLibraryBean>> getObjectsBySpec(
			QueryComponent<SharedLibraryBean> spec,
			List<String> field_list, boolean include) {
		return FutureUtils.createManagementFuture(_underlying_library_db.get().getObjectsBySpec(spec, field_list, include));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#countObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	@Override
	public ManagementFuture<Long> countObjectsBySpec(
			QueryComponent<SharedLibraryBean> spec) {
		return FutureUtils.createManagementFuture(_underlying_library_db.get().countObjectsBySpec(spec));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#countObjects()
	 */
	@Override
	public ManagementFuture<Long> countObjects() {
		return FutureUtils.createManagementFuture(_underlying_library_db.get().countObjects());
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#updateObjectById(java.lang.Object, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent)
	 */
	@Override
	public ManagementFuture<Boolean> updateObjectById(
			Object id,
			UpdateComponent<SharedLibraryBean> update) {
		// TODO limited in what can change?
		throw new RuntimeException(ErrorUtils.get(ErrorUtils.NOT_YET_IMPLEMENTED, "SharedLibraryService.updateObjectById"));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#updateObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.Optional, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent)
	 */
	@Override
	public ManagementFuture<Boolean> updateObjectBySpec(
			QueryComponent<SharedLibraryBean> unique_spec,
			Optional<Boolean> upsert,
			UpdateComponent<SharedLibraryBean> update) {
		// TODO limited in what can change?
		throw new RuntimeException(ErrorUtils.get(ErrorUtils.NOT_YET_IMPLEMENTED, "SharedLibraryService.updateObjectBySpec"));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#updateObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.Optional, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent)
	 */
	@Override
	public ManagementFuture<Long> updateObjectsBySpec(
			QueryComponent<SharedLibraryBean> spec,
			Optional<Boolean> upsert,
			UpdateComponent<SharedLibraryBean> update) {
		// TODO limited in what can change?
		throw new RuntimeException(ErrorUtils.get(ErrorUtils.NOT_YET_IMPLEMENTED, "SharedLibraryService.updateObjectsBySpec"));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#updateAndReturnObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.Optional, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent, java.util.Optional, java.util.List, boolean)
	 */
	@Override
	public ManagementFuture<Optional<SharedLibraryBean>> updateAndReturnObjectBySpec(
			QueryComponent<SharedLibraryBean> unique_spec,
			Optional<Boolean> upsert,
			UpdateComponent<SharedLibraryBean> update,
			Optional<Boolean> before_updated, List<String> field_list,
			boolean include) {
		// TODO limited in what can change?
		throw new RuntimeException(ErrorUtils.get(ErrorUtils.NOT_YET_IMPLEMENTED, "SharedLibraryService.updateAndReturnObjectBySpec"));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#deleteObjectById(java.lang.Object)
	 */
	@Override
	public ManagementFuture<Boolean> deleteObjectById(
			Object id) {		
		final QueryComponent<SharedLibraryBean> query = CrudUtils.allOf(SharedLibraryBean.class).when(SharedLibraryBean::_id, id);
		return deleteObjectBySpec(query);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#deleteObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	@Override
	public ManagementFuture<Boolean> deleteObjectBySpec(
			QueryComponent<SharedLibraryBean> unique_spec) {		
		return FutureUtils.createManagementFuture(
			_underlying_library_db.get().getObjectBySpec(unique_spec).thenCompose(lib -> {
				if (lib.isPresent()) {
					try {
						final FileContext fs = _storage_service.get().getUnderlyingPlatformDriver(FileContext.class, Optional.empty()).get();
						fs.delete(fs.makeQualified(new Path(lib.get().path_name())), false);
					}
					catch (Exception e) { // i suppose we don't really care if it fails..
						// (maybe add a message?)
						//DEBUG
						//e.printStackTrace();
					}
					return _underlying_library_db.get().deleteObjectBySpec(unique_spec);
				}
				else {
					return CompletableFuture.completedFuture(false);
				}
			}));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#deleteObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	@Override
	public ManagementFuture<Long> deleteObjectsBySpec(
			QueryComponent<SharedLibraryBean> spec) {
		// TODO also delete the file
		throw new RuntimeException(ErrorUtils.get(ErrorUtils.NOT_YET_IMPLEMENTED, "SharedLibraryService.deleteObjectsBySpec"));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#deleteDatastore()
	 */
	@Override
	public ManagementFuture<Boolean> deleteDatastore() {
		throw new RuntimeException("This method is not supported");
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#getRawService()
	 */
	@Override
	public IManagementCrudService<JsonNode> getRawService() {
		throw new RuntimeException("DataBucketCrudService.getRawService not supported");
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService#getCrudService()
	 */
	@Override
	public Optional<ICrudService<SharedLibraryBean>> getCrudService() {
		return Optional.of(this);
	}

}
