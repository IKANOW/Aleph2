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
import org.checkerframework.checker.nullness.qual.NonNull;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IBasicSearchService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.ProjectBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.FutureUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent;
import com.ikanow.aleph2.data_model.utils.FutureUtils.ManagementFuture;

public class SharedLibraryCrudService implements IManagementCrudService<SharedLibraryBean> {
	@SuppressWarnings("unused")
	private static final Logger _logger = LogManager.getLogger();	

	protected final IStorageService _storage_service;	
	protected final IManagementDbService _underlying_management_db;
	protected final ICrudService<SharedLibraryBean> _underlying_library_db;
	
	/** Guice invoked constructor
	 */
	@Inject
	public SharedLibraryCrudService(final IServiceContext service_context) {
		_underlying_management_db = service_context.getService(IManagementDbService.class, Optional.empty());
		_storage_service = service_context.getStorageService();
		_underlying_library_db = _underlying_management_db.getSharedLibraryStore();
		
		// Handle some simple optimization of the data bucket CRUD repo:
		Executors.newSingleThreadExecutor().submit(() -> {
			_underlying_library_db.optimizeQuery(Arrays.asList(
					BeanTemplateUtils.from(SharedLibraryBean.class).field(SharedLibraryBean::path_name)));
		});				
	}
	
	/** User constructor, for wrapping
	 */
	public SharedLibraryCrudService(final @NonNull IManagementDbService underlying_management_db, 
			final @NonNull IStorageService storage_service,
			final @NonNull ICrudService<SharedLibraryBean> underlying_library_db
			)
	{
		_underlying_management_db = underlying_management_db;
		_storage_service = storage_service;
		_underlying_library_db = underlying_library_db;
	}
	
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deregisterOptimizedQuery(java.util.List)
	 */
	@Override
	public @NonNull boolean deregisterOptimizedQuery(
			@NonNull List<String> ordered_field_list) {
		return _underlying_library_db.deregisterOptimizedQuery(ordered_field_list);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getSearchService()
	 */
	@Override
	public @NonNull Optional<IBasicSearchService<SharedLibraryBean>> getSearchService() {
		return _underlying_library_db.getSearchService();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> @NonNull T getUnderlyingPlatformDriver(
			@NonNull Class<T> driver_class, Optional<String> driver_options) {
		if (driver_class == ICrudService.class) {
			return (@NonNull T) _underlying_library_db;
		}
		else {
			throw new RuntimeException("SharedLibraryCrudService.getUnderlyingPlatformDriver not supported");
		}
	}

	@Override
	public @NonNull IManagementCrudService<SharedLibraryBean> getFilteredRepo(
			@NonNull String authorization_fieldname,
			Optional<AuthorizationBean> client_auth,
			Optional<ProjectBean> project_auth) {
		return new SharedLibraryCrudService(_underlying_management_db.getFilteredDb(client_auth, project_auth), 
				_storage_service,
				_underlying_library_db.getFilteredRepo(authorization_fieldname, client_auth, project_auth));
	}

	@Override
	public @NonNull ManagementFuture<Supplier<Object>> storeObject(
			@NonNull SharedLibraryBean new_object, boolean replace_if_present) {
		//TODO (ALEPH-19): convert this into an update, ie get old version, compare and overwrite
		return FutureUtils.createManagementFuture(_underlying_library_db.storeObject(new_object, replace_if_present));
	}

	@Override
	public @NonNull ManagementFuture<Supplier<Object>> storeObject(
			@NonNull SharedLibraryBean new_object) {
		return this.storeObject(new_object, false);
	}

	@Override
	public @NonNull ManagementFuture<Tuple2<Supplier<List<Object>>, Supplier<Long>>> storeObjects(
			@NonNull List<SharedLibraryBean> new_objects,
			boolean continue_on_error) {
		if (continue_on_error) {
			throw new RuntimeException("Can't call storeObjects with continue_on_error: true, use update instead");
		}
		return FutureUtils.createManagementFuture(_underlying_library_db.storeObjects(new_objects, continue_on_error));
	}

	@Override
	public @NonNull ManagementFuture<Tuple2<Supplier<List<Object>>, Supplier<Long>>> storeObjects(
			@NonNull List<SharedLibraryBean> new_objects) {
		return this.storeObjects(new_objects, false);
	}

	@Override
	public @NonNull ManagementFuture<Boolean> optimizeQuery(
			@NonNull List<String> ordered_field_list) {
		return FutureUtils.createManagementFuture(_underlying_library_db.optimizeQuery(ordered_field_list));
	}

	@Override
	public @NonNull ManagementFuture<Optional<SharedLibraryBean>> getObjectBySpec(
			@NonNull QueryComponent<SharedLibraryBean> unique_spec) {
		return FutureUtils.createManagementFuture(_underlying_library_db.getObjectBySpec(unique_spec));
	}

	@Override
	public @NonNull ManagementFuture<Optional<SharedLibraryBean>> getObjectBySpec(
			@NonNull QueryComponent<SharedLibraryBean> unique_spec,
			@NonNull List<String> field_list, boolean include) {
		return FutureUtils.createManagementFuture(_underlying_library_db.getObjectBySpec(unique_spec, field_list, include));
	}

	@Override
	public @NonNull ManagementFuture<Optional<SharedLibraryBean>> getObjectById(
			@NonNull Object id) {
		return FutureUtils.createManagementFuture(_underlying_library_db.getObjectById(id));
	}

	@Override
	public @NonNull ManagementFuture<Optional<SharedLibraryBean>> getObjectById(
			@NonNull Object id, @NonNull List<String> field_list,
			boolean include) {
		return FutureUtils.createManagementFuture(_underlying_library_db.getObjectById(id, field_list, include));
	}

	@Override
	public @NonNull ManagementFuture<Cursor<SharedLibraryBean>> getObjectsBySpec(
			@NonNull QueryComponent<SharedLibraryBean> spec) {
		return FutureUtils.createManagementFuture(_underlying_library_db.getObjectsBySpec(spec));
	}

	@Override
	public @NonNull ManagementFuture<Cursor<SharedLibraryBean>> getObjectsBySpec(
			@NonNull QueryComponent<SharedLibraryBean> spec,
			@NonNull List<String> field_list, boolean include) {
		return FutureUtils.createManagementFuture(_underlying_library_db.getObjectsBySpec(spec, field_list, include));
	}

	@Override
	public @NonNull ManagementFuture<Long> countObjectsBySpec(
			@NonNull QueryComponent<SharedLibraryBean> spec) {
		return FutureUtils.createManagementFuture(_underlying_library_db.countObjectsBySpec(spec));
	}

	@Override
	public @NonNull ManagementFuture<Long> countObjects() {
		return FutureUtils.createManagementFuture(_underlying_library_db.countObjects());
	}

	@Override
	public @NonNull ManagementFuture<Boolean> updateObjectById(
			@NonNull Object id,
			@NonNull UpdateComponent<SharedLibraryBean> update) {
		// TODO limited in what can change?
		return null;
	}

	@Override
	public @NonNull ManagementFuture<Boolean> updateObjectBySpec(
			@NonNull QueryComponent<SharedLibraryBean> unique_spec,
			Optional<Boolean> upsert,
			@NonNull UpdateComponent<SharedLibraryBean> update) {
		// TODO limited in what can change?
		return null;
	}

	@Override
	public @NonNull ManagementFuture<Long> updateObjectsBySpec(
			@NonNull QueryComponent<SharedLibraryBean> spec,
			Optional<Boolean> upsert,
			@NonNull UpdateComponent<SharedLibraryBean> update) {
		// TODO limited in what can change?
		return null;
	}

	@Override
	public @NonNull ManagementFuture<Optional<SharedLibraryBean>> updateAndReturnObjectBySpec(
			@NonNull QueryComponent<SharedLibraryBean> unique_spec,
			Optional<Boolean> upsert,
			@NonNull UpdateComponent<SharedLibraryBean> update,
			Optional<Boolean> before_updated, @NonNull List<String> field_list,
			boolean include) {
		// TODO limited in what can change?
		return null;
	}

	@Override
	public @NonNull ManagementFuture<Boolean> deleteObjectById(
			@NonNull Object id) {		
		final QueryComponent<SharedLibraryBean> query = CrudUtils.allOf(SharedLibraryBean.class).when(SharedLibraryBean::_id, id);
		return deleteObjectBySpec(query);
	}

	@Override
	public @NonNull ManagementFuture<Boolean> deleteObjectBySpec(
			@NonNull QueryComponent<SharedLibraryBean> unique_spec) {		
		return FutureUtils.createManagementFuture(
			_underlying_library_db.getObjectBySpec(unique_spec).thenCompose(lib -> {
				if (lib.isPresent()) {
					try {
						final FileContext fs = _storage_service.getUnderlyingPlatformDriver(FileContext.class, Optional.empty());
						fs.delete(fs.makeQualified(new Path(lib.get().path_name())), false);
					}
					catch (Exception e) { // i suppose we don't really care if it fails..
						// (maybe add a message?)
						/**/
						//DEBUG
						e.printStackTrace();
					}
					return _underlying_library_db.deleteObjectBySpec(unique_spec);
				}
				else {
					return CompletableFuture.completedFuture(false);
				}
			}));
	}

	@Override
	public @NonNull ManagementFuture<Long> deleteObjectsBySpec(
			@NonNull QueryComponent<SharedLibraryBean> spec) {
		// TODO also delete the file
		return null;
	}

	@Override
	public @NonNull ManagementFuture<Boolean> deleteDatastore() {
		throw new RuntimeException("This method is not supported");
	}

	@Override
	public @NonNull IManagementCrudService<JsonNode> getRawCrudService() {
		throw new RuntimeException("DataBucketCrudService.getRawCrudService not supported");
	}

}
