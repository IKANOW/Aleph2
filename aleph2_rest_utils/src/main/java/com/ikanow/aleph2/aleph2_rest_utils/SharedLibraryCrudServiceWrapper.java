/*******************************************************************************
 * Copyright 2016, The IKANOW Open Source Project.
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
package com.ikanow.aleph2.aleph2_rest_utils;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IBasicSearchService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.ProjectBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent;

/**
 * @author Burch
 *
 */
public class SharedLibraryCrudServiceWrapper implements ICrudService<Tuple2<SharedLibraryBean, FileDescriptor>> {
	private static final Logger _logger = LogManager.getLogger();
	final ICrudService<SharedLibraryBean> shared_library_crud;
	final IServiceContext service_context;
	public SharedLibraryCrudServiceWrapper(final ICrudService<SharedLibraryBean> shared_library_crud, final IServiceContext service_context) {
		this.shared_library_crud = shared_library_crud;
		this.service_context = service_context;
	}
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService#storeObject(java.lang.Object)
	 */
	@Override
	public CompletableFuture<Supplier<Object>> storeObject(
			Tuple2<SharedLibraryBean, FileDescriptor> new_object) {
		//if storing shared_lib object is successful, try to store the file
		return this.shared_library_crud.storeObject(new_object._1).thenCompose(f->{			
			_logger.error("stored object: " + f.get()); //I think I should be returning this
			final Tuple2<ICrudService<FileDescriptor>, FileDescriptor> storage = getSharedLibraryDataStore(service_context, new_object._1.path_name(), new_object._2);
			storage._1.storeObject(storage._2);
			return CompletableFuture.completedFuture(f);
		});
	}
	
	private static Tuple2<ICrudService<FileDescriptor>, FileDescriptor> getSharedLibraryDataStore(final IServiceContext service_context, final String output_path, final FileDescriptor file_descriptor) {
		_logger.error("filepath is: " + output_path);
		final String path = output_path.substring(0, output_path.lastIndexOf("/")+1); //TODO what if they use \ instead of /
		final String file_name = output_path.substring(output_path.lastIndexOf("/")+1);
		
		return new Tuple2<ICrudService<FileDescriptor>, FileDescriptor>( new DataStoreCrudService(service_context, path), new FileDescriptor(file_descriptor.input_stream(), file_name));
	}
			
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService#storeObjects(java.util.List)
	 */
	@Override
	public CompletableFuture<Tuple2<Supplier<List<Object>>, Supplier<Long>>> storeObjects(
			List<Tuple2<SharedLibraryBean, FileDescriptor>> new_objects) {
		// TODO Auto-generated method stub
		return null;
	}
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService#countObjects()
	 */
	@Override
	public CompletableFuture<Long> countObjects() {
		// TODO Auto-generated method stub
		return null;
	}
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService#deleteDatastore()
	 */
	@Override
	public CompletableFuture<Boolean> deleteDatastore() {
		// TODO Auto-generated method stub
		return null;
	}
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService#getCrudService()
	 */
	@Override
	public Optional<ICrudService<Tuple2<SharedLibraryBean, FileDescriptor>>> getCrudService() {
		// TODO Auto-generated method stub
		return null;
	}
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService#getUnderlyingPlatformDriver(java.lang.Class, java.util.Optional)
	 */
	@Override
	public <T> Optional<T> getUnderlyingPlatformDriver(Class<T> driver_class,
			Optional<String> driver_options) {
		// TODO Auto-generated method stub
		return null;
	}
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getFilteredRepo(java.lang.String, java.util.Optional, java.util.Optional)
	 */
	@Override
	public ICrudService<Tuple2<SharedLibraryBean, FileDescriptor>> getFilteredRepo(
			String authorization_fieldname,
			Optional<AuthorizationBean> client_auth,
			Optional<ProjectBean> project_auth) {
		// TODO Auto-generated method stub
		return null;
	}
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#storeObject(java.lang.Object, boolean)
	 */
	@Override
	public CompletableFuture<Supplier<Object>> storeObject(
			Tuple2<SharedLibraryBean, FileDescriptor> new_object,
			boolean replace_if_present) {
		// TODO Auto-generated method stub
		return null;
	}
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#storeObjects(java.util.List, boolean)
	 */
	@Override
	public CompletableFuture<Tuple2<Supplier<List<Object>>, Supplier<Long>>> storeObjects(
			List<Tuple2<SharedLibraryBean, FileDescriptor>> new_objects,
			boolean replace_if_present) {
		// TODO Auto-generated method stub
		return null;
	}
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#optimizeQuery(java.util.List)
	 */
	@Override
	public CompletableFuture<Boolean> optimizeQuery(
			List<String> ordered_field_list) {
		// TODO Auto-generated method stub
		return null;
	}
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deregisterOptimizedQuery(java.util.List)
	 */
	@Override
	public boolean deregisterOptimizedQuery(List<String> ordered_field_list) {
		// TODO Auto-generated method stub
		return false;
	}
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	@Override
	public CompletableFuture<Optional<Tuple2<SharedLibraryBean, FileDescriptor>>> getObjectBySpec(
			QueryComponent<Tuple2<SharedLibraryBean, FileDescriptor>> unique_spec) {
		// TODO Auto-generated method stub
		return null;
	}
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.List, boolean)
	 */
	@Override
	public CompletableFuture<Optional<Tuple2<SharedLibraryBean, FileDescriptor>>> getObjectBySpec(
			QueryComponent<Tuple2<SharedLibraryBean, FileDescriptor>> unique_spec,
			List<String> field_list, boolean include) {
		// TODO Auto-generated method stub
		return null;
	}
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectById(java.lang.Object)
	 */
	@Override
	public CompletableFuture<Optional<Tuple2<SharedLibraryBean, FileDescriptor>>> getObjectById(
			Object id) {
		// TODO Auto-generated method stub
		return null;
	}
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectById(java.lang.Object, java.util.List, boolean)
	 */
	@Override
	public CompletableFuture<Optional<Tuple2<SharedLibraryBean, FileDescriptor>>> getObjectById(
			Object id, List<String> field_list, boolean include) {
		// TODO Auto-generated method stub
		return null;
	}
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	@Override
	public CompletableFuture<com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.Cursor<Tuple2<SharedLibraryBean, FileDescriptor>>> getObjectsBySpec(
			QueryComponent<Tuple2<SharedLibraryBean, FileDescriptor>> spec) {
		// TODO Auto-generated method stub
		return null;
	}
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.List, boolean)
	 */
	@Override
	public CompletableFuture<com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.Cursor<Tuple2<SharedLibraryBean, FileDescriptor>>> getObjectsBySpec(
			QueryComponent<Tuple2<SharedLibraryBean, FileDescriptor>> spec,
			List<String> field_list, boolean include) {
		// TODO Auto-generated method stub
		return null;
	}
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#countObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	@Override
	public CompletableFuture<Long> countObjectsBySpec(
			QueryComponent<Tuple2<SharedLibraryBean, FileDescriptor>> spec) {
		// TODO Auto-generated method stub
		return null;
	}
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#updateObjectById(java.lang.Object, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent)
	 */
	@Override
	public CompletableFuture<Boolean> updateObjectById(Object id,
			UpdateComponent<Tuple2<SharedLibraryBean, FileDescriptor>> update) {
		// TODO Auto-generated method stub
		return null;
	}
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#updateObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.Optional, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent)
	 */
	@Override
	public CompletableFuture<Boolean> updateObjectBySpec(
			QueryComponent<Tuple2<SharedLibraryBean, FileDescriptor>> unique_spec,
			Optional<Boolean> upsert,
			UpdateComponent<Tuple2<SharedLibraryBean, FileDescriptor>> update) {
		// TODO Auto-generated method stub
		return null;
	}
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#updateObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.Optional, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent)
	 */
	@Override
	public CompletableFuture<Long> updateObjectsBySpec(
			QueryComponent<Tuple2<SharedLibraryBean, FileDescriptor>> spec,
			Optional<Boolean> upsert,
			UpdateComponent<Tuple2<SharedLibraryBean, FileDescriptor>> update) {
		// TODO Auto-generated method stub
		return null;
	}
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#updateAndReturnObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.Optional, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent, java.util.Optional, java.util.List, boolean)
	 */
	@Override
	public CompletableFuture<Optional<Tuple2<SharedLibraryBean, FileDescriptor>>> updateAndReturnObjectBySpec(
			QueryComponent<Tuple2<SharedLibraryBean, FileDescriptor>> unique_spec,
			Optional<Boolean> upsert,
			UpdateComponent<Tuple2<SharedLibraryBean, FileDescriptor>> update,
			Optional<Boolean> before_updated, List<String> field_list,
			boolean include) {
		// TODO Auto-generated method stub
		return null;
	}
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deleteObjectById(java.lang.Object)
	 */
	@Override
	public CompletableFuture<Boolean> deleteObjectById(Object id) {
		//delete from shared lib, then delete from filesys
		//have to get shared lib entry, so we know where the file is on filesys
		final Optional<SharedLibraryBean> shared_library_bean = shared_library_crud.getObjectById(id).join();
		return shared_library_bean.map(slb->{
			return shared_library_crud.deleteObjectById(id).<Boolean>thenCompose(f->{
				if ( f ) {
					final Tuple2<ICrudService<FileDescriptor>, FileDescriptor> storage = getSharedLibraryDataStore(service_context, slb.path_name(), new FileDescriptor(null, null));
					return storage._1.deleteObjectById(storage._2.file_name());
				} else {
					return CompletableFuture.completedFuture(f); //false, shared object delete must have failed
				}
			});
		}).orElseGet(()->CompletableFuture.completedFuture(false));
	}
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deleteObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	@Override
	public CompletableFuture<Boolean> deleteObjectBySpec(
			QueryComponent<Tuple2<SharedLibraryBean, FileDescriptor>> unique_spec) {
		// TODO Auto-generated method stub
		return null;
	}
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deleteObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	@Override
	public CompletableFuture<Long> deleteObjectsBySpec(
			QueryComponent<Tuple2<SharedLibraryBean, FileDescriptor>> spec) {
		// TODO Auto-generated method stub
		return null;
	}
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getRawService()
	 */
	@Override
	public ICrudService<JsonNode> getRawService() {
		// TODO Auto-generated method stub
		return null;
	}
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getSearchService()
	 */
	@Override
	public Optional<IBasicSearchService<Tuple2<SharedLibraryBean, FileDescriptor>>> getSearchService() {
		// TODO Auto-generated method stub
		return null;
	}
}
