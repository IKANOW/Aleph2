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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IBasicSearchService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.ProjectBean;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent;
import com.ikanow.aleph2.data_model.utils.FutureUtils;

/**
 * This is a wrapper around the storage service that handles uploading files
 * 
 * @author Burch
 *
 */
public class DataStoreCrudService implements ICrudService<FileDescriptor> {
	private final FileContext fileContext;
	private final String output_directory;
	private static final Logger _logger = LogManager.getLogger();
	/**
	 * Stores files at the given output dir, creates a storage_crud pointed at this location
	 */
	public DataStoreCrudService(final IServiceContext service_context, final String output_directory) {
		this.output_directory = output_directory;
		this.fileContext = service_context.getStorageService().getUnderlyingPlatformDriver(FileContext.class, Optional.empty()).get();
		_logger.error("Created DataStoreCrudService pointed at dir: " + output_directory);
	}
	
	public static class DataStoreCursor extends Cursor<FileDescriptor> {
		private static Collection<FileDescriptor> file_status;
		public DataStoreCursor(final Collection<FileDescriptor> file_status) {
			this.file_status = file_status;
		}
		
		@Override
		public Iterator<FileDescriptor> iterator() {
			return this.file_status.iterator();
		}

		@Override
		public void close() throws Exception {
			//do nothing, we already converted iter into list
		}

		@Override
		public long count() {
			return this.count();
		}
		
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService#storeObject(java.lang.Object)
	 */
	@Override
	public CompletableFuture<Supplier<Object>> storeObject(final FileDescriptor new_object) {		
		final String path = output_directory + new_object.file_name();
		_logger.error("attempting to store object: " + path);
		try {
			FileUtils.writeFile(fileContext, new_object.input_stream(), path);
		} catch (Exception e) {
			return FutureUtils.returnError(e);
		}
		//return file_name, that is what is used to query/delete by id
		return CompletableFuture.completedFuture(()->new_object.file_name());
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService#storeObjects(java.util.List)
	 */
	@Override
	public CompletableFuture<Tuple2<Supplier<List<Object>>, Supplier<Long>>> storeObjects(
			List<FileDescriptor> new_objects) {
		//TODO should I collect the futures so I can handle any failures?
		new_objects.stream().forEach(o->this.storeObject(o));
		
		//TODO what to return?
		return CompletableFuture.completedFuture(null);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService#countObjects()
	 */
	@Override
	public CompletableFuture<Long> countObjects() {
		//count the number of objects in the dir
		Long count = 0L;		
		try {
			final RemoteIterator<FileStatus> it = fileContext.listStatus(new Path(output_directory));
			while ( it.hasNext() ) {
				it.next();
				count++;
			}
		} catch (IllegalArgumentException | IOException e) {
			return FutureUtils.returnError(e);
		}
		
		return CompletableFuture.completedFuture(count);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService#deleteDatastore()
	 */
	@Override
	public CompletableFuture<Boolean> deleteDatastore() {
		//delete all files in the dir
//		try {
//			final RemoteIterator<FileStatus> it = fileContext.listStatus(new Path(output_directory));
//			while ( it.hasNext() ) {
//				final FileStatus fs = it.next();
//				fileContext.delete(fs.getPath(), true);
//			}
//		} catch (IllegalArgumentException | IOException e) {
//			return FutureUtils.returnError(e);
//		}
//		return CompletableFuture.completedFuture(true);
		return FutureUtils.returnError(new RuntimeException("deleteDatastore commented out because it is dangerous, will delete everything in output dir regardless of what put it there, use deleteObjectById with the filename as id instead"));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService#getCrudService()
	 */
	@Override
	public Optional<ICrudService<FileDescriptor>> getCrudService() {
		return Optional.of(this);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService#getUnderlyingPlatformDriver(java.lang.Class, java.util.Optional)
	 */
	@Override
	public <T> Optional<T> getUnderlyingPlatformDriver(Class<T> driver_class,
			Optional<String> driver_options) {
		return Optional.empty();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getFilteredRepo(java.lang.String, java.util.Optional, java.util.Optional)
	 */
	@Override
	public ICrudService<FileDescriptor> getFilteredRepo(String authorization_fieldname,
			Optional<AuthorizationBean> client_auth,
			Optional<ProjectBean> project_auth) {
		//TODO what is this?
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#storeObject(java.lang.Object, boolean)
	 */
	@Override
	public CompletableFuture<Supplier<Object>> storeObject(FileDescriptor new_object,
			boolean replace_if_present) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#storeObjects(java.util.List, boolean)
	 */
	@Override
	public CompletableFuture<Tuple2<Supplier<List<Object>>, Supplier<Long>>> storeObjects(
			List<FileDescriptor> new_objects, boolean replace_if_present) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#optimizeQuery(java.util.List)
	 */
	@Override
	public CompletableFuture<Boolean> optimizeQuery(
			List<String> ordered_field_list) {
		return FutureUtils.returnError(new RuntimeException("optimizeQuery not supported"));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deregisterOptimizedQuery(java.util.List)
	 */
	@Override
	public boolean deregisterOptimizedQuery(List<String> ordered_field_list) {
		return false;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	@Override
	public CompletableFuture<Optional<FileDescriptor>> getObjectBySpec(
			QueryComponent<FileDescriptor> unique_spec) {
		//TODO should this allow getting a fileback?
		return FutureUtils.returnError(new RuntimeException("getObjectBySpec not supported"));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.List, boolean)
	 */
	@Override
	public CompletableFuture<Optional<FileDescriptor>> getObjectBySpec(
			QueryComponent<FileDescriptor> unique_spec, List<String> field_list,
			boolean include) {
		return FutureUtils.returnError(new RuntimeException("getObjectBySpec not supported"));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectById(java.lang.Object)
	 */
	@Override
	public CompletableFuture<Optional<FileDescriptor>> getObjectById(Object id) {		
		return FutureUtils.returnError(new RuntimeException("function not supported"));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectById(java.lang.Object, java.util.List, boolean)
	 */
	@Override
	public CompletableFuture<Optional<FileDescriptor>> getObjectById(Object id,
			List<String> field_list, boolean include) {
		return FutureUtils.returnError(new RuntimeException("function not supported"));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	@Override
	public CompletableFuture<Cursor<FileDescriptor>> getObjectsBySpec(
			QueryComponent<FileDescriptor> spec) {
		try {
			return CompletableFuture.completedFuture(new DataStoreCursor(getFolderFilenames(output_directory, fileContext)));
		} catch (IllegalArgumentException | IOException e) {
			final CompletableFuture<Cursor<FileDescriptor>> fut = new CompletableFuture<Cursor<FileDescriptor>>();
			fut.completeExceptionally(e);
			return fut;
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.List, boolean)
	 */
	@Override
	public CompletableFuture<Cursor<FileDescriptor>> getObjectsBySpec(
			QueryComponent<FileDescriptor> spec, List<String> field_list, boolean include) {
		try {
			return CompletableFuture.completedFuture(new DataStoreCursor(getFolderFilenames(output_directory, fileContext)));
		} catch (IllegalArgumentException | IOException e) {
			final CompletableFuture<Cursor<FileDescriptor>> fut = new CompletableFuture<Cursor<FileDescriptor>>();
			fut.completeExceptionally(e);
			return fut;
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#countObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	@Override
	public CompletableFuture<Long> countObjectsBySpec(QueryComponent<FileDescriptor> spec) {
		try {
			return CompletableFuture.completedFuture(new DataStoreCursor(getFolderFilenames(output_directory, fileContext)).count());			
		} catch (IllegalArgumentException | IOException e) {
			final CompletableFuture<Long> fut = new CompletableFuture<Long>();
			fut.completeExceptionally(e);
			return fut;
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#updateObjectById(java.lang.Object, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent)
	 */
	@Override
	public CompletableFuture<Boolean> updateObjectById(Object id,
			UpdateComponent<FileDescriptor> update) {
		return FutureUtils.returnError(new RuntimeException("function not supported"));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#updateObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.Optional, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent)
	 */
	@Override
	public CompletableFuture<Boolean> updateObjectBySpec(
			QueryComponent<FileDescriptor> unique_spec, Optional<Boolean> upsert,
			UpdateComponent<FileDescriptor> update) {
		return FutureUtils.returnError(new RuntimeException("function not supported"));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#updateObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.Optional, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent)
	 */
	@Override
	public CompletableFuture<Long> updateObjectsBySpec(QueryComponent<FileDescriptor> spec,
			Optional<Boolean> upsert, UpdateComponent<FileDescriptor> update) {
		return FutureUtils.returnError(new RuntimeException("function not supported"));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#updateAndReturnObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.Optional, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent, java.util.Optional, java.util.List, boolean)
	 */
	@Override
	public CompletableFuture<Optional<FileDescriptor>> updateAndReturnObjectBySpec(
			QueryComponent<FileDescriptor> unique_spec, Optional<Boolean> upsert,
			UpdateComponent<FileDescriptor> update, Optional<Boolean> before_updated,
			List<String> field_list, boolean include) {
		return FutureUtils.returnError(new RuntimeException("function not supported"));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deleteObjectById(java.lang.Object)
	 */
	@Override
	public CompletableFuture<Boolean> deleteObjectById(Object id) {		
		try {			
			final Path path = new Path(output_directory + id.toString());
			_logger.error("Trying to delete: " + path.toString());
			final boolean delete_success = fileContext.delete(path, false); //this is always returning false
			_logger.error("success deleteing: " + delete_success);			
			return CompletableFuture.completedFuture(!doesPathExist(path, fileContext)); //if file does not exist, delete was a success
		} catch (IllegalArgumentException | IOException e) {
			final CompletableFuture<Boolean> fut = new CompletableFuture<Boolean>();
			fut.completeExceptionally(e);
			return fut;
		}
	}
	
	/**
	 * Utility for checking if a file exists or not, attempts to get
	 * the paths status and returns true if that is successful and
	 * its either a file or dir.
	 * 
	 * If a filenotfound exception is thrown, returns false
	 * 
	 * Lets all other exceptions raise
	 * 
	 * This is used to get around FileContext.delete not reporting correctly if a file has been successfully deleted.
	 * @param path
	 * @param context
	 * @return
	 * @throws AccessControlException
	 * @throws UnsupportedFileSystemException
	 * @throws IOException
	 */
	private static boolean doesPathExist(final Path path, final FileContext context) throws AccessControlException, UnsupportedFileSystemException, IOException {
		try {
			FileStatus status = context.getFileStatus(path);
			return status.isFile() || status.isDirectory();
		} catch (FileNotFoundException e) {
			return false; 
		}		
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deleteObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	@Override
	public CompletableFuture<Boolean> deleteObjectBySpec(
			QueryComponent<FileDescriptor> unique_spec) {
		return FutureUtils.returnError(new RuntimeException("function not supported"));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deleteObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	@Override
	public CompletableFuture<Long> deleteObjectsBySpec(QueryComponent<FileDescriptor> spec) {
		return FutureUtils.returnError(new RuntimeException("function not supported"));
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
	public Optional<IBasicSearchService<FileDescriptor>> getSearchService() {
		// TODO Auto-generated method stub
		return null;
	}
	
	private static Collection<FileDescriptor> getFolderFilenames(final String folder, final FileContext file_context) throws AccessControlException, FileNotFoundException, UnsupportedFileSystemException, IllegalArgumentException, IOException {
		Collection<FileDescriptor> file_names = new ArrayList<FileDescriptor>();
		RemoteIterator<FileStatus> file_status = file_context.listStatus(new Path(folder));
		while ( file_status.hasNext() ) 
			file_names.add(new FileDescriptor(null, file_status.next().getPath().getName()));
		return file_names;
	}
}
