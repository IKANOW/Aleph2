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
package com.ikanow.aleph2.core.shared.utils;

import java.io.FileNotFoundException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;

import fj.data.Validation;

/** Utilities for retrieving shared JARs to a local spot from where they can easily be used by a classloader
 * @author acp
 */
public class JarCacheUtils {

	/** Moves a shared JAR into a local spot (if required)
	 * @param library_bean
	 * @param fs
	 * @return either a basic message bean containing an error, or the fully qualified path of the cached JAR
	 */
	public static <M> CompletableFuture<Validation<BasicMessageBean, String>> getCachedJar(
			final String local_cached_jar_dir,
			final SharedLibraryBean library_bean, final IStorageService fs,
			final String handler_for_errors, final M msg_for_errors)
	{		
		try {
			final FileContext dfs = fs.getUnderlyingPlatformDriver(FileContext.class, Optional.empty()).get();
			final FileContext lfs = FileContext.getLocalFSFileContext(new Configuration());
			
			final Path cached_jar_file = lfs.makeQualified(new Path(local_cached_jar_dir + "/" + buildCachedJarName(library_bean))); 
			final Path original_jar_file = dfs.makeQualified(new Path(library_bean.path_name()));
			
			final FileStatus file_status = dfs.getFileStatus(original_jar_file); // (this will exception out if it doesn't exist, as it should)
			
			try {
				final FileStatus local_file_status = lfs.getFileStatus(cached_jar_file); // (this will exception in to case 2 if it doesn't exist)
				
				// if the local version exists then overwrite it
				
				if (file_status.getModificationTime() > local_file_status.getModificationTime()) {
					// (it gets kinda complicated here so just invalidate the entire classloader cache..)
					// TODO (ALEPH-12): add a coverage test for this
					ClassloaderUtils.clearCache();
					
					lfs.util().copy(original_jar_file, cached_jar_file, false, true);
				}
			}
			catch (FileNotFoundException f) {
				
				// 2) if the local version doesn't exist then just copy the distributed file across
				// (note: don't need to do anything with the classloader cache here since the file doesn't exist so can't have a cache key)
				
				lfs.util().copy(original_jar_file, cached_jar_file);
			}
			return CompletableFuture.completedFuture(Validation.success(cached_jar_file.toString()));
			
		} catch (Throwable e) {
			return CompletableFuture.completedFuture(Validation.fail
					(SharedErrorUtils.buildErrorMessage(handler_for_errors, 
							msg_for_errors, 
							SharedErrorUtils.getLongForm(SharedErrorUtils.SHARED_LIBRARY_NAME_NOT_FOUND, e, library_bean.path_name()) 
							)));
		}
	}
	
	/** Just creates a cached name as <lib bean id>.cache.jar
	 * @param library_bean the library bean to cache
	 * @return the cache name
	 */
	private static String buildCachedJarName(SharedLibraryBean library_bean) {
		return library_bean._id() + ".cache.jar";
	}
}
