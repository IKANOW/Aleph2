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
package com.ikanow.aleph2.data_import_manager.utils;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import scala.Tuple2;

import com.ikanow.aleph2.core.shared.utils.JarCacheUtils;
import com.ikanow.aleph2.core.shared.utils.SharedErrorUtils;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;

import fj.data.Validation;

/** Common utilities between the harvest and analytics managers
 * @author Alex
 */
public class LibraryCacheUtils {

	/** Given a bucket ...returns either - a future containing the first error encountered, _or_ a map (both name and id as keys) of path names 
	 * (and guarantee that the file has been cached when the future completes)
	 * @param bucket
	 * @param cache_tech_jar_only
	 * @param management_db
	 * @param globals
	 * @param fs
	 * @param handler_for_errors
	 * @param msg_for_errors
	 * @return  a future containing the first error encountered, _or_ a map (both name and id as keys) of path names 
	 */
	@SuppressWarnings("unchecked")
	public static <M> CompletableFuture<Validation<BasicMessageBean, Map<String, Tuple2<SharedLibraryBean, String>>>> 
		cacheJars(
				final DataBucketBean bucket, 
				final QueryComponent<SharedLibraryBean> spec,
				final IManagementDbService management_db, 
				final GlobalPropertiesBean globals,
				final IStorageService fs, 
				final IServiceContext context,
				final String handler_for_errors, 
				final M msg_for_errors
			)
	{
		try {
			return management_db.getSharedLibraryStore().secured(context, new AuthorizationBean(bucket.owner_id()))
					.getObjectsBySpec(spec)
					.thenComposeAsync(cursor -> {
						// This is a map of futures from the cache call - either an error or the path name
						// note we use a tuple of (id, name) as the key and then flatten out later 
						final Map<Tuple2<String, String>, Tuple2<SharedLibraryBean, CompletableFuture<Validation<BasicMessageBean, String>>>> map_of_futures = 
							StreamSupport.stream(cursor.spliterator(), true)
								.filter(lib -> {
									return true;
								})
								.collect(Collectors.<SharedLibraryBean, Tuple2<String, String>, Tuple2<SharedLibraryBean, CompletableFuture<Validation<BasicMessageBean, String>>>>
									toMap(
										// want to keep both the name and id versions - will flatten out below
										lib -> Tuples._2T(lib.path_name(), lib._id()), //(key)
										// spin off a future in which the file is being copied - save the shared library bean also
										lib -> Tuples._2T(lib, // (value) 
												JarCacheUtils.getCachedJar(globals.local_cached_jar_dir(), lib, fs, handler_for_errors, msg_for_errors))));
						
						// denest from map of futures to future of maps, also handle any errors here:
						// (some sort of "lift" function would be useful here - this are a somewhat inelegant few steps)
						
						final CompletableFuture<Validation<BasicMessageBean, String>>[] futures = 
								(CompletableFuture<Validation<BasicMessageBean, String>>[]) map_of_futures
								.values()
								.stream().map(t2 -> t2._2()).collect(Collectors.toList())
								.toArray(new CompletableFuture[0]);
						
						// (have to embed this thenApply instead of bringing it outside as part of the toCompose chain, because otherwise we'd lose map_of_futures scope)
						return CompletableFuture.allOf(futures).<Validation<BasicMessageBean, Map<String, Tuple2<SharedLibraryBean, String>>>>thenApply(f -> {								
							try {
								final Map<String, Tuple2<SharedLibraryBean, String>> almost_there = map_of_futures.entrySet().stream()
									.flatMap(kv -> {
										final Validation<BasicMessageBean, String> ret = kv.getValue()._2().join(); // (must have already returned if here
										return ret.<Stream<Tuple2<String, Tuple2<SharedLibraryBean, String>>>>
											validation(
												//Error:
												err -> { throw new RuntimeException(err.message()); } // (not ideal, but will do)
												,
												// Normal:
												s -> { 
													return Arrays.asList(
														Tuples._2T(kv.getKey()._1(), Tuples._2T(kv.getValue()._1(), s)), // result object with path_name
														Tuples._2T(kv.getKey()._2(), Tuples._2T(kv.getValue()._1(), s))) // result object with id
															.stream();
												});
									})
									.collect(Collectors.<Tuple2<String, Tuple2<SharedLibraryBean, String>>, String, Tuple2<SharedLibraryBean, String>>
										toMap(
											idname_path -> idname_path._1(), //(key)
											idname_path -> idname_path._2() // (value)
											))
									;								
								return Validation.<BasicMessageBean, Map<String, Tuple2<SharedLibraryBean, String>>>success(almost_there);
							}
							catch (Exception e) { // handle the exception thrown above containing the message bean from whatever the original error was!
								return Validation.<BasicMessageBean, Map<String, Tuple2<SharedLibraryBean, String>>>fail(
										SharedErrorUtils.buildErrorMessage(handler_for_errors.toString(), msg_for_errors,
												e.getMessage()));
							}
						});
					});
		}
		catch (Throwable e) { // (can only occur if the DB call errors)
			return CompletableFuture.completedFuture(
				Validation.fail(SharedErrorUtils.buildErrorMessage(handler_for_errors.toString(), msg_for_errors,
					ErrorUtils.getLongForm(SharedErrorUtils.ERROR_CACHING_SHARED_LIBS, e, bucket.full_name())
					)));
		}
	}
	
}
