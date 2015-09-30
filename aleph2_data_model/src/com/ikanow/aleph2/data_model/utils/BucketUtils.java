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

import java.util.Collections;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.data_import.HarvestControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;

/**
 * Provides a set of util functions for DataBucketBean
 * 
 * @author Burch
 *
 */
public class BucketUtils {
	
	public static final String TEST_BUCKET_PREFIX = "/aleph2_testing/";
	
	/**
	 * Returns a clone of the bean and modifies the full_name field to provide a
	 * test path instead (by prepending "/alphe2_testing/{user_id}" to the path).
	 * 
	 * @param original_bean
	 * @param user_id
	 * @return original_bean with the full_name field modified with a test path
	 */
	public static DataBucketBean convertDataBucketBeanToTest(final DataBucketBean original_bean, String user_id) {
		final String new_full_name = TEST_BUCKET_PREFIX + user_id + original_bean.full_name(); // (full name is is guaranteed to start /)
		return BeanTemplateUtils.clone(original_bean)
				.with(DataBucketBean::full_name, new_full_name)
				.done();
	}
	
	/** Check if a bucket is a test bucket (trivial)
	 * @param bucket - the bucket to check
	 * @return
	 */
	public static boolean isTestBucket(final DataBucketBean bucket) {
		return bucket.full_name().startsWith(TEST_BUCKET_PREFIX);
	}
	
	///////////////////////////////////////////////////////////////////////////
	
	//TODO: create sub-bucket
	
	//////////////////////////////////////////////////////////////////////////

	// CREATE BUCKET SIGNATURE
	
	private static final int MAX_COLL_COMP_LEN = 16;
	
	/** Creates a reproducible, unique, but human readable signature for a bucket that is safe to use as a kafka topic/mongodb collection/elasticsearch index/etc
	 * Generated as follows:
	 * 1) it's in the form of a set of "_" separated components, followed by "__", followed by a deterministic UUID (see 4) 
	 * Where:
	 * 2a) The first and last 2 components of the path are taken (eg a, y, z if the path were a/b/c/..../x/y/z)
	 *     (if the path is comprised of fewer than 3 components then only the 1/2 components are used) 
	 * 2b) truncated to 16B
	 * 2c) any character that isn't non-alphanumeric or _ is transformed to _
	 * 2d) multiple "_"s are truncated to a single "_"
	 * 3) If a "subcollection" is specified then it is treated as a 4th component, eg a_y_z_S for subcollection S above path, transformed identically to 2a-2d
	 * 4) The version 3 UUID of the path (_not_ including the subcollection - see NOTES below) is used (https://en.wikipedia.org/wiki/Universally_unique_identifier#Version_3_.28MD5_hash_.26_namespace.29)
	 * NOTES 
	 *  - subcollections must be alphanumeric and less than 16 chars to ensure uniqueness of multiple subcollections within a bucket)
	 *  - all subcollections of a bucket can be tied to the bucket using the trailing UUID
	 * @param path
	 * @return
	 */
	public static String getUniqueSignature(final String path, final Optional<String> subcollection) {
		
		final String[] components = Optional.of(path)
								.map(p -> p.startsWith("/") ? p.substring(1) : p)
								.get()
								.split("[/]");
		
		if (1 == components.length) {
			return tidyUpIndexName(safeTruncate(components[0], MAX_COLL_COMP_LEN)
										+ addOptionalSubCollection(subcollection, MAX_COLL_COMP_LEN))
										+ "__" + generateUuidSuffix(path);
		}
		else if (2 == components.length) {
			return tidyUpIndexName(safeTruncate(components[0], MAX_COLL_COMP_LEN) 
									+ "_" + safeTruncate(components[1], MAX_COLL_COMP_LEN)
									+ addOptionalSubCollection(subcollection, MAX_COLL_COMP_LEN))
									+ "__" + generateUuidSuffix(path);
		}
		else { // take the first and the last 2
			final int n = components.length;
			return tidyUpIndexName(safeTruncate(components[0], MAX_COLL_COMP_LEN) 
									+ "_" + safeTruncate(components[n-2], MAX_COLL_COMP_LEN) 
									+ "_" + safeTruncate(components[n-1], MAX_COLL_COMP_LEN) 
									+ addOptionalSubCollection(subcollection, MAX_COLL_COMP_LEN))
									+ "__" + generateUuidSuffix(path);
		}
	}
	// Utils for getBaseIndexName
	private static String addOptionalSubCollection(final Optional<String> subcollection, final int max_len) {
		return subcollection.map(sc -> "_" + safeTruncate(sc, max_len)).orElse("");
	}
	private static String tidyUpIndexName(final String in) {
		return Optional.of(in.toLowerCase().replaceAll("[^a-z0-9_]", "_").replaceAll("__+", "_"))
				.map(s -> s.endsWith("_") ? s.substring(0, s.length() - 1) : s)
				.get()
				;
	}
	private static String generateUuidSuffix(final String in) {
		return UuidUtils.get().getContentBasedUuid(in.getBytes()).substring(24);
	}
	private static String safeTruncate(final String in, final int max_len) {
		return in.length() < max_len ? in : in.substring(0, max_len);
	}
	
	/** Gets the entry point for modular batch processing (enrichment)
	 * @param library_beans - the map of library beans from the context
	 * @param control - the control metadata bean
	 * @return
	 */
	public static Optional<String> getBatchEntryPoint(final Map<String, SharedLibraryBean> library_beans, final EnrichmentControlMetadataBean control) {
		return getEntryPoint(library_beans, () -> control.entry_point(), () -> control.module_name_or_id(),
				() -> new HashSet<String>(Optional.ofNullable(control.library_names_or_ids()).orElse(Collections.emptyList())),
							(SharedLibraryBean library) -> library.batch_enrichment_entry_point());
		
	} 
		
	/** Gets the entry point for modular stream processing (enrichment)
	 * @param library_beans - the map of library beans from the context
	 * @param control - the control metadata bean
	 * @return
	 */
	public static Optional<String> getStreamingEntryPoint(final Map<String, SharedLibraryBean> library_beans, final EnrichmentControlMetadataBean control) {
		return getEntryPoint(library_beans, () -> control.entry_point(), () -> control.module_name_or_id(),
				() -> new HashSet<String>(Optional.ofNullable(control.library_names_or_ids()).orElse(Collections.emptyList())),
							(SharedLibraryBean library) -> library.streaming_enrichment_entry_point());
		
	} 
	
	/** Gets the entry point for modular batch processing (analytics)
	 * @param library_beans - the map of library beans from the context
	 * @param control - the control metadata bean
	 * @return
	 */
	public static Optional<String> getBatchEntryPoint(final Map<String, SharedLibraryBean> library_beans, final AnalyticThreadJobBean control) {
		return getEntryPoint(library_beans, () -> control.entry_point(), () -> control.module_name_or_id(),
				() -> new HashSet<String>(Optional.ofNullable(control.library_names_or_ids()).orElse(Collections.emptyList())),
							(SharedLibraryBean library) -> library.batch_enrichment_entry_point());
		
	} 
		
	/** Gets the entry point for modular stream processing (analytics)
	 * @param library_beans - the map of library beans from the context
	 * @param control - the control metadata bean
	 * @return
	 */
	public static Optional<String> getStreamingEntryPoint(final Map<String, SharedLibraryBean> library_beans, final AnalyticThreadJobBean control) {
		return getEntryPoint(library_beans, () -> control.entry_point(), () -> control.module_name_or_id(),
				() -> new HashSet<String>(Optional.ofNullable(control.library_names_or_ids()).orElse(Collections.emptyList())),
							(SharedLibraryBean library) -> library.streaming_enrichment_entry_point());
		
	} 
	
	/** Gets the entry point for modular harvest processing
	 * @param library_beans - the map of library beans from the context
	 * @param control - the control metadata bean
	 * @return
	 */
	public static Optional<String> getEntryPoint(final Map<String, SharedLibraryBean> library_beans, final HarvestControlMetadataBean control) {
		return getEntryPoint(library_beans, () -> control.entry_point(), () -> control.module_name_or_id(),
				() -> new HashSet<String>(Optional.ofNullable(control.library_names_or_ids()).orElse(Collections.emptyList())),
							(SharedLibraryBean library) -> null);
		
	} 
	
	
	/** A generic function that provides the entry point for a harvest or enrichment control metadata bean
	 * @param library_beans - the map of library beans from the context
	 * @param control_bean_get_entry - returns the entry point from whatever the type of control bean is
	 * @param control_bean_get_module - returns the module id/name from whatever the type of control bean is
	 * @param control_bean_get_lib - returns a set of libraries from whatever the type of control bean is
	 * @param library_get_entry - returns the batch or streaming enrichment entry point
	 * @return
	 */
	protected static Optional<String> getEntryPoint(final Map<String, SharedLibraryBean> library_beans, 
			final Supplier<String> control_bean_get_entry, final Supplier<String> control_bean_get_module, final Supplier<Set<String>> control_bean_get_lib, 
			final Function<SharedLibraryBean, String> library_get_entry								
			)
	{		
		final Set<String> library_ids_or_names = control_bean_get_lib.get();
		return Optional.ofNullable(control_bean_get_entry.get()).map(Optional::of) // Option 1: entry point override specified
						.orElseGet(() -> 
									Optional.ofNullable(control_bean_get_module.get()) // Option 2: module specified .. use either batch or enrichment
										.map(m -> library_beans.get(m))
										.map(lib -> Optional.ofNullable(library_get_entry.apply(lib)).orElse(lib.misc_entry_point()))										
								)
						.map(Optional::of)
						// Option 3: get the first library bean with a batch entry point
						.orElseGet(() ->
									library_beans.entrySet().stream()
										.filter(kv -> library_ids_or_names.contains(kv.getKey()))
										.filter(kv -> (null != library_get_entry.apply(kv.getValue())))										
										.findFirst()
										.map(kv -> library_get_entry.apply(kv.getValue()))
								)
						.map(Optional::of)
						// Option 4: get the first library bean with a misc entry point
						.orElseGet(() -> 
								library_beans.entrySet().stream()
								.filter(kv -> library_ids_or_names.contains(kv.getKey()))
								.filter(kv -> (null != kv.getValue().misc_entry_point()))
								.findFirst()
								.map(kv -> kv.getValue().misc_entry_point())
								)
						;
	}
	
}
