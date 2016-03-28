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

import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.ColumnarSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.SearchIndexSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.StorageSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.StorageSchemaBean.StorageSubSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.TemporalSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.data_import.HarvestControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.data_import.ManagementSchemaBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;

/**
 * Provides a set of util functions for DataBucketBean
 * 
 * @author Burch
 *
 */
public class BucketUtils {
	public static final String RESERVED_BUCKET_PREFIX = "/aleph2_";
	public static final String TEST_BUCKET_PREFIX = "/aleph2_testing/";
	public static final String LOG_BUCKET_PREFIX = "/aleph2_logging";
	public static final String EXTERNAL_BUCKET_PREFIX = "/aleph2_external/";
	private static final String BUCKETS_PREFIX = "/buckets";
	
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
	
	/**
	 * Returns back a DataBucketBean w/ the full_name changed to reflect the logging path.
	 * i.e. just prefixes the bucket.full_name with LOGGING_PREFIX (/alelph2_logging) and
	 * copies the management_data_schema overtop of data_schema
	 * 
	 * @param bucket
	 * @return
	 */
	public static DataBucketBean convertDataBucketBeanToLogging(final DataBucketBean bucket) {
		return BeanTemplateUtils.clone(bucket)
				.with(DataBucketBean::full_name, LOG_BUCKET_PREFIX + BUCKETS_PREFIX + bucket.full_name())
				.with(DataBucketBean::data_schema, getLoggingDataSchema(bucket.management_schema()))
				.done();
	}
	
	/** Check if a bucket is a test bucket (trivial)
	 * @param bucket - the bucket to check
	 * @return
	 */
	public static boolean isLoggingBucket(final DataBucketBean bucket) {
		return bucket.full_name().startsWith(LOG_BUCKET_PREFIX);
	}
	
	/**
	 * Returns a DataSchemaBean with the passed in mgmt schema schemas copied over or defaults inserted
	 * 
	 * @param mgmt_schema
	 * @return
	 */
	private static DataSchemaBean getLoggingDataSchema(final ManagementSchemaBean mgmt_schema) {
		return BeanTemplateUtils.build(DataSchemaBean.class)
				.with(DataSchemaBean::columnar_schema, Optionals.of(() -> mgmt_schema.columnar_schema()).orElse(BeanTemplateUtils.build(ColumnarSchemaBean.class)
							.with(ColumnarSchemaBean::field_type_include_list, Arrays.asList("number", "string", "date"))
						.done().get()))
				.with(DataSchemaBean::storage_schema, Optionals.of(() -> mgmt_schema.storage_schema()).orElse(BeanTemplateUtils.build(StorageSchemaBean.class)
							.with(StorageSchemaBean::enabled, true)
							.with(StorageSchemaBean::processed, BeanTemplateUtils.build(StorageSubSchemaBean.class)
										.with(StorageSubSchemaBean::exist_age_max, "3 months")
										.with(StorageSubSchemaBean::grouping_time_period, "1 week")
									.done().get())
						.done().get()))
				.with(DataSchemaBean::search_index_schema, Optionals.of(() -> mgmt_schema.search_index_schema()).orElse(BeanTemplateUtils.build(SearchIndexSchemaBean.class)
						.done().get()))
				.with(DataSchemaBean::temporal_schema, Optionals.of(() -> mgmt_schema.temporal_schema()).orElse(BeanTemplateUtils.build(TemporalSchemaBean.class)
							.with(TemporalSchemaBean::exist_age_max, "1 month")
							.with(TemporalSchemaBean::grouping_time_period, "1 week")
						.done().get()))
			.done().get();	
	}
	
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
	
	///////////////////////////////////////////////////////////////////////////

	// BUCKET CRUD UTILS
	
	/** Returns a CRUD query that returns a minimal superset of the requested buckets from the specified globs
	 *  which can be used together with refineMultiBucketQuery to return exactly the desired buckets
	 * @param buckets - a list of bucket paths including globs (eg ** and *)
	 * @return
	 */
	public static QueryComponent<DataBucketBean> getApproxMultiBucketQuery(final Collection<String> buckets) {
		// (just do this simply to start with and add performance if needed)
		
		// Split into simple and complex cases:
		final String regex = "[?*].*$";
		
		// Simple case:
		final Set<String> simple_case = buckets.stream()
			.flatMap(s -> {
				final String trans_s = s.replaceFirst(regex, "");
				if (s.length() == trans_s.length()) {
					return Stream.of(s); // no change
				}
				else if (trans_s.endsWith("/")) { // is included in simple and complex case, see below
					return Stream.of(trans_s.substring(0, trans_s.length() - 1)); // (remove the trailing '/')				
				}
				else {
					return Stream.empty();
				}
			})			
			.collect(Collectors.toSet())
			;

		final QueryComponent<DataBucketBean> complex_case = buckets.stream()
			.flatMap(s -> {
				final String trans_s = s.replaceFirst(regex, "");
				if (s.length() == trans_s.length()) {
					return Stream.empty(); // already handled by simple case above
				}
				else {
					return Stream.of(Tuples._2T(trans_s, getUpperLimit(trans_s)));
				}
			})
			.reduce(CrudUtils.anyOf(DataBucketBean.class)
					,
					(acc, v) -> acc.rangeIn(DataBucketBean::full_name, v._1(), false, v._2(), true)
					,
					(acc1, acc2) -> acc1 //(not reachable)
					)
			;		
		
		return CrudUtils.anyOf( // (big OR statement)
				CrudUtils.anyOf(DataBucketBean.class).withAny(DataBucketBean::full_name, simple_case)
				,
				complex_case
				);
	}
	
	/** Utility function for queries
	 * @param in
	 * @return
	 */
	protected static String getUpperLimit(final String in) {
		char x = in.charAt(in.length() - 1);
		return in.substring(0, in.length() - 1) + ((char)(x + 1));
	}
	
	/** Returns a predicate that can be used in a Stream.filter operation to filter out any non-matching buckets
	 * @param buckets - a list of bucket paths including globs (eg ** and *)
	 * @return
	 */
	public static Predicate<String> refineMultiBucketQuery(final Collection<String> buckets) {
		// (just do this simply to start with and add performance if needed)
		
		// Split into simple and complex cases:
		final String regex = "[?*].*$";
		
		// simple case:
		final Set<String> simple_case = buckets.stream()
				.filter(s -> {
					final String trans_s = s.replaceFirst(regex, "");
					return (s.length() == trans_s.length());
				})
				.collect(Collectors.toSet())
				;
		
		// complex case:
		final List<PathMatcher> complex_case = buckets.stream()
				.filter(s -> {
					final String trans_s = s.replaceFirst(regex, "");
					return (s.length() != trans_s.length());
				})
				.map(s -> FileSystems.getDefault().getPathMatcher("glob:" + s))
				.collect(Collectors.toList())
				;
			
		return s -> {
			if (simple_case.contains(s)) {
				return true;
			}
			else {
				final java.nio.file.Path p = FileSystems.getDefault().getPath(s);
				return complex_case.stream().anyMatch(matcher -> matcher.matches(p));
			}
		};
	}
	
	///////////////////////////////////////////////////////////////////////////

	// ENTRY POINT UTILS FOR HARVEST/ENRICHMENT/ANALYTICS TECHNOLOGY		
	
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
