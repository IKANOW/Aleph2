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
package com.ikanow.aleph2.data_model.interfaces.shared_services;

import java.util.concurrent.CompletableFuture;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;


import scala.Tuple2;

/** Provides very basic Lucene-type search services - querying, filtering, and faceting
 * @author acp
 */
public interface IBasicSearchService<O> {

	/** Count the number of objects matching a simple lucene-type query on the underlying data, where supported
	 * @param query_string - A lucene formatted query
	 * @param filter_string - A lucene formatted filter (doesn't affect scoring-based sort order, is sometimes faster)
	 * @return a future containing the number of matching objects
	 */
	CompletableFuture<Long> count(final Optional<String> query_string, final Optional<String> filter_string);
	
	/** Performs a simple lucene-type query on the underlying data, where supported
	 * @param query_string - A lucene formatted query
	 * @param filter_string - A lucene formatted filter (doesn't affect scoring-based sort order, is sometimes faster)
	 * @param limit - Optional max number of objects to return (defaults to 100)
	 * @param order_by - Optional ordering - first term in tuple is fieldname, second is <0 for ascending >0 for descending (if not specified then orders by score)
	 * @param skip - Optional initial number of records to ignore
	 * @param return_fields - list of fields to return
	 * @param facet_fields - A map of field name (supports dot notation) vs max number of terms to bring back as facets	  
	 * @return a 2-tuple - the first is a collection of matching objects, the second is a map of fields to facet on (value is the long
	 */
	CompletableFuture<Tuple2<Iterable<O>, Optional<Map<String, Collection<Tuple2<String, Long>>>>>> 
		search(final Optional<String> query_string, final Optional<String> filter_string, final Optional<Integer> limit,
				final Optional<Tuple2<String, Integer>> orderBy, final Optional<Integer> skip, final Optional<Collection<String>> return_fields,
				final Optional<Map<String, Long>> facet_fields);

	/** Performs a simple lucene-type query on the underlying data, where supported
	 * @param query_string - A lucene formatted query
	 * @param filter_string - A lucene formatted filter (doesn't affect scoring-based sort order, is sometimes faster)
	 * @param limit - Optional max number of objects to return (defaults to 100)
	 * @param order_by - Optional ordering - first term in tuple is fieldname, second is <0 for ascending >0 for descending (if not specified then orders by score)
	 * @param skip - Optional initial number of records to ignore
	 * @param return_fields - list of fields to return
	 * @return a a collection of matching objects
	 */
	CompletableFuture<Iterable<O>> 
		search(final Optional<String> query_string, final Optional<String> filter_string, final Optional<Integer> limit,
				final Optional<Tuple2<String, Integer>> orderBy, final Optional<Integer> skip, final Optional<Collection<String>> return_fields);
	
	/** Performs a simple lucene-type query on the underlying data, where supported
	 * @param query_string - A lucene formatted query
	 * @param filter_string - A lucene formatted filter (doesn't affect scoring-based sort order, is sometimes faster)
	 * @param limit - Optional max number of objects to return (defaults to 100)
	 * @return a a collection of matching objects
	 */
	CompletableFuture<Iterable<O>> search(final Optional<String> query_string, final Optional<String> filter_string, final Optional<Integer> limit);
	
	/** USE WITH CARE: this returns the driver to the underlying technology
	 *  shouldn't be used unless absolutely necessary!
	 * @param driver_class the class of the driver
	 * @param a string containing options in some technology-specific format
	 * @return a driver to the underlying technology. Will exception if you pick the wrong one!
	 */
	<T> T getUnderlyingPlatformDriver(final Class<T> driver_class, final Optional<String> driver_options);
}
