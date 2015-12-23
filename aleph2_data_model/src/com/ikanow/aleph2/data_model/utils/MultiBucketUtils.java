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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.Tuple2;

import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;

/** Utilities for handling multi-buckets and paths of buckets
 * @author Alex
 */
public class MultiBucketUtils {

	/** Expands a collection of fully formed normal buckets and minimally formed multi buckets into a set of fully formed normal buckets
	 *  DOESN'T CURRENTLY HANDLE NESTED MULTI-BUCKETS (BUT SHOULD LATER, JUST NEED TO ADD RECURSION TO THIS)
	 * @param buckets - can be 1+ multi buckets (including fake ones which just need owner_id and multi_bucket_children set, if owner_id not set then isn't secured)
	 * @param bucket_store
	 * @param service_context
	 * @return
	 */
	public static Map<String, DataBucketBean> expandMultiBuckets(
				final Collection<DataBucketBean> buckets, 
				final IManagementCrudService<DataBucketBean> bucket_store,
				final IServiceContext service_context)
	{
		
		// First get any buckets that we need to resolve
		//final Set<String> wildcard_bucket_list = 
		final Map<String, List<Tuple2<String, String>>> multi_bucket_list =
				buckets.stream()
					.flatMap(b -> Optional.ofNullable(b.multi_bucket_children())
											.map(s -> s.stream()
														.<Tuple2<String, String>>
															map(ss -> Tuples._2T(ss, Optional.ofNullable(b.owner_id()).orElse(""))))
											.orElse(Stream.empty())
					)
					.collect(Collectors.groupingBy((Tuple2<String, String> s_o) -> s_o._2()))
					;
		//(due to security architecture if we have any wild
		
		// Now get a final list of buckets
		
		final Map<String, DataBucketBean> final_bucket_list = 
				(multi_bucket_list.isEmpty()
				? buckets.stream()
				: Stream.concat(buckets.stream(), Lambdas.get(() -> {
					
					final List<CompletableFuture<Stream<DataBucketBean>>> res = multi_bucket_list.entrySet().stream().parallel()
							.<CompletableFuture<Stream<DataBucketBean>>>map(kv -> {
								
								final List<String> multi_paths = kv.getValue().stream().map(s_o -> s_o._1()).collect(Collectors.toList());
								
								//DEBUG
								//System.out.println("PATHS = " + wildcard_paths.stream().collect(Collectors.joining(";")));
								
								final QueryComponent<DataBucketBean> query = 
										BucketUtils.getApproxMultiBucketQuery(multi_paths);
								
								final Predicate<String> filter = BucketUtils.refineMultiBucketQuery(multi_paths);
								
								final ICrudService<DataBucketBean> secured_bucket_store = 
										Optional.of(bucket_store)
												.map(db -> kv.getKey().isEmpty() ? db : db.secured(service_context, new AuthorizationBean(kv.getKey())))
												.get()
												;
								
								//DEBUG
								//System.out.println("OWNER: " + kv.getKey() + " .. " + bucket_store.countObjects().join().intValue() + " query = \n" + query.toString());
								
								return secured_bucket_store												
											.getObjectsBySpec(query)
											.thenApply(cursor -> {
												return Optionals.streamOf(cursor.iterator(), false)
															.filter(b -> filter.test(b.full_name()))
															;
											});
							})
							.collect(Collectors.toList());
					
					// Wait for everything to stop
					CompletableFuture.allOf(res.stream().toArray(CompletableFuture[]::new)).join();
					
					// Create a stream of buckets
					
					return res.stream().flatMap(cf -> cf.join());
					
				}))
				).collect(Collectors.toMap(b -> b.full_name(), b -> b, (b1, b2) -> b1)) // (deduplicate)
				;		
		
		return final_bucket_list;
	}
}
