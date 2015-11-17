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
package com.ikanow.aleph2.data_import_manager.analytics.utils;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

import scala.Tuple2;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.ikanow.aleph2.data_model.utils.BucketUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerStateBean;
import com.ikanow.aleph2.management_db.utils.ActorUtils;

/** Utilities related to core services
 * @author Alex
 */
public class AnalyticTriggerCoreUtils {
	protected final static Cache<String, InterProcessMutex> _mutex_cache = CacheBuilder.newBuilder().expireAfterAccess(2, TimeUnit.HOURS).build();

	/** Removes any triggers other users might have ownership over and grabs ownership
	 * @param all_triggers - triggers indexed by bucket (+host for host-locked jobs)
	 * @param process_id
	 * @param curator
	 * @param mutex_fail_handler - sets the duration of the mutex wait, and what to do if it fails (ie for triggers, short wait and do nothing; for bucket message wait for longer and save them to a retry queue)
	 * @return - filtered trigger set, still indexed by bucket
	 */
	public static Map<Tuple2<String, String>, List<AnalyticTriggerStateBean>> registerOwnershipOfTriggers(
			final Map<Tuple2<String, String>, List<AnalyticTriggerStateBean>> all_triggers, 
			final String process_id,
			final CuratorFramework curator, 
			final Tuple2<Duration, Consumer<String>> mutex_fail_handler
			)
	{		
		return all_triggers.entrySet()
			.stream() //(can't be parallel - has to happen in the same thread)
			.map(kv -> Tuples._2T(kv, ActorUtils.BUCKET_ANALYTICS_TRIGGER_ZOOKEEEPER + BucketUtils.getUniqueSignature(kv.getKey()._1(), Optional.ofNullable(kv.getKey()._2()))))
			.flatMap(Lambdas.flatWrap_i(kv_path -> 
						Tuples._2T(kv_path._1(), _mutex_cache.get(kv_path._2(), () -> { return new InterProcessMutex(curator, kv_path._2()); }))))
			.flatMap(Lambdas.flatWrap_i(kv_mutex -> {
				if (kv_mutex._2().acquire(mutex_fail_handler._1().getSeconds(), TimeUnit.SECONDS)) {
					return kv_mutex._1();
				}
				else {
					mutex_fail_handler._2().accept(kv_mutex._1().getKey().toString()); // (run this synchronously, the callable can always run in a different thread if it wants to)
					throw new RuntimeException(""); // (leaves the flatWrap empty)
				}
			}))
			.collect(Collectors.toMap(kv -> kv.getKey(), kv -> kv.getValue()))
			;
	}
	
	/** Deregister interest in triggers once we have completed processing them
	 * @param job_names 
	 * @param curator
	 */
	public static void deregisterOwnershipOfTriggers(final Collection<Tuple2<String, String>> path_names, CuratorFramework curator) {
		path_names.stream() //(can't be parallel - has to happen in the same thread)
			.map(path -> ActorUtils.BUCKET_ANALYTICS_TRIGGER_ZOOKEEEPER + BucketUtils.getUniqueSignature(path._1(), Optional.ofNullable(path._2())))
			.map(path -> Tuples._2T(path, _mutex_cache.getIfPresent(path)))
			.filter(path_mutex -> null != path_mutex._2())
			.forEach(Lambdas.wrap_consumer_i(path_mutex -> path_mutex._2().release()));
	}
	
}
