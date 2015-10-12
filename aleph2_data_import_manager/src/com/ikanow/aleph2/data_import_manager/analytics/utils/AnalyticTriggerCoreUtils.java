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
package com.ikanow.aleph2.data_import_manager.analytics.utils;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

import scala.Tuple2;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerStateBean;

/** A set of utilities for retrieving and updating the trigger state
 * @author Alex
 */
public class AnalyticTriggerCoreUtils {
	protected final static Cache<String, InterProcessMutex> _mutex_cache = CacheBuilder.newBuilder().expireAfterAccess(2, TimeUnit.HOURS).build();

	/** Get a list of triggers indexed by bucket to examine
	 * @param trigger_crud
	 * @return
	 */
	public static CompletableFuture<Map<String, List<AnalyticTriggerStateBean>>> getTriggersToCheck(final ICrudService<AnalyticTriggerStateBean> trigger_crud) {		
		//TODO (ALEPH-12)
		
		return null;
	}
	
	/** Removes any triggers other users might have ownership over and grabs ownership
	 * @param all_triggers
	 * @param process_id
	 * @param curator
	 * @param mutex_fail_handler - sets the duration of the mutex wait, and what to do if it fails (ie for triggers, short wait and do nothing; for bucket message wait for longer and save them to a retry queue)
	 * @return
	 */
	public static Map<String, List<AnalyticTriggerStateBean>> registerOwnershipOfTriggers(
			final Map<String, List<AnalyticTriggerStateBean>> all_triggers, 
			final String process_id,
			final CuratorFramework curator, 
			final Tuple2<Duration, Runnable> mutex_fail_handler
			)
	{		
		//TODO (ALEPH-12) .. hmm i don't think this is gonna work is it? if i receive a one-off bucket message
		// then i can't just ignore it because a trigger is running .. would have to dump to a temp database and then read back ugh
		// alternative would be to use mutexes....i think maybe i prefer that to start with
		
		//TODO: need to be pretty aggressive with try/catches in here to make sure we don't leak away a mutex...
		
		return all_triggers;
	}
	
	/** Deregister interest in triggers once we have completed processing them
	 * @param job_names 
	 * @param curator
	 */
	public static void deregisterOwnershipOfTriggers(final Collection<String> path_names, CuratorFramework curator) {
		//TODO (ALEPH-12)
	}
	
	public static boolean isAnalyticJobActive(final DataBucketBean analytic_bucket, final AnalyticThreadJobBean job) {
		//TODO (ALEPH-12)
		return false;
	}
	
	//TODO (ALEPH-12): overwrite old logic with pending logic once a job is complete
}
