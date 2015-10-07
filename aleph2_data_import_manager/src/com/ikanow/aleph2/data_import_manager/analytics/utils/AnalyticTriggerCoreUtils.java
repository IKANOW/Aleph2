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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.curator.framework.CuratorFramework;

import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerStateBean;

/** A set of utilities for retrieving and updating the trigger state
 * @author Alex
 */
public class AnalyticTriggerCoreUtils {

	/** Get a list of triggers indexed by bucket to examine
	 * @param trigger_crud
	 * @return
	 */
	public CompletableFuture<Map<String, List<AnalyticTriggerStateBean>>> getTriggersToCheck(final ICrudService<AnalyticTriggerStateBean> trigger_crud) {
		
		//TODO (ALEPH-12)
		
		return null;
	}
	
	/** Removes any triggers other users might have ownership over and grabs ownership
	 * @param all_triggers
	 * @param curator
	 * @return
	 */
	public Map<String, List<AnalyticTriggerStateBean>> registerOwnershipOfTriggers(final Map<String, List<AnalyticTriggerStateBean>> all_triggers, final CuratorFramework curator, final String process_id) 
	{
		
		//TODO (ALEPH-12)
		
		return all_triggers;
	}
	
	/** Deregister interest in triggers once we have completed processing them
	 * @param job_names 
	 * @param curator
	 */
	public void deregisterOwnershipOfTriggers(final Collection<String> job_names, final CuratorFramework curator) {
		//TODO (ALEPH-12)
	}
	
	//TODO (ALEPH-12): overwrite old logic with pending logic once a job is complete
}
