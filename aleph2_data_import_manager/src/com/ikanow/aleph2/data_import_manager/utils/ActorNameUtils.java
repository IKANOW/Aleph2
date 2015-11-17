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
package com.ikanow.aleph2.data_import_manager.utils;

/** Names of the various actors
 * @author Alex
 */
public class ActorNameUtils {

	// Hostnames are prepended to all the suffix* names
	
	public static final String HARVEST_BUCKET_CHANGE_SUFFIX = ".harvest.actors.DataBucketHarvestChangeActor";
	public static final String ANALYTICS_BUCKET_CHANGE_SUFFIX = ".analytics.actors.DataBucketAnalyticsChangeActor";
	public static final String ANALYTICS_TRIGGER_WORKER_SUFFIX = ".analytics.actors.AnalyticsTriggerWorkerActor";
	public static final String ANALYTICS_TRIGGER_SUPERVISOR_SUFFIX = ".analytics.actors.AnalyticsTriggerSupervisorActor";	
}
