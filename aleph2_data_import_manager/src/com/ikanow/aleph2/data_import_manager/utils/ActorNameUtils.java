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

/** Names of the various actors
 * @author Alex
 */
public class ActorNameUtils {

	// Hostnames are prepended to all the suffix* names
	
	public static final String HARVEST_BUCKET_CHANGE_SUFFIX = ".harvest.actors.DataBucketHarvestChangeActor";
	public static final String ANALYTICS_BUCKET_CHANGE_SUFFIX = ".analytics.actors.DataBucketAnalyticsChangeActor";
	public static final String ANALYTICS_TRIGGER_SUPERVISOR_SUFFIX = ".analytics.actors.AnalyticsTriggerWorkerActor";
	public static final String ANALYTICS_TRIGGER_WORKER_SUFFIX = ".analytics.actors.AnalyticsTriggerSupervisorActor";	
}
