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
package com.ikanow.aleph2.data_import_manager.harvest.utils;

/** Errors specific to the harvester
 * @author Alex
 */
public class HarvestErrorUtils extends com.ikanow.aleph2.data_model.utils.ErrorUtils {

	public static final String NO_TECHNOLOGY_NAME_OR_ID = "No harvest technology name or id in bucket {0}";
	public static final String HARVEST_TECHNOLOGY_NAME_NOT_FOUND = "No valid harvest technology {0} found for bucket {1}";
	public static final String MESSAGE_NOT_RECOGNIZED = "Message type {1} not recognized for bucket {0}";
	public static final String HARVEST_TECH_ERROR = "Harvest technology from bucket {1} (harvest tech {2}) manually called exception: {0}";
	public static final String HARVEST_UNKNOWN_ERROR = "Unknown error from bucket {1} called exception: {0}";	 
	public static final String TRIED_TO_RUN_MULTI_NODE_ON_UNSUPPORTED_TECH = "Tried to create a multi-node bucket {0} but technology {1} does not support multi-node: set 'multi_node_enabled' field to 'false'";
}
