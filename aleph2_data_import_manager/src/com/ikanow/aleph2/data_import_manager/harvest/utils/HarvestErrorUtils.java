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
