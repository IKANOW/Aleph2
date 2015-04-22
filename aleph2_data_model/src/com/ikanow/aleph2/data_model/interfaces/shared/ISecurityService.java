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
package com.ikanow.aleph2.data_model.interfaces.shared;

import java.util.List;

import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;

/**
 * This class has 2 jobs:
 * 
 * 1. Check if a user has access to a requested resource, this should work by
 * passing in the user token (aka cookie or whatever we use) which will get that
 * user's usergroups.  We then check if at least one of those usergroups are allowed to 
 * access the resource
 * 
 * e.g. a gui will try to call api1, before the call gets routed to api1,
 * our base api service will call this security service to check if the user can
 * access api1
 * 
 * 2. Check if a user(group?) has access to a requested data service operation. I think we want
 * to do this via usergroups only so the dataservice that is requesting an action will pass in a
 * list of usergroups and the action they want to perform, this will check if all usergroups can 
 * perform that action.  For this to work dataservices will have to register the actions they want
 * to be able to check against and which usergroups can perform those actions.
 * 
 * e.g. that same guy call that routed to api1 is trying to add a user to the management db
 * before adding the user, api1 will call this security service to see if the user can
 * perform adding users
 * 
 * @author Burch
 *
 */
public interface ISecurityService {

	enum AccessType { read_only, read_write, unelevated_admin, admin };
	
	//TODO send: user-id, credentials, credentials type, desired tokens (*), return list of "role tokens"
	// token:  role (owner/admin/rw/r), these are then associated with each artefact type
	// ie they are user groups
	
	//TODO request privilege escalation
	
	//TODO use futures everywhere
	
	//this is for validating access to data
	public boolean checkBucketAccess(String token, DataBucketBean bucket);
	
	//this is for validating access to any other part of the application (plugins, individual api calls, etc)
	public void addSecurityRule(String ruleName, List<String> allowedUsergroups);
	public boolean checkSecurityRule(String ruleName, List<String> usergroups);
}
