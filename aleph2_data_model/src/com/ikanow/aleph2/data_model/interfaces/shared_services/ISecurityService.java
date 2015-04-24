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

public interface ISecurityService {

	enum AccessType { read_only, read_write, unelevated_admin, admin };
	
	//TODO (ALEPH-2): send: user-id, credentials, credentials type, desired tokens (*), return list of "role tokens"
	// token:  role (owner/admin/rw/r), these are then associated with each artefact type
	// ie they are user groups
	
	//TODO (ALEPH-2) request privilege escalation
	
	//TODO (ALEPH-2): use futures everywhere
}
