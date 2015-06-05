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
package com.ikanow.aleph2.data_model.interfaces.data_services.samples;

import java.util.Map;


import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.objects.shared.Identity;

public class SampleSecurityService implements ISecurityService {
	
	@Override
	public boolean hasPermission(Identity identity,
			Class<?> resourceClass, String resourceIdentifier,
			String operation) {
		return false;
	}

	@Override
	public boolean hasPermission(Identity identity,
			String resourceName, String resourceIdentifier,
			String operation) {
		return false;
	}

	@Override
	public Identity getIdentity(Map<String, Object> token)
			throws Exception {
		return null;
	}

	@Override
	public void grantPermission(Identity identity,
			Class<?> resourceClass, String resourceIdentifier,
			String operation) {
	}

	@Override
	public void grantPermission(Identity identity,
			String resourceName, String resourceIdentifier,
			String operation) {
	}

	@Override
	public void revokePermission(Identity identity,
			Class<?> resourceClass, String resourceIdentifier,
			String operation) {
	}

	@Override
	public void revokePermission(Identity identity,
			String resourceName, String resourceIdentifier,
			String operation) {
	}

	@Override
	public void clearPermission(Class<?> resourceClass,
			String resourceIdentifier) {
	}

	@Override
	public void clearPermission(String resourceName,
			String resourceIdentifier) {
	}
}
