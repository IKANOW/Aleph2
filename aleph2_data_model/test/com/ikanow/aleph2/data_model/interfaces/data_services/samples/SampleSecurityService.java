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

import org.checkerframework.checker.nullness.qual.NonNull;

import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.objects.shared.Identity;

public class SampleSecurityService implements ISecurityService {
	
	@Override
	public boolean hasPermission(@NonNull Identity identity,
			@NonNull Class<?> resourceClass, String resourceIdentifier,
			@NonNull String operation) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean hasPermission(@NonNull Identity identity,
			@NonNull String resourceName, String resourceIdentifier,
			@NonNull String operation) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Identity getIdentity(@NonNull Map<String, Object> token)
			throws Exception {
		return null;
	}

	@Override
	public void grantPermission(@NonNull Identity identity,
			@NonNull Class<?> resourceClass, String resourceIdentifier,
			@NonNull String operation) {
		// TODO Auto-generated method stub

	}

	@Override
	public void grantPermission(@NonNull Identity identity,
			@NonNull String resourceName, String resourceIdentifier,
			@NonNull String operation) {
		// TODO Auto-generated method stub

	}

	@Override
	public void revokePermission(@NonNull Identity identity,
			@NonNull Class<?> resourceClass, String resourceIdentifier,
			@NonNull String operation) {
		// TODO Auto-generated method stub

	}

	@Override
	public void revokePermission(@NonNull Identity identity,
			@NonNull String resourceName, String resourceIdentifier,
			@NonNull String operation) {
		// TODO Auto-generated method stub

	}

	@Override
	public void clearPermission(@NonNull Class<?> resourceClass,
			String resourceIdentifier) {
		// TODO Auto-generated method stub

	}

	@Override
	public void clearPermission(@NonNull String resourceName,
			String resourceIdentifier) {
		// TODO Auto-generated method stub

	}
}
