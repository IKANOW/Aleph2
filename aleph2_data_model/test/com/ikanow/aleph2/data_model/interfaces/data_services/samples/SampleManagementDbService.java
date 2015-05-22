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

import java.util.Optional;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.ProjectBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;

public class SampleManagementDbService implements IManagementDbService {

	@Override
	public @NonNull IManagementCrudService<SharedLibraryBean> getSharedLibraryStore() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> @NonNull ICrudService<T> getPerLibraryState(
			@NonNull Class<T> clazz, @NonNull SharedLibraryBean library,
			@NonNull Optional<String> sub_collection) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public @NonNull IManagementCrudService<DataBucketBean> getDataBucketStore() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public @NonNull IManagementCrudService<DataBucketStatusBean> getDataBucketStatusStore() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> @NonNull ICrudService<T> getPerBucketState(
			@NonNull Class<T> clazz, @NonNull DataBucketBean bucket,
			@NonNull Optional<String> sub_collection) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public @NonNull IManagementCrudService<AnalyticThreadBean> getAnalyticThreadStore() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> @NonNull ICrudService<T> getPerAnalyticThreadState(
			@NonNull Class<T> clazz,
			@NonNull AnalyticThreadBean analytic_thread,
			@NonNull Optional<String> sub_collection) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> @NonNull T getUnderlyingPlatformDriver(
			@NonNull Class<T> driver_class, Optional<String> driver_options) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public @NonNull IManagementDbService getFilteredDb(
			Optional<AuthorizationBean> client_auth,
			Optional<ProjectBean> project_auth) {
		// TODO Auto-generated method stub
		return null;
	}

}
