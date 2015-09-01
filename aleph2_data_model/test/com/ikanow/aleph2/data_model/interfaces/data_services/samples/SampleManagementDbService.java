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

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.shared.AssetStateDirectoryBean;
import com.ikanow.aleph2.data_model.objects.shared.AssetStateDirectoryBean.StateDirectoryType;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.ProcessingTestSpecBean;
import com.ikanow.aleph2.data_model.objects.shared.ProjectBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.FutureUtils.ManagementFuture;

public class SampleManagementDbService implements IManagementDbService {

	@Override
	public IManagementCrudService<SharedLibraryBean> getSharedLibraryStore() {
		return null;
	}

	@Override
	public <T> ICrudService<T> getPerLibraryState(
			Class<T> clazz, SharedLibraryBean library,
			Optional<String> sub_collection) {
		return null;
	}

	@Override
	public IManagementCrudService<DataBucketBean> getDataBucketStore() {
		return null;
	}

	@Override
	public IManagementCrudService<DataBucketStatusBean> getDataBucketStatusStore() {
		return null;
	}

	@Override
	public <T> ICrudService<T> getBucketHarvestState(
			Class<T> clazz, DataBucketBean bucket,
			Optional<String> sub_collection) {
		return null;
	}

	@Override
	public <T> ICrudService<T> getBucketEnrichmentState(
			Class<T> clazz, DataBucketBean bucket,
			Optional<String> sub_collection) {
		return null;
	}
	
	@Override
	public <T> ICrudService<T> getBucketAnalyticThreadState(
			Class<T> clazz, DataBucketBean bucket,
			Optional<String> sub_collection) {
		return null;
	}

	@Override
	public <T> Optional<T> getUnderlyingPlatformDriver(
			Class<T> driver_class, Optional<String> driver_options) {
		return null;
	}

	@Override
	public IManagementDbService getFilteredDb(
			Optional<AuthorizationBean> client_auth,
			Optional<ProjectBean> project_auth) {
		return null;
	}

	@Override
	public <T> ICrudService<T> getRetryStore(
			Class<T> retry_message_clazz) {
		return null;
	}

	@Override
	public List<Object> getUnderlyingArtefacts() {
		return null;
	}

	@Override
	public IManagementDbService readOnlyVersion() {
		return null;
	}

	@Override
	public <T> ICrudService<T> getBucketDeletionQueue(
			Class<T> deletion_queue_clazz) {
		return null;
	}

	@Override
	public ICrudService<AssetStateDirectoryBean> getStateDirectory(
			Optional<DataBucketBean> bucket_filter, Optional<StateDirectoryType> type_filter) {
		return null;
	}

	@Override
	public ManagementFuture<Boolean> purgeBucket(DataBucketBean to_purge,
			Optional<Duration> in) {
		return null;
	}

	@Override
	public ManagementFuture<Boolean> testBucket(DataBucketBean to_test,
			ProcessingTestSpecBean test_spec) {
		return null;
	}

	@Override
	public <T> ICrudService<T> getBucketTestQueue(Class<T> test_queue_clazz) {
		return null;
	}

	@Override
	public IManagementDbService getSecureddDb(AuthorizationBean client_auth, Optional<ProjectBean> project_auth) {	
		return null;
	}


}
