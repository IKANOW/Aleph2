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
package com.ikanow.aleph2.management_db.module;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import com.google.inject.Inject;
import com.google.inject.Module;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IExtraDependencyLoader;
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
import com.ikanow.aleph2.management_db.module.MockUnderlyingManagementDbModule.IMockUnderlyingCrudServiceFactory;

public class MockUnderlyingManagementDbService implements IManagementDbService, IExtraDependencyLoader {

	protected IMockUnderlyingCrudServiceFactory _crud_factory;
	
	@Inject
	public MockUnderlyingManagementDbService(
			IMockUnderlyingCrudServiceFactory crud_factory) {
		_crud_factory = crud_factory;
		//DEBUG
		//System.out.println("Hello world from: " + this.getClass() + ": underlying=" + crud_factory);
	}
	
	public IManagementCrudService<SharedLibraryBean> getSharedLibraryStore() {
		return null;
	}

	public <T> ICrudService<T> getPerLibraryState(Class<T> clazz,
			SharedLibraryBean library, Optional<String> sub_collection) {
		return null;
	}

	public IManagementCrudService<DataBucketBean> getDataBucketStore() {
		return null;
	}

	public <T> ICrudService<T> getPerBucketState(Class<T> clazz,
			DataBucketBean bucket, Optional<String> sub_collection) {
		return null;
	}

	public <T> Optional<T> getUnderlyingPlatformDriver(Class<T> driver_class,
			Optional<String> driver_options) {
		return null;
	}

	/** This service needs to load some additional classes via Guice. Here's the module
	 * @return
	 */
	public List<Module> getDependencyModules() {
		return Arrays.asList(new MockUnderlyingManagementDbModule());
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IExtraDependencyLoader#youNeedToImplementTheStaticFunctionCalled_getExtraDependencyModules()
	 */
	@Override
	public void youNeedToImplementTheStaticFunctionCalled_getExtraDependencyModules() {
		// (done see above)		
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
	public Collection<Object> getUnderlyingArtefacts() {
		return null;
	}

	@Override
	public IManagementDbService readOnlyVersion() {
		return null;
	}

	@Override
	public <T> ICrudService<T> getBucketHarvestState(Class<T> clazz,
			DataBucketBean bucket, Optional<String> collection) {
		return null;
	}

	@Override
	public <T> ICrudService<T> getBucketEnrichmentState(Class<T> clazz,
			DataBucketBean bucket, Optional<String> sub_collection) {
		return null;
	}

	@Override
	public <T> ICrudService<T> getBucketAnalyticThreadState(Class<T> clazz,
			DataBucketBean bucket, Optional<String> collection) {
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
	public IManagementCrudService<DataBucketStatusBean> getDataBucketStatusStore() {
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

}
