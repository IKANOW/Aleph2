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

import java.util.Optional;

import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.management_db.module.MockUnderlyingManagementDbModule.MockUnderlyingCrudServiceFactory;

public class MockUnderlyingManagementDbService implements IManagementDbService {

	protected MockUnderlyingCrudServiceFactory _crud_factory;
	
	@Inject
	public MockUnderlyingManagementDbService(MockUnderlyingCrudServiceFactory crud_factory) {
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

	public IManagementCrudService<DataBucketStatusBean> getDataBucketStatusStore() {
		return null;
	}

	public <T> ICrudService<T> getPerBucketState(Class<T> clazz,
			DataBucketBean bucket, Optional<String> sub_collection) {
		return null;
	}

	public IManagementCrudService<AnalyticThreadBean> getAnalyticThreadStore() {
		return null;
	}

	public <T> ICrudService<T> getPerAnalyticThreadState(Class<T> clazz,
			AnalyticThreadBean analytic_thread, Optional<String> sub_collection) {
		return null;
	}

	public <T> T getUnderlyingPlatformDriver(Class<T> driver_class,
			Optional<String> driver_options) {
		return null;
	}

}
