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
package com.ikanow.aleph2.management_db.services;

import java.util.Optional;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;

/** A layer that sits in between the managers and modules on top, and the actual database technology underneath,
 *  and performs control activities (launching into Akka) and an additional layer of validation
 * @author acp
 */
public class CoreManagementDbService implements IManagementDbService {

	protected final IManagementDbService _underlying_management_db;	
	protected DataBucketCrudService _data_bucket_service;
	
	/** Guice invoked constructor
	 * @param underlying_management_db
	 * @param data_bucket_service
	 */
	@Inject
	public CoreManagementDbService(final @Named("management_db_service.underlying") IManagementDbService underlying_management_db,
			final DataBucketCrudService data_bucket_service)
	{
		_underlying_management_db = underlying_management_db;
		_data_bucket_service = data_bucket_service;

		//DEBUG
		//System.out.println("Hello world from: " + this.getClass() + ": bucket=" + _data_bucket_service);
		//System.out.println("Hello world from: " + this.getClass() + ": underlying=" + _underlying_management_db);
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getSharedLibraryStore()
	 */
	public ICrudService<SharedLibraryBean> getSharedLibraryStore() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getPerLibraryState(java.lang.Class, com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean, java.util.Optional)
	 */
	public <T> ICrudService<T> getPerLibraryState(Class<T> clazz,
			SharedLibraryBean library, Optional<String> sub_collection) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getDataBucketStore()
	 */
	public ICrudService<DataBucketBean> getDataBucketStore() {
		return _data_bucket_service;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getDataBucketStatusStore()
	 */
	public ICrudService<DataBucketStatusBean> getDataBucketStatusStore() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getPerBucketState(java.lang.Class, com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Optional)
	 */
	public <T> ICrudService<T> getPerBucketState(Class<T> clazz,
			DataBucketBean bucket, Optional<String> sub_collection) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getAnalyticThreadStore()
	 */
	public ICrudService<AnalyticThreadBean> getAnalyticThreadStore() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getPerAnalyticThreadState(java.lang.Class, com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadBean, java.util.Optional)
	 */
	public <T> ICrudService<T> getPerAnalyticThreadState(Class<T> clazz,
			AnalyticThreadBean analytic_thread, Optional<String> sub_collection) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getUnderlyingPlatformDriver(java.lang.Class, java.util.Optional)
	 */
	public <T> T getUnderlyingPlatformDriver(Class<T> driver_class,
			Optional<String> driver_options) {
		// TODO Auto-generated method stub
		return null;
	}

}
