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

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.inject.Inject;
import com.google.inject.Module;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IExtraDependencyLoader;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.ProjectBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.management_db.module.CoreManagementDbModule;

/** A layer that sits in between the managers and modules on top, and the actual database technology underneath,
 *  and performs control activities (launching into Akka) and an additional layer of validation
 * @author acp
 */
public class CoreManagementDbService implements IManagementDbService, IExtraDependencyLoader {
	@SuppressWarnings("unused")
	private static final Logger _logger = LogManager.getLogger();	

	protected final IManagementDbService _underlying_management_db;	
	protected final DataBucketCrudService _data_bucket_service;
	protected final DataBucketStatusCrudService _data_bucket_status_service;
	protected final SharedLibraryCrudService _shared_library_service;
	
	protected final Optional<AuthorizationBean> _auth;
	protected final Optional<ProjectBean> _project;	
	
	/** Guice invoked constructor
	 * @param underlying_management_db
	 * @param data_bucket_service
	 */
	@Inject
	public CoreManagementDbService(final IServiceContext service_context,
			final DataBucketCrudService data_bucket_service, final DataBucketStatusCrudService data_bucket_status_service,
			final SharedLibraryCrudService shared_library_service
			)
	{
		//(just return null here if underlying management not present, things will fail catastrophically unless this is a test)
		_underlying_management_db = service_context.getService(IManagementDbService.class, Optional.empty()).orElse(null);
		_data_bucket_service = data_bucket_service;
		_data_bucket_status_service = data_bucket_status_service;
		_shared_library_service = shared_library_service;
		
		_auth = Optional.empty();
		_project = Optional.empty();
		
		//DEBUG
		//System.out.println("Hello world from: " + this.getClass() + ": bucket=" + _data_bucket_service);
		//System.out.println("Hello world from: " + this.getClass() + ": underlying=" + _underlying_management_db);

	}
	
	/** User constructor for building a cloned version with different auth settings
	 * @param crud_factory 
	 * @param auth_fieldname
	 * @param auth
	 * @param project
	 */
	public CoreManagementDbService(final IManagementDbService underlying_management_db,
			final DataBucketCrudService data_bucket_service, final DataBucketStatusCrudService data_bucket_status_service,
			final SharedLibraryCrudService shared_library_service,		
			final Optional<AuthorizationBean> auth, final Optional<ProjectBean> project) {
		_underlying_management_db = underlying_management_db;
		_data_bucket_service = data_bucket_service;
		_data_bucket_status_service = data_bucket_status_service;
		_shared_library_service = shared_library_service;
		
		_auth = auth;
		_project = project;		
	}
	
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getFilteredDb(java.lang.String, java.util.Optional, java.util.Optional)
	 */
	public IManagementDbService getFilteredDb(final Optional<AuthorizationBean> client_auth, final Optional<ProjectBean> project_auth)
	{
		return new CoreManagementDbService(_underlying_management_db, 
				_data_bucket_service, _data_bucket_status_service, _shared_library_service,
				client_auth, project_auth);
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getSharedLibraryStore()
	 */
	public IManagementCrudService<SharedLibraryBean> getSharedLibraryStore() {
		ManagementDbActorContext.get().getDistributedServices().waitForAkkaJoin(Optional.empty());
		return _shared_library_service;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getPerLibraryState(java.lang.Class, com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean, java.util.Optional)
	 */
	public <T> ICrudService<T> getPerLibraryState(Class<T> clazz,
			SharedLibraryBean library, Optional<String> sub_collection) {
		ManagementDbActorContext.get().getDistributedServices().waitForAkkaJoin(Optional.empty());
		// TODO (ALEPH-19) add this
		throw new RuntimeException("Method not yet supported");
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getDataBucketStore()
	 */
	public IManagementCrudService<DataBucketBean> getDataBucketStore() {
		ManagementDbActorContext.get().getDistributedServices().waitForAkkaJoin(Optional.empty());
		return _data_bucket_service;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getDataBucketStatusStore()
	 */
	public IManagementCrudService<DataBucketStatusBean> getDataBucketStatusStore() {
		ManagementDbActorContext.get().getDistributedServices().waitForAkkaJoin(Optional.empty());
		return _data_bucket_status_service;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getPerBucketState(java.lang.Class, com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Optional)
	 */
	public <T> ICrudService<T> getPerBucketState(Class<T> clazz,
			DataBucketBean bucket, Optional<String> sub_collection) {
		ManagementDbActorContext.get().getDistributedServices().waitForAkkaJoin(Optional.empty());
		// TODO (ALEPH-19) add this
		throw new RuntimeException("Method not yet supported");
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getAnalyticThreadStore()
	 */
	public IManagementCrudService<AnalyticThreadBean> getAnalyticThreadStore() {
		ManagementDbActorContext.get().getDistributedServices().waitForAkkaJoin(Optional.empty());
		// TODO (ALEPH-19) add this
		throw new RuntimeException("Method not yet supported");
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getPerAnalyticThreadState(java.lang.Class, com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadBean, java.util.Optional)
	 */
	public <T> ICrudService<T> getPerAnalyticThreadState(Class<T> clazz,
			AnalyticThreadBean analytic_thread, Optional<String> sub_collection) {
		ManagementDbActorContext.get().getDistributedServices().waitForAkkaJoin(Optional.empty());
		// TODO (ALEPH-19) add this
		throw new RuntimeException("Method not yet supported");
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getUnderlyingPlatformDriver(java.lang.Class, java.util.Optional)
	 */
	public <T> Optional<T> getUnderlyingPlatformDriver(Class<T> driver_class,
			Optional<String> driver_options) {
		throw new RuntimeException("No underlying drivers for CoreManagementDbService - did you want to get the underlying IManagementDbService? Use IServiceContext.getService(IManagementDbService.class, ...) if so.");
	}

	/** This service needs to load some additional classes via Guice. Here's the module that defines the bindings
	 * @return
	 */
	public static List<Module> getExtraDependencyModules() {
		return Arrays.asList((Module)new CoreManagementDbModule());
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IExtraDependencyLoader#youNeedToImplementTheStaticFunctionCalled_getExtraDependencyModules()
	 */
	@Override
	public void youNeedToImplementTheStaticFunctionCalled_getExtraDependencyModules() {
		// (done see above)		
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getRetryStore(java.lang.Class)
	 */
	@Override
	public <T> ICrudService<T> getRetryStore(
			Class<T> retry_message_clazz) {
		ManagementDbActorContext.get().getDistributedServices().waitForAkkaJoin(Optional.empty());
		return _underlying_management_db.getRetryStore(retry_message_clazz);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getUnderlyingArtefacts()
	 */
	@Override
	public Collection<Object> getUnderlyingArtefacts() {
		final LinkedList<Object> ll = new LinkedList<Object>();
		ll.add(this);
		ll.addAll(_underlying_management_db.getUnderlyingArtefacts());
		return ll;
	}
}
