package com.ikanow.aleph2.management_db.services;

import java.util.Optional;

import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.ProjectBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;

public class SecuredCoreManagementDbService extends CoreManagementDbService {

	protected AuthorizationBean authorizationBean = null;

	public SecuredCoreManagementDbService(IServiceContext service_context, DataBucketCrudService data_bucket_service,
			DataBucketStatusCrudService data_bucket_status_service, SharedLibraryCrudService shared_library_service,
			ManagementDbActorContext actor_context) {
		super(service_context, data_bucket_service, data_bucket_status_service, shared_library_service, actor_context);		
	}

	public SecuredCoreManagementDbService(IServiceContext service_context, IManagementDbService _underlying_management_db, DataBucketCrudService data_bucket_service,
			DataBucketStatusCrudService data_bucket_status_service, SharedLibraryCrudService shared_library_service,
			ManagementDbActorContext actor_context,AuthorizationBean authorizationBean) {
		super(service_context, data_bucket_service, data_bucket_status_service, shared_library_service, actor_context);
		this.authorizationBean  = authorizationBean;
	}

	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getSharedLibraryStore()
	 */
	@Override
	public IManagementCrudService<SharedLibraryBean> getSharedLibraryStore() {
		return super.getSharedLibraryStore();
	}


}
