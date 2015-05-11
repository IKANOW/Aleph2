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

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IBasicSearchService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.ProjectBean;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;

/**
 * @author acp
 *
 */
public class DataBucketCrudService implements ICrudService<DataBucketBean> {

	protected final IManagementDbService _underlying_management_db;
	
	/** Guice invoked constructor
	 * @param underlying_management_db
	 */
	@Inject
	public DataBucketCrudService(
			final @Named("management_db_service.underlying") IManagementDbService underlying_management_db)
	{
		_underlying_management_db = underlying_management_db;

		//DEBUG
		//System.out.println("Hello world from: " + this.getClass() + ": underlying=" + _underlying_management_db);
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getFilteredRepo(java.lang.String, java.util.Optional, java.util.Optional)
	 */
	public ICrudService<DataBucketBean> getFilteredRepo(
			String authorization_fieldname,
			Optional<AuthorizationBean> client_auth,
			Optional<ProjectBean> project_auth) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#storeObject(java.lang.Object, boolean)
	 */
	public Future<Supplier<Object>> storeObject(DataBucketBean new_object,
			boolean replace_if_present) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#storeObject(java.lang.Object)
	 */
	public Future<Supplier<Object>> storeObject(DataBucketBean new_object) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#storeObjects(java.util.List, boolean)
	 */
	public Future<Tuple2<Supplier<List<Object>>, Supplier<Long>>> storeObjects(
			List<DataBucketBean> new_objects, boolean continue_on_error) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#storeObjects(java.util.List)
	 */
	public Future<Tuple2<Supplier<List<Object>>, Supplier<Long>>> storeObjects(
			List<DataBucketBean> new_objects) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#optimizeQuery(java.util.List)
	 */
	public Future<Boolean> optimizeQuery(List<String> ordered_field_list) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deregisterOptimizedQuery(java.util.List)
	 */
	public boolean deregisterOptimizedQuery(List<String> ordered_field_list) {
		// TODO Auto-generated method stub
		return false;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	public Future<Optional<DataBucketBean>> getObjectBySpec(
			QueryComponent<DataBucketBean> unique_spec) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.List, boolean)
	 */
	public Future<Optional<DataBucketBean>> getObjectBySpec(
			QueryComponent<DataBucketBean> unique_spec,
			List<String> field_list, boolean include) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectById(java.lang.Object)
	 */
	public Future<Optional<DataBucketBean>> getObjectById(Object id) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectById(java.lang.Object, java.util.List, boolean)
	 */
	public Future<Optional<DataBucketBean>> getObjectById(Object id,
			List<String> field_list, boolean include) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	public Future<com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.Cursor<DataBucketBean>> getObjectsBySpec(
			QueryComponent<DataBucketBean> spec) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.List, boolean)
	 */
	public Future<com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.Cursor<DataBucketBean>> getObjectsBySpec(
			QueryComponent<DataBucketBean> spec, List<String> field_list,
			boolean include) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#countObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	public Future<Long> countObjectsBySpec(QueryComponent<DataBucketBean> spec) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#countObjects()
	 */
	public Future<Long> countObjects() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#updateObjectById(java.lang.Object, java.util.Optional, java.util.Optional, java.util.Optional)
	 */
	public Future<Boolean> updateObjectById(Object id,
			Optional<DataBucketBean> set,
			Optional<QueryComponent<DataBucketBean>> add,
			Optional<QueryComponent<DataBucketBean>> remove) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#updateObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.Optional, java.util.Optional, java.util.Optional, java.util.Optional)
	 */
	public Future<Boolean> updateObjectBySpec(
			QueryComponent<DataBucketBean> unique_spec,
			Optional<Boolean> upsert, Optional<DataBucketBean> set,
			Optional<QueryComponent<DataBucketBean>> add,
			Optional<QueryComponent<DataBucketBean>> remove) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#updateObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.Optional, java.util.Optional, java.util.Optional, java.util.Optional)
	 */
	public Future<Long> updateObjectsBySpec(
			QueryComponent<DataBucketBean> spec, Optional<Boolean> upsert,
			Optional<DataBucketBean> set,
			Optional<QueryComponent<DataBucketBean>> add,
			Optional<QueryComponent<DataBucketBean>> remove) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#updateAndReturnObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.Optional, java.util.Optional, java.util.Optional, java.util.Optional, java.util.Optional, java.util.List, boolean)
	 */
	public Future<Optional<DataBucketBean>> updateAndReturnObjectBySpec(
			QueryComponent<DataBucketBean> unique_spec,
			Optional<Boolean> upsert, Optional<DataBucketBean> set,
			Optional<QueryComponent<DataBucketBean>> add,
			Optional<QueryComponent<DataBucketBean>> remove,
			Optional<Boolean> before_updated, List<String> field_list,
			boolean include) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deleteObjectById(java.lang.Object)
	 */
	public Future<Boolean> deleteObjectById(Object id) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deleteObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	public Future<Boolean> deleteObjectBySpec(
			QueryComponent<DataBucketBean> unique_spec) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deleteObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	public Future<Long> deleteObjectsBySpec(QueryComponent<DataBucketBean> spec) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deleteDatastore()
	 */
	public Future<Boolean> deleteDatastore() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getRawCrudService()
	 */
	public ICrudService<JsonNode> getRawCrudService() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getSearchService()
	 */
	public Optional<IBasicSearchService<DataBucketBean>> getSearchService() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getUnderlyingPlatformDriver(java.lang.Class, java.util.Optional)
	 */
	public <T> T getUnderlyingPlatformDriver(Class<T> driver_class,
			Optional<String> driver_options) {
		// TODO Auto-generated method stub
		return null;
	}

}
