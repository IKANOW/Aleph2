package com.ikanow.aleph2.data_model.security;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IBasicSearchService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.ProjectBean;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent;
import com.ikanow.aleph2.data_model.utils.FutureUtils.ManagementFuture;


public class SecuredManagementDbService<T> implements IManagementCrudService<T> {
	final IManagementCrudService<T> _delegate;
	private AuthorizationBean authBean = null;

	public SecuredManagementDbService(IManagementCrudService<T> delegate, AuthorizationBean authBean) {
		_delegate = delegate;
		this.authBean = authBean;
	}
	
	/**
	 * @param authorization_fieldname
	 * @param client_auth
	 * @param project_auth
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#getFilteredRepo(java.lang.String, java.util.Optional, java.util.Optional)
	 */
	public IManagementCrudService<T> getFilteredRepo(
			String authorization_fieldname,
			Optional<AuthorizationBean> client_auth,
			Optional<ProjectBean> project_auth) {
		return _delegate.getFilteredRepo(authorization_fieldname,
				client_auth, project_auth).readOnlyVersion();
	}
	
	/**
	 * @param new_object
	 * @param replace_if_present
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#storeObject(java.lang.Object, boolean)
	 */
	public ManagementFuture<Supplier<Object>> storeObject(T new_object,
			boolean replace_if_present) {
		throw new RuntimeException(ErrorUtils.READ_ONLY_CRUD_SERVICE);
	}
	/**
	 * @param new_object
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#storeObject(java.lang.Object)
	 */
	public ManagementFuture<Supplier<Object>> storeObject(T new_object) {
		throw new RuntimeException(ErrorUtils.READ_ONLY_CRUD_SERVICE);
	}
	/**
	 * @param new_objects
	 * @param continue_on_error
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#storeObjects(java.util.List, boolean)
	 */
	public ManagementFuture<Tuple2<Supplier<List<Object>>, Supplier<Long>>> storeObjects(
			List<T> new_objects, boolean continue_on_error) {
		throw new RuntimeException(ErrorUtils.READ_ONLY_CRUD_SERVICE);
	}
	/**
	 * @param new_objects
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#storeObjects(java.util.List)
	 */
	public ManagementFuture<Tuple2<Supplier<List<Object>>, Supplier<Long>>> storeObjects(
			List<T> new_objects) {
		throw new RuntimeException(ErrorUtils.READ_ONLY_CRUD_SERVICE);
	}
	/**
	 * @param ordered_field_list
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#optimizeQuery(java.util.List)
	 */
	public ManagementFuture<Boolean> optimizeQuery(
			List<String> ordered_field_list) {
		throw new RuntimeException(ErrorUtils.READ_ONLY_CRUD_SERVICE);
	}
	/**
	 * @param unique_spec
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#getObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	public ManagementFuture<Optional<T>> getObjectBySpec(
			QueryComponent<T> unique_spec) {
		throw new RuntimeException(ErrorUtils.READ_ONLY_CRUD_SERVICE);
	}
	/**
	 * @param ordered_field_list
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deregisterOptimizedQuery(java.util.List)
	 */
	public boolean deregisterOptimizedQuery(List<String> ordered_field_list) {
		throw new RuntimeException(ErrorUtils.READ_ONLY_CRUD_SERVICE);
	}
	/**
	 * @param unique_spec
	 * @param field_list
	 * @param include
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#getObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.List, boolean)
	 */
	public ManagementFuture<Optional<T>> getObjectBySpec(
			QueryComponent<T> unique_spec, List<String> field_list,
			boolean include) {
		return _delegate.getObjectBySpec(unique_spec, field_list, include);
	}
	/**
	 * @param id
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#getObjectById(java.lang.Object)
	 */
	public ManagementFuture<Optional<T>> getObjectById(Object id) {
		return _delegate.getObjectById(id);
	}
	/**
	 * @param id
	 * @param field_list
	 * @param include
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#getObjectById(java.lang.Object, java.util.List, boolean)
	 */
	public ManagementFuture<Optional<T>> getObjectById(Object id,
			List<String> field_list, boolean include) {
		return _delegate.getObjectById(id, field_list, include);
	}
	/**
	 * @param spec
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#getObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	public ManagementFuture<com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.Cursor<T>> getObjectsBySpec(
			QueryComponent<T> spec) {
		return _delegate.getObjectsBySpec(spec);
	}
	/**
	 * @param spec
	 * @param field_list
	 * @param include
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#getObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.List, boolean)
	 */
	public ManagementFuture<com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.Cursor<T>> getObjectsBySpec(
			QueryComponent<T> spec, List<String> field_list, boolean include) {
		return _delegate.getObjectsBySpec(spec, field_list, include);
	}
	/**
	 * @param spec
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#countObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	public ManagementFuture<Long> countObjectsBySpec(QueryComponent<T> spec) {
		return _delegate.countObjectsBySpec(spec);
	}
	/**
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#countObjects()
	 */
	public ManagementFuture<Long> countObjects() {
		return _delegate.countObjects();
	}
	/**
	 * @param id
	 * @param update
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#updateObjectById(java.lang.Object, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent)
	 */
	public ManagementFuture<Boolean> updateObjectById(Object id,
			UpdateComponent<T> update) {
		throw new RuntimeException(ErrorUtils.READ_ONLY_CRUD_SERVICE);
	}
	/**
	 * @param unique_spec
	 * @param upsert
	 * @param update
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#updateObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.Optional, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent)
	 */
	public ManagementFuture<Boolean> updateObjectBySpec(
			QueryComponent<T> unique_spec, Optional<Boolean> upsert,
			UpdateComponent<T> update) {
		throw new RuntimeException(ErrorUtils.READ_ONLY_CRUD_SERVICE);
	}
	/**
	 * @param spec
	 * @param upsert
	 * @param update
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#updateObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.Optional, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent)
	 */
	public ManagementFuture<Long> updateObjectsBySpec(
			QueryComponent<T> spec, Optional<Boolean> upsert,
			UpdateComponent<T> update) {
		throw new RuntimeException(ErrorUtils.READ_ONLY_CRUD_SERVICE);
	}
	/**
	 * @param unique_spec
	 * @param upsert
	 * @param update
	 * @param before_updated
	 * @param field_list
	 * @param include
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#updateAndReturnObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.Optional, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent, java.util.Optional, java.util.List, boolean)
	 */
	public ManagementFuture<Optional<T>> updateAndReturnObjectBySpec(
			QueryComponent<T> unique_spec, Optional<Boolean> upsert,
			UpdateComponent<T> update, Optional<Boolean> before_updated,
			List<String> field_list, boolean include) {
		throw new RuntimeException(ErrorUtils.READ_ONLY_CRUD_SERVICE);
	}
	/**
	 * @param id
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#deleteObjectById(java.lang.Object)
	 */
	public ManagementFuture<Boolean> deleteObjectById(Object id) {
		throw new RuntimeException(ErrorUtils.READ_ONLY_CRUD_SERVICE);
	}
	/**
	 * @param unique_spec
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#deleteObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	public ManagementFuture<Boolean> deleteObjectBySpec(
			QueryComponent<T> unique_spec) {
		throw new RuntimeException(ErrorUtils.READ_ONLY_CRUD_SERVICE);
	}
	/**
	 * @param spec
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#deleteObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	public ManagementFuture<Long> deleteObjectsBySpec(QueryComponent<T> spec) {
		throw new RuntimeException(ErrorUtils.READ_ONLY_CRUD_SERVICE);
	}
	/**
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#deleteDatastore()
	 */
	public ManagementFuture<Boolean> deleteDatastore() {
		throw new RuntimeException(ErrorUtils.READ_ONLY_CRUD_SERVICE);
	}
	/**
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#getRawService()
	 */
	public IManagementCrudService<JsonNode> getRawService() {
		return _delegate.getRawService().readOnlyVersion();
	}
	/**
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getSearchService()
	 */
	public Optional<IBasicSearchService<T>> getSearchService() {
		return _delegate.getSearchService();
	}
	/**
	 * @param driver_class
	 * @param driver_options
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getUnderlyingPlatformDriver(java.lang.Class, java.util.Optional)
	 */
	public <X> Optional<X> getUnderlyingPlatformDriver(
			Class<X> driver_class, Optional<String> driver_options) {
		return _delegate.getUnderlyingPlatformDriver(driver_class,
				driver_options);
	}
	@Override
	public Optional<ICrudService<T>> getCrudService() {
		return Optional.of(this);
	}
}
