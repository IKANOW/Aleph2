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
package com.ikanow.aleph2.security.service;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IBasicSearchService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISubject;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.ProjectBean;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent;
import com.ikanow.aleph2.data_model.utils.FutureUtils;
import com.ikanow.aleph2.data_model.utils.FutureUtils.ManagementFuture;


public class SecuredCrudManagementDbService<T> implements IManagementCrudService<T> {
	private static final Logger logger = LogManager.getLogger(SecuredCrudManagementDbService.class);

	final IManagementCrudService<T> _delegate;
	protected AuthorizationBean authBean = null;
	protected IServiceContext serviceContext = null;
	protected ISecurityService securityService;

	public static String ROLE_ADMIN="admin";
	
	private ISubject subject; // system user's subject
	
	protected PermissionExtractor permissionExtractor = new PermissionExtractor(); // default permission extractor;
	//BiConsumer<? super Optional<T>, ? super Throwable> action = new
	protected BiConsumer<? super Optional<T>, ? super Throwable> readCheckOne = (o, t) -> {
			      logger.debug("readCheckOne:"+o+",t="+t);
			      //System.out.println(t);
				if(o.isPresent()){
					checkReadPermissions(o.get(),true);
				}			     
			    };
	

     protected BiConsumer<? super com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.Cursor<T>, ? super Throwable> readCheckMany = (c, t) -> {
	      logger.debug("readCheckMany:"+c+",t="+t);
	      //System.out.println(t);
			//checkReadPermissions(c);
	      
	    };

		Function<? super com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.Cursor<T>,? extends com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.Cursor<T>> convertCursor = (c)->{
			return new SecuredCursor(c);
		};
		
	    public PermissionExtractor getPermissionExtractor() {
		return permissionExtractor;
	}


	public void setPermissionExtractor(PermissionExtractor permissionExtractor) {
		this.permissionExtractor = permissionExtractor;
	}


	public SecuredCrudManagementDbService(IServiceContext serviceContext, IManagementCrudService<T> delegate, AuthorizationBean authBean) {
		_delegate = delegate;
		this.serviceContext  = serviceContext;
		this.authBean = authBean;
		this.securityService = serviceContext.getSecurityService();
		login();
	}


	/**
	 * @param authorization_fieldname
	 * @param client_auth
	 * @param project_auth
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#getFilteredRepo(java.lang.String,
	 *      java.util.Optional, java.util.Optional)
	 */
	public IManagementCrudService<T> getFilteredRepo(String authorization_fieldname, Optional<AuthorizationBean> client_auth,
			Optional<ProjectBean> project_auth) {
		return _delegate.getFilteredRepo(authorization_fieldname, client_auth, project_auth).readOnlyVersion();
	}

	/**
	 * @param new_object
	 * @param replace_if_present
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#storeObject(java.lang.Object,
	 *      boolean)
	 */
	public ManagementFuture<Supplier<Object>> storeObject(T new_object, boolean replace_if_present) {
		checkWritePermissions(new_object);
		return _delegate.storeObject(new_object, replace_if_present);
	}


	/**
	 * @param new_object
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#storeObject(java.lang.Object)
	 */
	public ManagementFuture<Supplier<Object>> storeObject(T new_object) {
		checkWritePermissions(new_object);
		return _delegate.storeObject(new_object);
	}

	/**
	 * @param new_objects
	 * @param continue_on_error
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#storeObjects(java.util.List,
	 *      boolean)
	 */
	public ManagementFuture<Tuple2<Supplier<List<Object>>, Supplier<Long>>> storeObjects(List<T> new_objects, boolean continue_on_error) {
		checkWritePermissions(new_objects);
		return _delegate.storeObjects(new_objects, continue_on_error);
	}

	/**
	 * @param new_objects
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#storeObjects(java.util.List)
	 */
	public ManagementFuture<Tuple2<Supplier<List<Object>>, Supplier<Long>>> storeObjects(
			List<T> new_objects) {
		checkWritePermissions(new_objects);
		return _delegate.storeObjects(new_objects);
	}

	/**
	 * @param ordered_field_list
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#optimizeQuery(java.util.List)
	 */
	public ManagementFuture<Boolean> optimizeQuery(List<String> ordered_field_list) {
		return _delegate.optimizeQuery(ordered_field_list);
	}

	/**
	 * @param unique_spec
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#getObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	public ManagementFuture<Optional<T>> getObjectBySpec(QueryComponent<T> unique_spec) {
		ManagementFuture<Optional<T>> mf = _delegate.getObjectBySpec(unique_spec); 
		return FutureUtils.createManagementFuture(mf.whenComplete(readCheckOne));
	}

	/**
	 * @param ordered_field_list
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deregisterOptimizedQuery(java.util.List)
	 */
	public boolean deregisterOptimizedQuery(List<String> ordered_field_list) {
		return _delegate.deregisterOptimizedQuery(ordered_field_list);	}

	/**
	 * @param unique_spec
	 * @param field_list
	 * @param include
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#getObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent,
	 *      java.util.List, boolean)
	 */
	public ManagementFuture<Optional<T>> getObjectBySpec(QueryComponent<T> unique_spec, List<String> field_list, boolean include) {
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
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#getObjectById(java.lang.Object,
	 *      java.util.List, boolean)
	 */
	public ManagementFuture<Optional<T>> getObjectById(Object id, List<String> field_list, boolean include) {
		return _delegate.getObjectById(id, field_list, include);
	}

	/**
	 * @param spec
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#getObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	public ManagementFuture<com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.Cursor<T>> getObjectsBySpec(QueryComponent<T> spec) {
		ManagementFuture<com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.Cursor<T>> mf = _delegate.getObjectsBySpec(spec);		
		return FutureUtils.createManagementFuture(mf.thenApply(convertCursor));
	}

	/**
	 * @param spec
	 * @param field_list
	 * @param include
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#getObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent,
	 *      java.util.List, boolean)
	 */
	public ManagementFuture<com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.Cursor<T>> getObjectsBySpec(
			QueryComponent<T> spec, List<String> field_list, boolean include) {
		ManagementFuture<com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.Cursor<T>> mf = _delegate.getObjectsBySpec(spec, field_list, include);		
		return FutureUtils.createManagementFuture(mf.thenApply(convertCursor));
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
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#updateObjectById(java.lang.Object,
	 *      com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent)
	 */
	public ManagementFuture<Boolean> updateObjectById(Object id, UpdateComponent<T> update) {
		return _delegate.updateObjectById(id, update);
	}

	/**
	 * @param unique_spec
	 * @param upsert
	 * @param update
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#updateObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent,
	 *      java.util.Optional,
	 *      com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent)
	 */
	public ManagementFuture<Boolean> updateObjectBySpec(QueryComponent<T> unique_spec, Optional<Boolean> upsert, UpdateComponent<T> update) {
		return _delegate.updateObjectBySpec(unique_spec, upsert,  update);
	}

	/**
	 * @param spec
	 * @param upsert
	 * @param update
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#updateObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent,
	 *      java.util.Optional,
	 *      com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent)
	 */
	public ManagementFuture<Long> updateObjectsBySpec(QueryComponent<T> spec, Optional<Boolean> upsert, UpdateComponent<T> update) {
		return _delegate.updateObjectsBySpec(spec,  upsert, update);
	}

	/**
	 * @param unique_spec
	 * @param upsert
	 * @param update
	 * @param before_updated
	 * @param field_list
	 * @param include
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#updateAndReturnObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent,
	 *      java.util.Optional,
	 *      com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent,
	 *      java.util.Optional, java.util.List, boolean)
	 */
	public ManagementFuture<Optional<T>> updateAndReturnObjectBySpec(QueryComponent<T> unique_spec, Optional<Boolean> upsert,
			UpdateComponent<T> update, Optional<Boolean> before_updated, List<String> field_list, boolean include) {
		return _delegate.updateAndReturnObjectBySpec(unique_spec,  upsert, update, before_updated, field_list, include);
	}

	/**
	 * @param id
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#deleteObjectById(java.lang.Object)
	 */
	public ManagementFuture<Boolean> deleteObjectById(Object id) {
		return _delegate.deleteObjectById(id);
	}

	/**
	 * @param unique_spec
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#deleteObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	public ManagementFuture<Boolean> deleteObjectBySpec(QueryComponent<T> unique_spec) {
		return _delegate.deleteObjectBySpec(unique_spec);
	}

	/**
	 * @param spec
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#deleteObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	public ManagementFuture<Long> deleteObjectsBySpec(QueryComponent<T> spec) {
		return _delegate.deleteObjectsBySpec(spec);
	}

	/**
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#deleteDatastore()
	 */
	public ManagementFuture<Boolean> deleteDatastore() {
		checkDeletePermission();
		return _delegate.deleteDatastore();
	}

	/**
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService#getRawService()
	 */
	public IManagementCrudService<JsonNode> getRawService() {
		return _delegate.getRawService();
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
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getUnderlyingPlatformDriver(java.lang.Class,
	 *      java.util.Optional)
	 */
	public <X> Optional<X> getUnderlyingPlatformDriver(Class<X> driver_class, Optional<String> driver_options) {
		return _delegate.getUnderlyingPlatformDriver(driver_class, driver_options);
	}

	@Override
	public Optional<ICrudService<T>> getCrudService() {
		return Optional.of(this);
	}
	
	protected void checkWritePermissions(T new_object) {
		String permission = permissionExtractor.extractPermissionIdentifier(new_object);
		
		boolean permitted = securityService.hasRole(subject,ROLE_ADMIN); 
//		boolean permitted = securityService.isPermitted(subject,permission); 
		if(!permitted){
			String msg = "Subject "+subject.getSubject()+" has no write permissions ("+permission+")for "+new_object.getClass();
			logger.error(msg);
			throw new SecurityException(msg);					
		}
		
	}
	protected void checkDeletePermission() {
		
		boolean permitted = securityService.hasRole(subject,ROLE_ADMIN); 
		if(!permitted){
			String msg = "Subject "+subject.getSubject()+" has no write permissions for deletions";
			logger.error(msg);
			throw new SecurityException(msg);					
		}
		
	}

	protected void checkWritePermissions(List<T> new_objects) {
		for (T t : new_objects) {
			checkWritePermissions(t);
		}		
	}

	/**
	 * Read permissions are the default permissions. 
	 * @param new_object
	 */
	protected boolean checkReadPermissions(Object new_object, boolean throwOrReturn) {
		String permission = permissionExtractor.extractPermissionIdentifier(new_object);
		boolean permitted = false;
		if(permission!=null){
			permitted = securityService.isPermitted(subject,permission); 
			if(!permitted && throwOrReturn){
				String msg = "Subject "+subject.getSubject()+" has no read permissions ("+permission+")for "+new_object.getClass();
				logger.error(msg);
				throw new SecurityException(msg);					
			}
		}
		return permitted;
	}

	/**
	 * Login in as admin role,e.g. tomcat user
	 * @return
	 */
	protected ISubject login() {
		this.subject = securityService.loginAsSystem();			
		securityService.releaseRunAs(subject);		
		securityService.runAs(subject, Arrays.asList(authBean.getPrincipalName()));
		return subject;
	}

	
	/**
	 * This class is wrapping the crud cursor and additionally performs security checks.
	 * @author jfreydank
	 *
	 */
	protected class SecuredCursor extends com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.Cursor<T>{
		
		private com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.Cursor<T> delegate;

		public SecuredCursor(com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.Cursor<T> delegate){
			this.delegate = delegate;
		}
		
		@Override
		public Iterator<T> iterator() {
			return new SecureIterator(delegate.iterator());
		}

		@Override
		public void close() throws Exception {
			delegate.close();
			
		}

		@Override
		public long count() {
			// TODO
			return delegate.count();
		}
		
		protected class SecureIterator implements Iterator<T>{
			
			private Iterator<T> itDelegate;
			private T nextValid = null;
			
			public SecureIterator(Iterator<T> itDelegate){
				this.itDelegate = itDelegate;
			}

			@Override
			public boolean hasNext() {
				while(itDelegate.hasNext()){
					T nextCandidate = itDelegate.next();
					if(checkReadPermissions(nextCandidate,false)){
						// found next one with
						nextValid = nextCandidate;
						break;
					}
					
				} // while
				
				return nextValid!=null;
			}

			@Override
			public T next() {
				T n = nextValid;
				// reset nextValid, cannot be retrieved twice
				nextValid = null;
				return n;							
			}
		}
	} // cursor
}
