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
package com.ikanow.aleph2.data_model.utils;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.CompletableFuture;

import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.IReadOnlyCrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService.IReadOnlyManagementCrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.security.SecuredCrudManagementDbService;

public class ManagementDbUtils {

	//(NOTE THE TEST CODE FOR THIS RESIDES IN aleph2_management_db_service_mongodb, since it was easier to test using a real DB)
	
	/** Converts a normal CRUD service to a trivial management CRUD service (side channel always empty)
	 * @param delegate
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T> IManagementCrudService<T> wrap(ICrudService<T> delegate) {
		
		InvocationHandler handler = new InvocationHandler() {
			@Override
			public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
				if (method.getName().equals("secured")) { // (call the parent IManagementCrudService version of secured - this is a slight hack to get up and running)
					return new SecuredCrudManagementDbService<T>((IServiceContext)args[0], (IManagementCrudService<T>)proxy, (AuthorizationBean)args[1]);
				}
				else return lowLevelWrapper(delegate, proxy, method, args);
			}
		};

		return (IManagementCrudService<T>)Proxy.newProxyInstance(IManagementCrudService.class.getClassLoader(),
																new Class[] { IManagementCrudService.class }, handler);
	}
		
	/** Converts a normal CRUD service to a trivial management CRUD service (side channel always empty) (read only version)
	 * @param delegate
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T> IReadOnlyManagementCrudService<T> wrap(IReadOnlyCrudService<T> delegate) {
		
		InvocationHandler handler = new InvocationHandler() {
			@Override
			public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
				return lowLevelWrapper(delegate, proxy, method, args);
			}
		};

		return (IReadOnlyManagementCrudService<T>)Proxy.newProxyInstance(IReadOnlyManagementCrudService.class.getClassLoader(),
																new Class[] { IReadOnlyManagementCrudService.class }, handler);
	}
	
	/** Utility function for wrap
	 * @param delegate
	 * @param proxy
	 * @param method
	 * @param args
	 * @return
	 * @throws Throwable
	 */
	@SuppressWarnings("unchecked")
	private static <T> Object lowLevelWrapper(ICrudService<T> delegate, Object proxy, Method method, Object[] args) throws Throwable {
		
		Method m = delegate.getClass().getMethod(method.getName(), method.getParameterTypes());
		try {
			Object o = m.invoke(delegate, args);
		
			if (o instanceof CompletableFuture) {
				return FutureUtils.createManagementFuture((CompletableFuture<T>) o);
			}
			else if (o instanceof IReadOnlyCrudService) { // don't need to wrap 
				return wrap((IReadOnlyCrudService<?>)o);
			}
			else if (o instanceof ICrudService) {
				return wrap((ICrudService<?>)o);
			}
			else { // (for get underlying driver)
				return o;
			}
		}
		catch (InvocationTargetException e) {
			throw e.getCause();
		}
	}
}
