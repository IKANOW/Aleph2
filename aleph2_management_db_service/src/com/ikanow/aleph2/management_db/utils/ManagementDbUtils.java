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
package com.ikanow.aleph2.management_db.utils;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.CompletableFuture;

import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.utils.FutureUtils;

public class ManagementDbUtils {

	/** Converts a normal CRUD service to a trivial management CRUD service (side channel always empty)
	 * @param delegate
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T> IManagementCrudService<T> wrap(ICrudService<T> delegate) {
		
		//public class MyInvocationHandler implements InvocationHandler {
		InvocationHandler handler = new InvocationHandler() {
			@Override
			public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
				
				Method m = delegate.getClass().getMethod(method.getName(), method.getParameterTypes());
				Object o = m.invoke(delegate, args);
				
				if (o instanceof CompletableFuture) {
					return FutureUtils.createManagementFuture((CompletableFuture<T>) o);
				}
				else if (o instanceof ICrudService) {
					return wrap((ICrudService<?>)o);
				}
				else { // (for get underlying driver)
					return o;
				}
			}
		};

		return (IManagementCrudService<T>)Proxy.newProxyInstance(IManagementCrudService.class.getClassLoader(),
																new Class[] { IManagementCrudService.class }, handler);
	}
}
