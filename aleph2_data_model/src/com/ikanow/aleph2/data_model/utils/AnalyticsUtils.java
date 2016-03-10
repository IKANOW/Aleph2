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
 *******************************************************************************/
package com.ikanow.aleph2.data_model.utils;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.util.Optional;
import java.util.function.Function;

import scala.Tuple2;

import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsAccessContext;

/** Some utilities to help the analytics code
 * @author Alex
 */
public class AnalyticsUtils {

	/** Retrieves the parameter type of the passed-in interface (eg InputFormat for "interface HadoopInput extends IAnalyticsContext<InputFormat>")
	 * @param clazz - the sub-interface of IAnalyticsAccessContext
	 * @return - the parameterization
	 */
	public static <X extends IAnalyticsAccessContext<?>> Class<?> getTypeName(final Class<X> clazz) {
		return (Class<?>)((ParameterizedType)clazz.getGenericInterfaces()[0]).getActualTypeArguments()[0];
	}
	
	/** Given a sub-interface of unknown type (Eg since ES knows nothing about our internal Hadoop implementation), injects a concrete class that also
	 *  extends IAnalyticsAccessContext (and can share the same parameter type, ie switching on getTypeName)
	 * @param clazz - the sub-interface of IAnalyticsAccessContext
	 * @param implementation - a concrete class that is the same interface with the same parameterization
	 * @return the concrete class with the designated sub-interface
	 */
	@SuppressWarnings("unchecked")
	public static <X extends IAnalyticsAccessContext<?>> X injectImplementation(final Class<X> clazz, final IAnalyticsAccessContext<?> implementation) {
		return (X) Proxy.newProxyInstance(clazz.getClassLoader(),				
				new Class[] { clazz }, 
				new InvocationHandler() {			
					@Override
					public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
						final Method m = implementation.getClass().getMethod(method.getName(), method.getParameterTypes());
						m.setAccessible(true);
						return m.invoke(implementation, args);
					}				
		});
	}
	
	/** Utility function to retrieve the types of a function
	 *  The idea for this is that users will be able to injection functions into AnalyticsContexts, but it requires more changes
	 *  to injectionImplementation because you can't cast from a lambda to a extension of a functional interface
	 * @param function_clazz
	 * @return
	 */
	public static <F extends Function<?, ?>> Optional<Tuple2<Class<?>, Class<?>>> getFunctionTypes(final Class<F> function_clazz) {
	    final Type super_type = function_clazz.getGenericInterfaces()[0];

	    if (super_type instanceof ParameterizedType) {
	        ParameterizedType parameterized_type = (ParameterizedType) super_type;
	        return Optional.of(
	        		Tuples._2T(
	        				(Class<?>) parameterized_type.getActualTypeArguments()[0],
	        				(Class<?>) parameterized_type.getActualTypeArguments()[1]
	        				));	        				
	    }
	    else return Optional.empty();
	}
}
