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
package com.ikanow.aleph2.core.shared.utils;

import java.net.URL;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.xeustechnologies.jcl.JarClassLoader;
import org.xeustechnologies.jcl.JclObjectFactory;

import scala.Tuple2;

import com.codepoetics.protonpack.StreamUtils;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Tuples;

import fj.data.Validation;

public class ClassloaderUtils {
	protected final static Cache<String, JarClassLoader> _classloader_cache = CacheBuilder.newBuilder().expireAfterAccess(2, TimeUnit.HOURS).build();
	
	/** Clear the entire cache
	 */
	public static synchronized void clearCache() {
		_classloader_cache.invalidateAll();
	}
	
	/** Returns an instance of the requested class from the designated classpath (union of the libs below)
	 * @param primary_lib - optionally, a single library
	 * @param secondary_libs - optionally a set of other libraries
	 * @return an instance of the desired function
	 */
	public static <R, M> Validation<BasicMessageBean, R> getFromCustomClasspath(
													final Class<R> interface_clazz,
													final String implementation_classname,
													final Optional<String> primary_lib, 
													final List<String> secondary_libs,
													final String handler_for_errors,
													final M msg_for_errors
													)
	{
		return getFromCustomClasspath_withClassloader(interface_clazz, implementation_classname, primary_lib, secondary_libs, handler_for_errors, msg_for_errors)
				.map(t2 -> t2._1());
	}
	
	/** Returns an instance of the requested class from the designated classpath (union of the libs below)
	 * @param primary_lib - optionally, a single library
	 * @param secondary_libs - optionally a set of other libraries
	 * @return an instance of the desired function
	 */
	public synchronized static <R, M> Validation<BasicMessageBean, Tuple2<R, ClassLoader>> getFromCustomClasspath_withClassloader(
													final Class<R> interface_clazz,
													final String implementation_classname,
													final Optional<String> primary_lib, 
													final List<String> secondary_libs,
													final String handler_for_errors,
													final M msg_for_errors
													)
	{
		try {
			final String cache_signature = getCacheSignature(primary_lib, secondary_libs);
			
			final JarClassLoader jcl = _classloader_cache.get(cache_signature, 
						() -> {
							final JarClassLoader jcl_int = new JarClassLoader();
							primary_lib.ifPresent(Lambdas.wrap_consumer_u(pl -> jcl_int.add(new URL(pl))));
							secondary_libs.forEach(Lambdas.wrap_consumer_u(j -> jcl_int.add(new URL(j)))); 
							return jcl_int;
						});
			
			final JclObjectFactory factory = JclObjectFactory.getInstance();
		
			@SuppressWarnings("unchecked")
			final R ret_val = (R) factory.create(jcl, implementation_classname);
			if (null == ret_val) {
				throw new RuntimeException("Unknown error");
			}
			else if (!interface_clazz.isAssignableFrom(ret_val.getClass())) {
				return Validation.fail(SharedErrorUtils.buildErrorMessage(handler_for_errors, 
						msg_for_errors, 
						ErrorUtils.get(SharedErrorUtils.ERROR_CLASS_NOT_SUPERCLASS, implementation_classname, interface_clazz) 
						));				
			}
			else return Validation.success(Tuples._2T(ret_val, jcl));
		}
		catch (Throwable e) {
			return Validation.fail(SharedErrorUtils.buildErrorMessage(handler_for_errors, 
							msg_for_errors, 
							ErrorUtils.getLongForm(SharedErrorUtils.ERROR_LOADING_CLASS, e, implementation_classname) 
							));
		}
	}	
	
	/** Returns a string that is used to access the object cache
	 * @param primary_lib
	 * @param secondary_libs
	 * @return
	 */
	protected static String getCacheSignature(													
			final Optional<String> primary_lib, 
			final List<String> secondary_libs)
	{
		return Stream.concat(
				StreamUtils.stream(primary_lib),
				secondary_libs.stream()
				)
				.sorted()
				.collect(Collectors.joining(":"));		
	}
}
