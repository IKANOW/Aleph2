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
package com.ikanow.aleph2.data_model.utils;

import java.lang.reflect.Method;
import java.util.function.Function;

import org.checkerframework.checker.nullness.qual.NonNull;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

/**
 * A set of utilities for access beans/pojos
 * @author acp
 *
 */
public class ObjectUtils {
	
	/**
	 * Enables type-safe access to a single classes
	 * @param clazz - the containing class for the fields
	 * @return a MethodNamingHelper for this class
	 */
	public static <T> MethodNamingHelper<T> from(Class<T> clazz) {
		return new MethodNamingHelper<T>(clazz);
	}
	
	/**
	 * Enables type-safe access to a single classes
	 * @param a - any non-null instance of the class
	 * @return a MethodNamingHelper for this class
	 */
	@SuppressWarnings("unchecked")
	public static <T> MethodNamingHelper<T> from(@NonNull T a) {
		return new MethodNamingHelper<T>((Class<T>) a.getClass());
	}
	
	/**
	 * A helper class that enables type safe field specification
	 * Note: depends on all accessors being in the format "_<fieldname>()" for the given <fieldname>  
	 * @author acp
	 *
	 * @param <T>
	 */
	public static class MethodNamingHelper<T> implements MethodInterceptor {
		
		protected String _name;
		protected T _recorder;
		@SuppressWarnings("unchecked")
		protected MethodNamingHelper(Class<T> clazz) {
			Enhancer enhancer = new Enhancer();
			enhancer.setSuperclass(clazz);
			enhancer.setCallback(this);
			_recorder = (T) enhancer.create();
		}
		@Override
		public Object intercept(Object object, Method method, Object[] args,
				MethodProxy proxy) throws Throwable
		{
			if (method.getName().equals("field")) {
				return _name;
			}
			else {
				_name = method.getName().substring(1);
			}
			return null;
		}
		/**
		 * @param getter - the method reference (T::<function>)
		 * @return
		 */
		public String field(Function<T, ?> getter) {
			getter.apply(_recorder);
			return _name;
		}
	}
}