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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.Function;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigRenderOptions;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

/**
 * A set of utilities for access beans/pojos
 * @author acp
 *
 */
public class BeanTemplateUtils {

	/** Creates a property bean from the supplied config object
	 * @param bean_root - the root of the configuration tree that needs to be converted to bean
	 * @param bean_clazz - the class of the properties bean
	 * @return
	 * @throws IOException 
	 * @throws JsonMappingException 
	 * @throws JsonParseException 
	 */
	static public <T> T from(Config bean_root, Class<T> bean_clazz) throws JsonParseException, JsonMappingException, IOException {
		ObjectMapper object_mapper = new ObjectMapper();
		object_mapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);		
		return object_mapper.readValue(bean_root.root().render(ConfigRenderOptions.concise()), bean_clazz);
	}
	
	
	/** Contains a partial bean
	 * @author acp
	 *
	 * @param <T> - the bean type
	 */
	public static class BeanTemplate<T> {
		
		/** Creates a template from this full bean
		 * @param o
		 * @return
		 */
		public static <O> BeanTemplate<O> of(O o) { return new BeanTemplate<O>(o); }
		
		/** Gets an element of the bean 
		 * @param getter
		 * @return
		 */
		public <R> R get(final @NonNull Function<T, R> getter) {
			return getter.apply(_element);
		}
		
		/** If you are really sure you want this, gets the (probably partial) bean in this template
		 * @return the (probably partial) bean in this template
		 */
		public T get() { return _element; }			
				
		// Private implementation
		protected BeanTemplate(T element) { _element = element; }
		protected T _element;
	}
	
	/**
	 * Enables type-safe access to a single classes
	 * @param clazz - the containing class for the fields
	 * @return a MethodNamingHelper for this class
	 */
	@NonNull
	public static <T> MethodNamingHelper<T> from(final @NonNull Class<T> clazz) {
		return new MethodNamingHelper<T>(clazz, Optional.empty());
	}
	
	/**
	 * Enables type-safe access to a single classes
	 * @param a - any non-null instance of the class
	 * @return a MethodNamingHelper for this class
	 */
	@SuppressWarnings("unchecked")
	@NonNull
	public static <T> MethodNamingHelper<T> from(final @NonNull T a) {
		return new MethodNamingHelper<T>((Class<T>) a.getClass(), Optional.empty());
	}
	
	/** Clones the specified object, returning a builder that can be used to replace specified values
	 * @param the object to clone
	 * @return Clone Helper, finish with done() to return the class
	 */
	@NonNull
	public static <T> CloningHelper<T> clone(final @NonNull T a) {
		try {
			return new CloningHelper<T>(a);
		} catch (Exception e) {
			throw new RuntimeException("CloningHelper.clone", e);
		}
	}
	/**Builds an immutable object using the specified value just to get the class (see clone to actually use the input variable)
	 * @param a - the object determining the class to use
	 * @return Clone Helper, finish with done() to return the class
	 */
	@SuppressWarnings("unchecked")
	@NonNull
	public static <T> TemplateHelper<T> build(final @NonNull T a) {
		try {
			return new TemplateHelper<T>((Class<T>) a.getClass());
		} catch (Exception e) {
			throw new RuntimeException("TemplateHelper.build", e);
		}
	}
	/**Builds an immutable object of the specified class
	 * @param a - the class to use
	 * @return Clone Helper, finish with done() to return the class
	 */
	@NonNull
	public static <T> TemplateHelper<T> build(final @NonNull Class<T> clazz) {
		try {
			return new TemplateHelper<T>(clazz);
		} catch (Exception e) {
			throw new RuntimeException("TemplateHelper.build", e);
		}
	}	
	
	/** Intermediate class for building tempaltes
	 * @author acp
	 *
	 * @param <T> - the class being helped
	 */
	public static class TemplateHelper<T> extends CommonHelper<T> {
		
		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.utils.BeanTemplateUtils.CommonHelper#with(java.lang.String, java.lang.Object)
		 */
		@Override
		public <U> @NonNull TemplateHelper<T> with(@NonNull String fieldName,
				@NonNull U val) {
			return (@NonNull TemplateHelper<T>) super.with(fieldName, val);
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.utils.BeanTemplateUtils.CommonHelper#with(java.util.function.Function, java.lang.Object)
		 */
		@Override
		public <U> @NonNull TemplateHelper<T> with(
				@NonNull Function<T, ?> getter, @NonNull U val) {
			return (@NonNull TemplateHelper<T>) super.with(getter, val);
		}

		/** Finishes the cloning process - returning as a template
		 * @return the final version of the element
		 */
		public BeanTemplate<T> done() {
			return new BeanTemplate<T>(_element);
		}
		
		// Implementation
		
		protected TemplateHelper(Class<T> clazz) throws NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
			super(clazz);
		}
		protected TemplateHelper(T element) throws NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
			super(element);
		}
	}
	/** Intermediate class for cloning
	 * @author acp
	 *
	 * @param <T> - the class being helped
	 */
	public static class CloningHelper<T> extends CommonHelper<T> {
		
		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.utils.BeanTemplateUtils.CommonHelper#with(java.lang.String, java.lang.Object)
		 */
		@Override
		public <U> @NonNull CloningHelper<T> with(@NonNull String fieldName,
				@NonNull U val) {
			return (@NonNull CloningHelper<T>) super.with(fieldName, val);
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.utils.BeanTemplateUtils.CommonHelper#with(java.util.function.Function, java.lang.Object)
		 */
		@Override
		public <U> @NonNull CloningHelper<T> with(
				@NonNull Function<T, ?> getter, @NonNull U val) {
			return (@NonNull CloningHelper<T>) super.with(getter, val);
		}

		/** Finishes the cloning process - returning as a template
		 * @return the final version of the element
		 */
		public BeanTemplate<T> asTemplate() {
			return new BeanTemplate<T>(_element);
		}
		
		/** Finishes the building/cloning process
		 * @return the final version of the element
		 */
		@NonNull
		public T done() {
			return _element;
		}
				
		// Implementation
		
		protected CloningHelper(Class<T> clazz) throws NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
			super(clazz);
		}
		protected CloningHelper(T element) throws NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
			super(element);
		}		
	}
	/** Intermediate class for cloning or templating
	 * @author acp
	 *
	 * @param <T> - the class being helped
	 */
	public static class CommonHelper<T> {
		/**Set a field in a cloned/new object
		 * @param fieldName The field to set
		 * @param val the value to which it should be set
		 * @return Clone Helper, finish with done() to return the class
		 */
		@NonNull
		public <U> CommonHelper<T> with(final @NonNull String fieldName, final @NonNull U val) {
			try {
				Field f = _element.getClass().getDeclaredField(fieldName);
				f.setAccessible(true);
				f.set(_element, val);
			}
			catch (Exception e) {
				throw new RuntimeException("CloningHelper", e);
			}
			return this;
		}
		
		/**Set a field in a cloned/new object
		 * @param fieldName The field to set
		 * @param val the value to which it should be set
		 * @return Clone Helper, finish with done() to return the class
		 */
		@NonNull
		public <U> CommonHelper<T> with(final @NonNull Function<T, ?> getter, final @NonNull U val) {
			try {
				if (null == _naming_helper) {
					_naming_helper = from(_element);
				}
				Field f = _element.getClass().getDeclaredField(_naming_helper.field(getter));
				f.setAccessible(true);
				f.set(_element, val);
			}
			catch (Exception e) {
				throw new RuntimeException("CloningHelper", e);
			}
			return this;
		}		
		
		protected void cloneInitialFields(final @NonNull T to_clone) {
			Arrays.stream(_element.getClass().getDeclaredFields())
				.filter(f -> !Modifier.isStatic(f.getModifiers())) // (ignore static fields)
				.map(f -> { try { f.setAccessible(true); return Tuples._2T(f, f.get(to_clone)); } catch (Exception e) { return null; } })
				.filter(t -> (null != t) && (null != t._2()))
				.forEach(t -> { try { t._1().set(_element, t._2()); } catch (Exception e) { } } );
		}
		@SuppressWarnings("unchecked")
		protected CommonHelper(final @NonNull Class<?> element_clazz) throws InstantiationException, IllegalAccessException, NoSuchMethodException, SecurityException, IllegalArgumentException, InvocationTargetException {
			final Constructor<T> contructor = (Constructor<T>) element_clazz.getDeclaredConstructor();
			contructor.setAccessible(true);
			_element = (T) contructor.newInstance();
		}
		@NonNull
		protected CommonHelper(final @NonNull T to_clone) throws NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
			@SuppressWarnings("unchecked")
			final Constructor<T> contructor = (Constructor<T>) to_clone.getClass().getDeclaredConstructor();
			contructor.setAccessible(true);
			_element = (T) contructor.newInstance();
			cloneInitialFields(to_clone);
		}
		protected final T _element;
		protected MethodNamingHelper<T> _naming_helper = null;
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
		protected Optional<String> _parent_path;
		@SuppressWarnings("unchecked")
		protected MethodNamingHelper(final @NonNull Class<T> clazz, final Optional<String> parent_path) {
			Enhancer enhancer = new Enhancer();
			enhancer.setSuperclass(clazz);
			enhancer.setCallback(this);
			_recorder = (T) enhancer.create();
			_parent_path = parent_path;
		}
		@Override
		@Nullable
		public Object intercept(final Object object, final Method method, final Object[] args,
				final MethodProxy proxy) throws Throwable
		{
			if (method.getName().equals("field")) {
				return _name;
			}
			else {
				_name = method.getName();
			}
			return null;
		}
		/**
		 * @param getter - the method reference (T::<function>)
		 * @return
		 */
		@NonNull
		public String field(final @NonNull Function<T, ?> getter) {
			getter.apply(_recorder);
			return _name;
		}
		/** Returns a nested fieldname in an object hierarchy (given a non-null object of nested type)
		 * @param getter - the getter utility defining the fieldname of the nested object 
		 * @param from - an object of the nested type 
		 * @return a MethodNamingHelper for the nested class
		 */
		@SuppressWarnings("unchecked")
		@NonNull
		public <U> MethodNamingHelper<U> nested(final @NonNull Function<T, ?> getter, final @NonNull U from) {
			return (MethodNamingHelper<U>) nested(getter, from.getClass());			
		}
		/** Returns a nested fieldname in an object hierarchy
		 * @param getter - the getter utility defining the fieldname of the nested object 
		 * @param nested_clazz - the class of the nested type
		 * @return a MethodNamingHelper for the nested class
		 */
		@NonNull
		public <U> MethodNamingHelper<U> nested(final @NonNull Function<T, ?> getter, final @NonNull Class<U> nested_clazz) {
			String new_parent_path =  _parent_path.orElse("") + "." + field(getter) + ".";
			return new MethodNamingHelper<U>(nested_clazz, Optional.of(new_parent_path));
		}
	}
}