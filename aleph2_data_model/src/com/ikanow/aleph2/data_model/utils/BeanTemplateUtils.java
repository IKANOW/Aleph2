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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import com.codepoetics.protonpack.StreamUtils;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
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
	static public <T> T from(final Config bean_root, final Class<T> bean_clazz) throws JsonParseException, JsonMappingException, IOException {
		if (null != bean_root) {
			ObjectMapper object_mapper = BeanTemplateUtils.configureMapper(Optional.empty());
			return object_mapper.readValue(bean_root.root().render(ConfigRenderOptions.concise()), bean_clazz);
		}
		else {
			return BeanTemplateUtils.build(bean_clazz).done().get();
		}
	}
	
	/** Converts a bean to its JsonNode representation (not high performance)
	 * @param bean - the bean to convert to JSON
	 * @return - the JSON
	 */
	static public <T> JsonNode toJson(final T bean) {
		ObjectMapper object_mapper = BeanTemplateUtils.configureMapper(Optional.empty());
		return object_mapper.valueToTree(bean);		
	}
	
	/** Converts a bean to its Map<String, Object> representation (not high performance)
	 * @param bean - the bean to convert to JSON
	 * @return - the JSON
	 */
	@SuppressWarnings("unchecked")
	static public <T> Map<String, Object> toMap(final T bean) {
		ObjectMapper object_mapper = BeanTemplateUtils.configureMapper(Optional.empty());
		return object_mapper.convertValue(bean, Map.class);		
	}
	
	/** Converts a JsonNode to a bean template of the specified type
	 * (note: not very high performance, should only be used for management-type operations)
	 * @param map_json - the bean to convert to JSON
	 * @return - the bean template
	 */
	static public <T> BeanTemplate<T> from(final Map<String, Object> map_json, final Class<T> clazz) {
		try {
			ObjectMapper object_mapper = BeanTemplateUtils.configureMapper(Optional.empty());
			return BeanTemplate.of(object_mapper.convertValue(map_json, clazz));
		}
		catch (Exception e) { // on fail returns an unchecked error
			throw new RuntimeException(e); // (this can only happen due to "static" code type issues, so unchecked exception is fine
		}
	}

	/** Converts a JsonNode to a bean template of the specified type
	 * (note: not very high performance, should only be used for management-type operations)
	 * @param bean - the JSON string to convert to a bean 
	 * @return - the bean template
	 */
	static public <T> BeanTemplate<T> from(final String string_json, final Class<T> clazz) {
		try {
			ObjectMapper object_mapper = BeanTemplateUtils.configureMapper(Optional.empty());
			return BeanTemplate.of(object_mapper.readValue(string_json, clazz));
		}
		catch (Exception e) { // on fail returns an unchecked error
			throw new RuntimeException(e); // (this can only happen due to "static" code type issues, so unchecked exception is fine
		}
	}

	/** Converts a JsonNode to a bean template of the specified type
	 * (note: not very high performance, should only be used for management-type operations)
	 * @param bean - the JSON node to convert to a bean 
	 * @return - the bean template
	 */
	static public <T> BeanTemplate<T> from(final JsonNode bean_json, final Class<T> clazz) {
		try {
			ObjectMapper object_mapper = BeanTemplateUtils.configureMapper(Optional.empty());
			return BeanTemplate.of(object_mapper.treeToValue(bean_json, clazz));
		}
		catch (Exception e) { // on fail returns an unchecked error
			throw new RuntimeException(e); // (this can only happen due to "static" code type issues, so unchecked exception is fine
		}
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
		public <R> R get(final Function<T, R> getter) {
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
	
	/** Returns a template builder of the designated type from the JSON (note: not very high performance, should only be used for management-type operations)
	 * @param json
	 * @param bean_clazz
	 * @return
	 * @throws JsonParseException
	 * @throws JsonMappingException
	 * @throws IOException
	 */
	static public <T> TemplateHelper<T> build(final JsonNode json, final Class<T> bean_clazz) throws JsonParseException, JsonMappingException, IOException {
		ObjectMapper object_mapper = BeanTemplateUtils.configureMapper(Optional.empty());
		return build(object_mapper.treeToValue(json, bean_clazz));		
	}	
	
	/** Returns a template builder of the designated type from the JSON (note: not very high performance, should only be used for management-type operations)
	 * @param json_str
	 * @param bean_clazz
	 * @return
	 * @throws JsonParseException
	 * @throws JsonMappingException
	 * @throws IOException
	 */
	static public <T> TemplateHelper<T> build(final String json_str, final Class<T> bean_clazz) throws JsonParseException, JsonMappingException, IOException {
		ObjectMapper object_mapper = BeanTemplateUtils.configureMapper(Optional.empty());
		return build(object_mapper.treeToValue(object_mapper.readTree(json_str.getBytes()), bean_clazz));		
	}	
	
	/**
	 * Enables type-safe access to a single classes
	 * @param clazz - the containing class for the fields
	 * @return a MethodNamingHelper for this class
	 */
	public static <T> MethodNamingHelper<T> from(final Class<T> clazz) {
		return new MethodNamingHelper<T>(clazz, Optional.empty());
	}
	
	/**
	 * Enables type-safe access to a single classes
	 * @param a - any non-null instance of the class
	 * @return a MethodNamingHelper for this class
	 */
	@SuppressWarnings("unchecked")
	public static <T> MethodNamingHelper<T> from(final T a) {
		return new MethodNamingHelper<T>((Class<T>) a.getClass(), Optional.empty());
	}
	
	/** Clones the specified object, returning a builder that can be used to replace specified values
	 * @param the object to clone
	 * @return Clone Helper, finish with done() to return the class
	 */
	public static <T> CloningHelper<T> clone(final T a) {
		try {
			return new CloningHelper<T>(a);
		} catch (Exception e) {
			throw new RuntimeException("CloningHelper.clone", e);
		}
	}
	/**Builds an immutable object using the specified value as a starting point
	 * @param a - the object determining the class to use
	 * @return Clone Helper, finish with done() to return the class
	 */
	public static <T> TemplateHelper<T> build(final T a) {
		try {
			return new TemplateHelper<T>(a);
		} catch (Exception e) {
			throw new RuntimeException("TemplateHelper.build", e);
		}
	}
	/**Builds an immutable object of the specified class
	 * @param a - the class to use
	 * @return Clone Helper, finish with done() to return the class
	 */
	public static <T> TemplateHelper<T> build(final Class<T> clazz) {
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
		public <U> TemplateHelper<T> with(String fieldName,
				U val) {
			return (TemplateHelper<T>) super.with(fieldName, val);
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.utils.BeanTemplateUtils.CommonHelper#with(java.util.function.Function, java.lang.Object)
		 */
		@Override
		public <U> TemplateHelper<T> with(
				Function<T, ?> getter, U val) {
			return (TemplateHelper<T>) super.with(getter, val);
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
		public <U> CloningHelper<T> with(String fieldName,
				U val) {
			return (CloningHelper<T>) super.with(fieldName, val);
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.utils.BeanTemplateUtils.CommonHelper#with(java.util.function.Function, java.lang.Object)
		 */
		@Override
		public <U> CloningHelper<T> with(
				Function<T, ?> getter, U val) {
			return (CloningHelper<T>) super.with(getter, val);
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
		public <U> CommonHelper<T> with(final String fieldName, final U val) {
			try {
				final Field f = getDeclaredAndInheritedField(fieldName, _element.getClass());
				f.setAccessible(true);
				f.set(_element, val);
			}
			catch (Exception e) {
				throw new RuntimeException("CloningHelper", e);
			}
			return this;
		}
		
		/** Gets an element of the bean 
		 * @param getter
		 * @return
		 */
		@SuppressWarnings("unchecked")
		public <R> R get(final String name) {
			try {
				final Field f = getDeclaredAndInheritedField(name, _element.getClass());
				f.setAccessible(true);
				return (R)f.get(_element);
			}
			catch (Exception e) {
				throw new RuntimeException("BeanTemplate.get", e);
			}
		}
		
		private Field getDeclaredAndInheritedField(final String field, Class<?> clazz) throws NoSuchFieldException {
			try {
				return clazz.getDeclaredField(field);
			}
			catch (NoSuchFieldException e) {
				final Class<?> next_clazz = clazz.getSuperclass();
				if (null == next_clazz) { 
					throw e;
				}
				return getDeclaredAndInheritedField(field, next_clazz);
			}
		}
		
		/**Set a field in a cloned/new object
		 * @param fieldName The field to set
		 * @param val the value to which it should be set
		 * @return Clone Helper, finish with done() to return the class
		 */
		public <U> CommonHelper<T> with(final Function<T, ?> getter, final U val) {
			if (null == _naming_helper) {
				_naming_helper = from(_element);
			}
			return with(_naming_helper.field(getter), val);
		}		
		
		protected void cloneInitialFields(final T to_clone) {			
			StreamUtils.<Class<?>>takeWhile(Stream.iterate(_element.getClass(), c -> c.getSuperclass()), c -> null != c)
				.<Field>flatMap(c -> Arrays.stream(c.getDeclaredFields()))
				.filter(f -> !Modifier.isStatic(f.getModifiers())) // (ignore static fields)
				.flatMap(Lambdas.flatWrap_i(f -> { f.setAccessible(true); return Tuples._2T(f, f.get(to_clone)); }))
				.filter(t -> (null != t) && (null != t._2()))
				.forEach(Lambdas.wrap_consumer_i(t -> t._1().set(_element, t._2())));
			;
		}
		
		@SuppressWarnings("unchecked")
		protected CommonHelper(final Class<?> element_clazz) throws InstantiationException, IllegalAccessException, NoSuchMethodException, SecurityException, IllegalArgumentException, InvocationTargetException {
			final Constructor<T> contructor = (Constructor<T>) element_clazz.getDeclaredConstructor();
			contructor.setAccessible(true);
			_element = (T) contructor.newInstance();
		}
		protected CommonHelper(final T to_clone) throws NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
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
		protected MethodNamingHelper(final Class<T> clazz, final Optional<String> parent_path) {
			Enhancer enhancer = new Enhancer();
			enhancer.setSuperclass(clazz);
			enhancer.setCallback(this);
			_recorder = (T) enhancer.create();
			_parent_path = parent_path;
		}
		@Override
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
		/** Returns the field (in its nested format if obtained from a nested method helper)
		 * @param getter - the method reference (T::<function>)
		 * @return
		 */
		public String field(final Function<T, ?> getter) {
			getter.apply(_recorder);
			return field(_name);
		}
		/** Returns the field (in its nested format if obtained from a nested method helper)
		 * @param field_name - the String version of the field name (eg needed for maps)
		 * @return
		 */
		public String field(final String field_name) {
			return _parent_path.map(s -> s + field_name).orElse(field_name);
		}
		/** Returns the field (in its nested format if obtained from a nested method helper)
		 * @param getter - the method reference (T::<function>)
		 * @return
		 */
		public String non_nested_field(final Function<T, ?> getter) {
			getter.apply(_recorder);
			return non_nested_field(_name);
		}
		/** Returns the field (in its nested format if obtained from a nested method helper)
		 * @param field_name - the String version of the field name (eg needed for maps)
		 * @return
		 */
		public String non_nested_field(final String field_name) {
			return field_name;
		}
		/** Returns a nested fieldname in an object hierarchy (given a non-null object of nested type)
		 * @param getter - the getter utility defining the fieldname of the nested object 
		 * @param from - an object of the nested type 
		 * @return a MethodNamingHelper for the nested class
		 */
		@SuppressWarnings("unchecked")
		public <U> MethodNamingHelper<U> nested(final Function<T, ?> getter, final U from) {
			return (MethodNamingHelper<U>) nested(getter, from.getClass());			
		}
		/** Returns a nested fieldname in an object hierarchy
		 * @param getter - the getter utility defining the fieldname of the nested object 
		 * @param nested_clazz - the class of the nested type
		 * @return a MethodNamingHelper for the nested class
		 */
		public <U> MethodNamingHelper<U> nested(final Function<T, ?> getter, final Class<U> nested_clazz) {
			return nested(non_nested_field(getter), nested_clazz);
		}
		/** Returns a nested fieldname in an object hierarchy (given a non-null object of nested type)
		 * @param field_name - the String version of the field name (eg needed for maps)
		 * @param from - an object of the nested type 
		 * @return a MethodNamingHelper for the nested class
		 */
		public <U> MethodNamingHelper<U> nested(final String field_name, final Class<U> nested_clazz) {
			String new_parent_path =  _parent_path.orElse("") + non_nested_field(field_name) + ".";
			return new MethodNamingHelper<U>(nested_clazz, Optional.of(new_parent_path));
		}
	}
	
	/** Configures a mapper with the desired properties for use in Aleph2
	 * @param configure_me - leave this empty to create a new mapper, or add one to configure an existing mapper
	 * @return
	 */
	public static ObjectMapper configureMapper(final Optional<ObjectMapper> configure_me) {
		final ObjectMapper mapper = configure_me.orElse(new ObjectMapper());
		mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
		mapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);		
		mapper.setSerializationInclusion(Include.NON_NULL);
		mapper.setVisibility(PropertyAccessor.IS_GETTER, JsonAutoDetect.Visibility.NONE);
		
		final SimpleModule module = new SimpleModule();
		module.addDeserializer(Number.class, new NumberDeserializer());
		mapper.registerModule(module);
		
		return mapper;
	}
	
	
	/**
	 * Converts Number values to their original type. 
	 * MongoDB converts integers to double representation. This method reverses this.
	 */
	public static class NumberDeserializer extends JsonDeserializer<Number> {

		@Override
		public Number deserialize(final JsonParser jp, final DeserializationContext ctxt)
				throws IOException, JsonProcessingException {
			
			final JsonToken currentToken = jp.getCurrentToken();
			boolean valueChanged = false;
			
			if (currentToken.equals(JsonToken.VALUE_NUMBER_FLOAT) || currentToken.equals(JsonToken.VALUE_NUMBER_INT)) {
				String s = jp.getText();
				if (s.indexOf('.') > 0 && s.indexOf('e') < 0 && s.indexOf('E') < 0) {
					while (s.endsWith("0")) {
						s = s.substring(0, s.length() - 1);
						valueChanged = true;
					}
					if (s.endsWith(".")) {
						s = s.substring(0, s.length() - 1);
						valueChanged = true;
					}
				}
				else if (s.indexOf('e') >= 0 || s.indexOf('E') >= 0)
				{
					//converts scientific notation to number in the case that it can be parsed and
					//displayed without scientific notation (which is probably how the user entered it)
					s = new BigDecimal(s).toPlainString();
					valueChanged = true;
				}
				
				
				if (valueChanged)
				{
					try {
						return Integer.parseInt(s);
					} catch (NumberFormatException e) {
						try {
							return Long.parseLong(s);
						} catch (NumberFormatException e1) {
						}		
					}
				}
				
			}
			return jp.getNumberValue();
		}
	}
}