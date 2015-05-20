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

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import com.ikanow.aleph2.data_model.interfaces.data_access.IServiceContext;
import com.ikanow.aleph2.data_model.interfaces.data_services.IColumnarService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IDocumentService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IGeospatialService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IGraphService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.data_services.ITemporalService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IExtraDependencyLoader;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IUuidService;
import com.ikanow.aleph2.data_model.objects.shared.ConfigDataServiceEntry;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Utility functions for loading modules into the system.  Typically is used
 * by calling {@link #loadModulesFromConfig(Config)} this will create all
 * the service injectors needed.  Services can then be retrieved from {@link #getService(Class, Optional)}
 * or using any of the other Contexts that implement IServiceContext.
 * 
 * @author Burch
 *
 */
public class ModuleUtils {		 
	private final static String SERVICES_PROPERTY = "service";
	private static Set<Class<?>> interfaceHasDefault = null;
	private static Set<String> serviceDefaults = new HashSet<String>(Arrays.asList("SecurityService", "ColumnarService", 
			"DataWarehouseService", "DocumentService", "GeospatialService", "GraphService", "ManagementDbService", 
			"SearchIndexService", "StorageService", "TemporalService", "CoreDistributedServices"));
	private static Logger logger = LogManager.getLogger();	
	@SuppressWarnings("rawtypes")
	private static Map<Key, Injector> serviceInjectors = null;
	private static Injector parent_injector = null;
	private static GlobalPropertiesBean globals = new GlobalPropertiesBean(null, null, null); // (all defaults)
	
	/**
	 * Loads up all the services it can find in the given config file.  Typically
	 * the config comes from ConfigFactory.load() which just loads the default
	 * environment config files and env vars.  To override the config location
	 * call this function before requesting a getService.
	 * 
	 * @param config
	 * @throws Exception 
	 */
	public static void loadModulesFromConfig(@NonNull Config config) throws Exception {
		initialize(config);		
	}
	
	/**
	 * Reads in the config file for properties of the format:
	 * service.{Service name}.interface={full path to interface (optional)}
	 * service.{Service name}.service={full path to service}
	 * service.{Service name}.default={true|false (optional)}
	 * 
	 * Will then try to create injectors for each of these services that can be retrieved by their 
	 * Service name.  If default is set to true they can be retrieved by their
	 * interface/service directly.
	 * 
	 * @param config
	 * @param parent_injector
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings("rawtypes")
	private static Map<Key, Injector> loadServicesFromConfig(
			@NonNull Config config, Injector parent_injector) throws Exception {				
		Map<Key, Injector> injectors = new HashMap<Key, Injector>();
		List<ConfigDataServiceEntry> serviceProperties = PropertiesUtils.getDataServiceProperties(config, SERVICES_PROPERTY);
		List<Exception> exceptions = new ArrayList<Exception>();
		serviceProperties.stream()
			.forEach( entry -> {
				try {
					injectors.putAll(bindServiceEntry(entry, parent_injector));
				} catch (Exception e) {
					logger.error("Error during service binding");
					exceptions.add(e);
				}
			});
		if ( exceptions.size() > 0 )
			throw new Exception(exceptions.size() + " exceptions occured during loading services from config file.");
		return injectors;
	}
	
	/**
	 * Creates a child injector w/ all the necessary bindings setup from
	 * a config entry.  Most actions attempted in this method will throw
	 * an exception if something is wrong so it can be caught above.
	 * 
	 * How this works:
	 * First we set this entry to a default one if they use the same name
	 * as the default hard coded names or set default to true in the config.
	 * Then we attempt to get the service class and interface class (if exists)
	 * to make sure they are valid.
	 * If the service class extends IExtraDependencyLoader we will try to grab its
	 * depedency modules.
	 * 
	 * The injector is then creates from those extra depedencies and a custom one
	 * that creates the bindings from interface to service (w/ annotations).  The
	 * injector is set in the service context then so it can be used in the future.
	 * 
	 * @param entry
	 * @param parent_injector
	 * @throws Exception
	 */
	@SuppressWarnings("rawtypes")
	private static Map<Key, Injector> bindServiceEntry(@NonNull ConfigDataServiceEntry entry, @NonNull Injector parent_injector) throws Exception {
		Map<Key, Injector> injectorMap = new HashMap<Key, Injector>();
		entry = new ConfigDataServiceEntry(entry.annotationName, entry.interfaceName, entry.serviceName, entry.isDefault || serviceDefaults.contains(entry.annotationName));		
		logger.info("BINDING: " + entry.annotationName + " " + entry.interfaceName + " " + entry.serviceName + " " + entry.isDefault);
		
		Class serviceClazz = Class.forName(entry.serviceName);		
		List<Module> modules = new ArrayList<Module>();
		modules.addAll(getExtraDepedencyModules(serviceClazz));
		Optional<Class> interfaceClazz = getInterfaceClass(entry.interfaceName);
		if ( entry.isDefault && interfaceClazz.isPresent() )
			validateOnlyOneDefault(interfaceClazz);		
		
		//add default service binding w/ annotation
		modules.add(new ServiceBinderModule(serviceClazz, interfaceClazz, Optional.ofNullable(entry.annotationName)));
		if ( entry.isDefault ) //if default, add service binding w/o annotation
			modules.add(new ServiceBinderModule(serviceClazz, interfaceClazz, Optional.empty()));
		//create the child injector
		Injector child_injector = parent_injector.createChildInjector(modules);
		//add injector to serviceContext for interface+annotation, interface w/o annotation, or service only 
		if ( interfaceClazz.isPresent()) {
			injectorMap.put(getKey(interfaceClazz.get(), Optional.ofNullable(entry.annotationName)), child_injector);
			if ( entry.isDefault )
				injectorMap.put(getKey(interfaceClazz.get(), Optional.empty()), child_injector);
		} else {
			injectorMap.put(getKey(serviceClazz, Optional.empty()), child_injector);
		}
		return injectorMap;
	}

	/**
	 * Throws an exception if this interface already has a default binding.
	 * 
	 * If not, adds this interface to the list of default bindings (so future checks will throw an error).
	 * 
	 * @param interfaceClazz An interface that wants to set the default binding.
	 * @throws Exception
	 */
	@SuppressWarnings("rawtypes")
	private static void validateOnlyOneDefault(Optional<Class> interfaceClazz) throws Exception {
		if (interfaceHasDefault.contains(interfaceClazz.get()))
			throw new Exception(interfaceClazz.get() + " already had a default binding, there can be only one.");
		else
			interfaceHasDefault.add(interfaceClazz.get());
	}

	/**
	 * Returns back an optional with the interface class if interfaceName was not empty.
	 * 
	 * Will throw an exception if interfaceName cannot be turned into a class. (via Class.forName(interfaceName);)
	 * 
	 * @param interfaceName interface to try and get the Class of or empty
	 * @return
	 * @throws ClassNotFoundException
	 */
	@SuppressWarnings("rawtypes")
	private static Optional<Class> getInterfaceClass(Optional<String> interfaceName) throws ClassNotFoundException {
		if ( interfaceName.isPresent() ) 
			return Optional.of(Class.forName(interfaceName.get()));
		return Optional.empty();
	}

	/**
	 * If service class implements {@link com.ikanow.aleph2.data_model.interfaces.shared_services.IExtraDependencyLoader}
	 * then try to invoke the static method getExtraDepedencyModules to retrieve any additional dependencies this service needs.
	 * 
	 * Otherwise return an empty list.
	 * 
	 * @param serviceClazz
	 * @return
	 * @throws Exception 
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static List<Module> getExtraDepedencyModules(Class<?> serviceClazz) throws Exception {
		//if serviceClazz implements IExtraDepedency then add those bindings
		if ( IExtraDependencyLoader.class.isAssignableFrom(serviceClazz) ) {
			logger.debug("Loading Extra Depedency Modules");
			List<Module> modules = new ArrayList<Module>();
			Class[] param_types = new Class[0];
			Object[] params = new Object[0];			
			try {
				modules.addAll((List<Module>) serviceClazz.getMethod("getExtraDependencyModules", param_types).invoke(null, params));
			} catch (IllegalAccessException | IllegalArgumentException
					| InvocationTargetException | NoSuchMethodException
					| SecurityException e) {
				logger.error("Module: " + serviceClazz.getSimpleName() + " implemented IExtraDependencyModule but forgot to create the static method getExtraDependencyModules():List<Module> double check you have this set up correctly.");
				throw new Exception("Module: " + serviceClazz.getSimpleName() + " implemented IExtraDependencyModule but forgot to create the static method getExtraDependencyModules():List<Module> double check you have this set up correctly. \n" + e.getMessage());
			}
			
			return modules;
		}
		return Collections.emptyList();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static Key getKey(@NonNull Class serviceClazz, @NonNull Optional<String> serviceName) {		
		if ( serviceName.isPresent() )
			return Key.get(serviceClazz, Names.named(serviceName.get()));
		else
			return Key.get(serviceClazz);
	}
	
	/**
	 * Returns back an instance of the requested serviceClazz/annotation
	 * if an injector exists for it.  If the injectors have not yet been
	 * created will try to load them from the default config.
	 * 
	 * @param serviceClazz
	 * @param serviceName
	 * @return
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <I> I getService(@NonNull Class<I> serviceClazz, @NonNull Optional<String> serviceName) {
		if ( serviceInjectors == null ) {
			try {
				loadModulesFromConfig(ConfigFactory.load());
			} catch (Exception e) {
				logger.error("Error loading modules", e);
			}
		}
		Key key = getKey(serviceClazz, serviceName);
		Injector injector = serviceInjectors.get(key);
		if ( injector != null )
			return (I) injector.getInstance(key);
		else 
			return null;
	}
	
	private static void initialize(@NonNull Config config) throws Exception {	
		final Config subconfig = PropertiesUtils.getSubConfig(config, GlobalPropertiesBean.PROPERTIES_ROOT).orElse(null);
		synchronized (ModuleUtils.class) {
			globals = BeanTemplateUtils.from(subconfig, GlobalPropertiesBean.class);
		}
		logger.info("Resetting default bindings, this could cause issues if it occurs after initialization and typically should not occur except during testing");
		interfaceHasDefault = new HashSet<Class<?>>();
		parent_injector = Guice.createInjector(new ServiceModule());		
		serviceInjectors = loadServicesFromConfig(config, parent_injector);		
	}
	
	/**
	 * Creates a child injector from our parent configured injector to allow applications to take
	 * advantage of our injection without having to create a config file.  The typical reason to
	 * do this is to inject the IServiceContext into your application so you can access the other
	 * configured services via {@link com.ikanow.aleph2.data_model.interface.data_access.IServiceContext#getService()}
	 * 
	 * @param modules Any modules you wanted added to your child injector (put your bindings in these)
	 * @param config If exists will reset injectors to create defaults via the config
	 * @return
	 * @throws Exception
	 */
	public static Injector createInjector(@NonNull List<Module> modules, @NonNull Optional<Config> config) throws Exception {		
		if ( parent_injector == null && !config.isPresent() )
			config = Optional.of(ConfigFactory.load());
		if ( config.isPresent() )
			initialize(config.get());
		return parent_injector.createChildInjector(modules);
	}
	
	/**
	 * Implementation of the IServiceContext class for easy usage
	 * from the other contexts.
	 * 
	 * @author Burch
	 *
	 */
	public static class ServiceContext implements IServiceContext {

		/**
		 * Delegates to the ModuleUtils get service call for the
		 * requested class, serviceName.
		 * 
		 */
		@Override
		public <I> I getService(Class<I> serviceClazz,
				Optional<String> serviceName) {
			return ModuleUtils.getService(serviceClazz, serviceName);
		}

		/**
		 * Utility function that just calls {@link #getService(Class, Optional)}
		 * 
		 */
		@Override
		public IColumnarService getColumnarService() {
			return getService(IColumnarService.class, Optional.empty());
		}

		/**
		 * Utility function that just calls {@link #getService(Class, Optional)}
		 * 
		 */
		@Override
		public IDocumentService getDocumentService() {
			return getService(IDocumentService.class, Optional.empty());
		}

		/**
		 * Utility function that just calls {@link #getService(Class, Optional)}
		 * 
		 */
		@Override
		public IGeospatialService getGeospatialService() {
			return getService(IGeospatialService.class, Optional.empty());
		}

		/**
		 * Utility function that just calls {@link #getService(Class, Optional)}
		 * 
		 */
		@Override
		public IGraphService getGraphService() {
			return getService(IGraphService.class, Optional.empty());
		}

		/**
		 * Utility function that just calls {@link #getService(Class, Optional)}
		 * 
		 */
		@Override
		public IManagementDbService getManagementDbService() {
			return getService(IManagementDbService.class, Optional.empty());
		}

		/**
		 * Utility function that just calls {@link #getService(Class, Optional)}
		 * 
		 */
		@Override
		public ISearchIndexService getSearchIndexService() {
			return getService(ISearchIndexService.class, Optional.empty());
		}

		/**
		 * Utility function that just calls {@link #getService(Class, Optional)}
		 * 
		 */
		@Override
		public IStorageService getStorageIndexService() {
			return getService(IStorageService.class, Optional.empty());
		}

		/**
		 * Utility function that just calls {@link #getService(Class, Optional)}
		 * 
		 */
		@Override
		public ITemporalService getTemporalService() {
			return getService(ITemporalService.class, Optional.empty());
		}

		/**
		 * Utility function that just calls {@link #getService(Class, Optional)}
		 * 
		 */
		@Override
		public ISecurityService getSecurityService() {
			return getService(ISecurityService.class, Optional.empty());
		}

		@Override
		public GlobalPropertiesBean getGlobalProperties() {
			return globals;
		}
	}
	
	/**
	 * Module that the config loader uses to bind the configured interfaces
	 * to the given services.
	 * 
	 * @author Burch
	 *
	 */
	public static class ServiceBinderModule extends AbstractModule {

		@SuppressWarnings("rawtypes")
		private Class serviceClass;
		@SuppressWarnings("rawtypes")
		private Optional<Class> interfaceClazz;
		private Optional<String> annotationName;
		
		@SuppressWarnings("rawtypes")
		public ServiceBinderModule(@NonNull Class serviceClazz, Optional<Class> interfaceClazz, Optional<String> annotationName) {
			this.serviceClass = serviceClazz;
			this.interfaceClazz = interfaceClazz;
			this.annotationName = annotationName;
		}
		
		/**
		 * Configures the given interface/service against the following rules
		 * A. If an interface is present
		 * 		A1. Annotation Present: Bind to service with annotationName
		 * 		A2. No Annotation Present: Bind to service
		 * B. Otherwise just bind service, it cannot use the annotation because it must be requested directly from the classname
		 */
		@SuppressWarnings("unchecked")
		@Override
		protected void configure() {
			if ( interfaceClazz.isPresent() ) {
				if ( annotationName.isPresent() ) {
					bind(interfaceClazz.get()).annotatedWith(Names.named(annotationName.get())).to(serviceClass).in(Scopes.SINGLETON); 
				} else
					bind(interfaceClazz.get()).to(serviceClass).in(Scopes.SINGLETON);
			} else {
				bind(serviceClass).in(Scopes.SINGLETON); //you can't annotate a plain bind
			}		
		}

	}
	
	/**
	 * Default IServiceContext binding module.
	 * 
	 * @author Burch
	 *
	 */
	public static class ServiceModule extends AbstractModule {

		@Override
		protected void configure() {
			bind(IServiceContext.class).to(ServiceContext.class).in(Scopes.SINGLETON);
			bind(IUuidService.class).toInstance(UuidUtils.get());
			bind(GlobalPropertiesBean.class).toInstance(globals);
		}
		
	}
}
