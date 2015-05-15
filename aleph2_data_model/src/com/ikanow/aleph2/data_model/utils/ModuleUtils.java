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
import com.ikanow.aleph2.data_model.objects.shared.ConfigDataServiceEntry;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class ModuleUtils {		 
	private final static String SERVICES_PROPERTY = "service";
	private static Set<Class<?>> interfaceHasDefault = new HashSet<Class<?>>();
	private static Set<String> serviceDefaults = new HashSet<String>(Arrays.asList("SecurityService")); //TODO add default titles for the rest of the services
	private static Logger logger = LogManager.getLogger();	
	private static Map<Key, Injector> serviceInjectors = null;
	
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
		Injector parent_injector = Guice.createInjector(); //TODO put any global injections we want here
		//get any global classes we want from parent_injector.getInstance(class);
		serviceInjectors = loadServicesFromConfig(config, parent_injector);
	}
	
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
					logger.error("Error during service binding", e);
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
		logger.error("BINDING: " + entry.annotationName + " " + entry.interfaceName + " " + entry.serviceName + " " + entry.isDefault);
		
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
		} else
			injectorMap.put(getKey(serviceClazz, Optional.ofNullable(entry.annotationName)), child_injector);			
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
	 * @throws SecurityException 
	 * @throws NoSuchMethodException 
	 * @throws InvocationTargetException 
	 * @throws IllegalArgumentException 
	 * @throws IllegalAccessException 
	 */
	private static List<Module> getExtraDepedencyModules(Class<?> serviceClazz) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		//if serviceClazz implements IExtraDepedency then add those bindings
		if ( IExtraDependencyLoader.class.isAssignableFrom(serviceClazz) ) {
			logger.debug("Loading Extra Depedency Modules");
			List<Module> modules = new ArrayList<Module>();
			modules.addAll((List<Module>) serviceClazz.getMethod("getExtraDependencyModules", null).invoke(null, null));
			return modules;
		}
		return Collections.emptyList();
	}

	private static Key getKey(@NonNull Class serviceClazz, @NonNull Optional<String> serviceName) {		
		if ( serviceName.isPresent() )
			return Key.get(serviceClazz, Names.named(serviceName.get()));
		else
			return Key.get(serviceClazz);
	}
	
	public static <I> I getService(@NonNull Class<I> serviceClazz, @NonNull Optional<String> serviceName) {
		if ( serviceInjectors == null ) {
			try {
				loadModulesFromConfig(ConfigFactory.load());
			} catch (Exception e) {
				logger.error("Error loading modules", e);
			}
		}
		Key key = getKey(serviceClazz, serviceName);
		return (I) serviceInjectors.get(key).getInstance(key);
	}
	
	public static class ServiceContext implements IServiceContext {

		@Override
		public <I> I getService(Class<I> serviceClazz,
				Optional<String> serviceName) {
			return ModuleUtils.getService(serviceClazz, serviceName);
		}

		@Override
		public IColumnarService getColumnarService() {
			return getService(IColumnarService.class, Optional.empty());
		}

		@Override
		public IDocumentService getDocumentService() {
			return getService(IDocumentService.class, Optional.empty());
		}

		@Override
		public IGeospatialService getGeospatialService() {
			return getService(IGeospatialService.class, Optional.empty());
		}

		@Override
		public IGraphService getGraphService() {
			return getService(IGraphService.class, Optional.empty());
		}

		@Override
		public IManagementDbService getManagementDbService() {
			return getService(IManagementDbService.class, Optional.empty());
		}

		@Override
		public ISearchIndexService getSearchIndexService() {
			return getService(ISearchIndexService.class, Optional.empty());
		}

		@Override
		public IStorageService getStorageIndexService() {
			return getService(IStorageService.class, Optional.empty());
		}

		@Override
		public ITemporalService getTemporalService() {
			return getService(ITemporalService.class, Optional.empty());
		}

		@Override
		public ISecurityService getSecurityService() {
			return getService(ISecurityService.class, Optional.empty());
		}
	}
	
	public static class ServiceBinderModule extends AbstractModule {

		private Class serviceClass;
		private Optional<Class> interfaceClazz;
		private Optional<String> annotationName;
		
		public ServiceBinderModule(@NonNull Class serviceClazz, Optional<Class> interfaceClazz, Optional<String> annotationName) {
			this.serviceClass = serviceClazz;
			this.interfaceClazz = interfaceClazz;
			this.annotationName = annotationName;
		}
		
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
}
