package com.ikanow.aleph2.access_manager.data_access;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.ikanow.aleph2.data_model.interfaces.data_access.IServiceContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IExtraDependencyLoader;
import com.ikanow.aleph2.data_model.objects.shared.ConfigDataServiceEntry;
import com.ikanow.aleph2.data_model.utils.ContextUtils;
import com.ikanow.aleph2.data_model.utils.PropertiesUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Handles setting up all the bindings managed by the config files.
 * 
 * @author Burch
 *
 */
public class AccessMananger {
	private final static String SERVICES_PROPERTY = "service";
	private static Set<Class<?>> interfaceHasDefault = new HashSet<Class<?>>();
	private static Set<String> serviceDefaults = new HashSet<String>(Arrays.asList("SecurityService")); //TODO add default titles for the rest of the services
	private static Logger logger = Logger.getLogger(AccessMananger.class.getName());
	
	/**
	 * Initializes the Access Manager application, this will kick off the creation
	 * of the ServiceContext and setting up the bindings for that.
	 * 
	 * Uses ConfigFactory.load() as the config to read in.
	 * 
	 * @throws Exception
	 */
	public static void initialize() throws Exception {
		initialize(ConfigFactory.load());
	}
	
	/**
	 * Initializes the Access Manager application, this will kick off the creation
	 * of the ServiceContext and setting up the bindings for that.  Uses the passed
	 * in configuration.
	 * 
	 * @param config
	 * @throws Exception
	 */
	public static void initialize(@NonNull Config config) throws Exception {
		Injector parent_injector = Guice.createInjector(new ServiceModule());
		ServiceContext serviceContext = (ServiceContext) parent_injector.getInstance(IServiceContext.class);						
		loadServicesFromConfig(config, parent_injector, serviceContext); //TODO this modifies service context, we don't want to do that
		ContextUtils.setServiceContext(serviceContext);
	}
	
	/**
	 * Reads the config file and loads every service it comes across following the config format:
	 * service.{serviceName}.interface={full interface path (optional) defaults to empty}
	 * service.{serviceName}.service={full service path}
	 * service.{serviceName}.default={true|false, (optional) defaults to false}
	 * 
	 * Tries to setup bindings for every service it finds, and adds the injector to serviceContext
	 * so they can be created in the future.
	 * 
	 * @param config
	 * @param parent_injector
	 * @param serviceContext
	 * @throws Exception
	 */
	private static void loadServicesFromConfig(@NonNull Config config, @NonNull Injector parent_injector, @NonNull ServiceContext serviceContext) throws Exception {		
		List<ConfigDataServiceEntry> serviceProperties = PropertiesUtils.getDataServiceProperties(config, SERVICES_PROPERTY);
		List<Exception> exceptions = new ArrayList<Exception>();
		serviceProperties.stream()
			.forEach( entry -> {
				try {
					bindServiceEntry(entry, parent_injector, serviceContext);
				} catch (Exception e) {
					logger.log(Level.ALL, "Error during service binding", e);
					exceptions.add(e);
				}
			});
		if ( exceptions.size() > 0 )
			throw new Exception(exceptions.size() + " exceptions occured during loading services from config file.");
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
	 * @param serviceContext
	 * @throws Exception
	 */
	@SuppressWarnings("rawtypes")
	private static void bindServiceEntry(@NonNull ConfigDataServiceEntry entry, @NonNull Injector parent_injector, @NonNull ServiceContext serviceContext) throws Exception {
		entry = new ConfigDataServiceEntry(entry.annotationName, entry.interfaceName, entry.serviceName, entry.isDefault || serviceDefaults.contains(entry.serviceName));		
		logger.fine("BINDING: " + entry.annotationName + " " + entry.interfaceName + " " + entry.serviceName + " " + entry.isDefault);
		
		Class serviceClazz = Class.forName(entry.serviceName);		
		List<Module> modules = getExtraDepedencyModules(serviceClazz);
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
			serviceContext.addDataService(interfaceClazz.get(), Optional.ofNullable(entry.annotationName), child_injector);
			if ( entry.isDefault )
				serviceContext.addDataService(interfaceClazz.get(), Optional.empty(), child_injector);
		} else
			serviceContext.addDataService(serviceClazz, Optional.ofNullable(entry.annotationName), child_injector);
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
			logger.fine("Loading Extra Depedency Modules");
			List<Module> modules = new ArrayList<Module>();
			modules.addAll((List<Module>) serviceClazz.getMethod("getExtraDependencyModules", null).invoke(null, null));
			return modules;
		}
		return Collections.emptyList();
	}
}
