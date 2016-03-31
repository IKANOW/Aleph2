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

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.inject.AbstractModule;
import com.google.inject.CreationException;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
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
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IUuidService;
import com.ikanow.aleph2.data_model.objects.shared.ConfigDataServiceEntry;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.utils.Lambdas.ThrowableWrapper.Supplier;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import fj.data.Either;

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
	private static Logger logger = LogManager.getLogger();	
	
	/** Loads the normal (non SL4J logger) into the classpath before LOG4J-OVER-SL4J or SL4J-LOG4J12 have a change to load
	 *  logging to work
	 * @author Alex
	 */
	protected static class Loggable {
		protected Optional<org.apache.log4j.Logger> _v1_logger = Optional.empty(); // (don't make final in case Loggable c'tor isn't called)
		public Loggable() {
			_v1_logger = Lambdas.get(() -> {
				try {
					return Optional.<org.apache.log4j.Logger>of(org.apache.log4j.LogManager.getLogger(ModuleUtils.class));
				}
				catch (Throwable t) {
					logger.error(ErrorUtils.getLongForm("Error creating v1 logger: {0}", t));
					return Optional.<org.apache.log4j.Logger>empty();
				}
			});			
		}
	}
	public static Loggable _v1_logger = new Loggable(); // (just to load into the classpath) 	
	
	private final static String SERVICES_PROPERTY = "service";
	private static Set<Class<?>> interfaceHasDefault = null;
	private static Set<String> serviceDefaults = new HashSet<String>(Arrays.asList("SecurityService", "ColumnarService", 
			"DataWarehouseService", "DocumentService", "GeospatialService", "GraphService", "ManagementDbService", "ManagementDbService",
			"SearchIndexService", "StorageService", "TemporalService", "CoreDistributedServices", "LoggingService"));
	@SuppressWarnings("rawtypes")
	private static Map<Key, Injector> serviceInjectors = null;
	private static Injector parent_injector = null;
	private static GlobalPropertiesBean globals = BeanTemplateUtils.build(GlobalPropertiesBean.class).done().get();
		//(do it this way to avoid having to keep changing this test every time globals changes)
	private static Config saved_config = null;
	@SuppressWarnings("rawtypes")
	private static BiFunction<Injector,Key,Object> getInstance = ModuleUtils.memoize(ModuleUtils::getInstance_onceOnly);
	
	/** Returns the static config set up by a call to loadModulesFromConfig or createInjector
	 *  INTENDED TO BE CALLED FROM guice_submodule.configure() (or later of course, though you should be using injected beans by then)
	 * @return the user config (or whatever is on the classpath as a fallback)
	 */
	public static Config getStaticConfig() {
		return Optional.ofNullable(saved_config).orElse(ConfigFactory.load());
	}
	/** Returns the global configuration bean associated with the last configuration generated
	 * @return
	 */
	public static GlobalPropertiesBean getGlobalProperties() {
		return globals;
	}
	
	/**
	 * Loads up all the services it can find in the given config file.  Typically
	 * the config comes from ConfigFactory.load() which just loads the default
	 * environment config files and env vars.  To override the config location
	 * call this function before requesting a getService.
	 * 
	 * @param config
	 * @throws Exception 
	 */
	protected static void loadModulesFromConfig(Config config) throws Exception {
		initialize(config, Optional.empty());		
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
	 * If a service has already been created with the same {full path to service} as
	 * another service, the second service will use the first as an injector, then we go back
	 * and replace the original injectors with the new one.
	 * 
	 * @param config
	 * @param parent_injector
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings("rawtypes")
	private static Map<Key, Injector> loadServicesFromConfig(
			Config config, Injector parent_injector) throws Exception {	
		//temporary map so we don't create multiple injectors for the same service class
		Map<String, Injector> service_class_injectors = new HashMap<String, Injector>(); 
		//actual list of key->injector we are returning
		Map<Key, Injector> injectors = new HashMap<Key, Injector>();
		List<ConfigDataServiceEntry> serviceProperties = PropertiesUtils.getDataServiceProperties(config, SERVICES_PROPERTY);
		List<Exception> exceptions = new ArrayList<Exception>();
		serviceProperties.stream()
			.forEach( entry -> {
				try {			
					Map<Key, Injector> injector_entries;
					Injector sibling_injector = service_class_injectors.get(entry.serviceName);
					//if there is not an injector for this serviceName, just use the parent binding
					if ( sibling_injector == null ) {
						injector_entries = bindServiceEntry(entry, parent_injector, true);
						if ( injector_entries.size() > 0 ) {
							Injector injector_entry = injector_entries.entrySet().iterator().next().getValue();		
							service_class_injectors.put(entry.serviceName, injector_entry);
						}
					} else {
						//an injector already exists, use it to create the injector, then replace all existing entries w/ it
						injector_entries = bindServiceEntry(entry, sibling_injector, false);
						if ( injector_entries.size() > 0 ) {
							Injector injector_entry = injector_entries.entrySet().iterator().next().getValue();		
							service_class_injectors.put(entry.serviceName, injector_entry);
							//replace any existing entries in injectors w/ this new copy
							for ( Entry<Key, Injector> inj : injectors.entrySet() ) {
								if ( inj.getValue() == sibling_injector ) {
									logger.info("replacing previous injector with child (note: this is expected if a single service handles multiple interfaces)");
									injectors.put(inj.getKey(), injector_entry);
								}
							}
						}
					}
					//always bind all the new entries we created					
					injectors.putAll(injector_entries);
				} catch (Exception e) {
					if (e instanceof CreationException) { // (often fails to provide useful information, so we'll insert it ourselves..)
						CreationException ce = (CreationException) e;
						e = null;
						for (com.google.inject.spi.Message m: ce.getErrorMessages()) {
							if (null == e) e = new RuntimeException(m.toString(), e);
							else {
								logger.error(ErrorUtils.get("Sub-Error during service {1}:{2} binding {0}", m.toString(), entry.interfaceName, entry.serviceName));										
							}
						}
					}
					logger.error(ErrorUtils.getLongForm("Error during service {1}:{2} binding {0}",e, entry.interfaceName, entry.serviceName));
					exceptions.add(e);
				}
			});
		if ( exceptions.size() > 0 ){
			throw new Exception(exceptions.size() + " exceptions occured during loading services from config file, first shown", exceptions.get(0));
		}
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
	 * @param addExtraDependencies 
	 * @param optional 
	 * @throws Exception
	 */
	@SuppressWarnings("rawtypes")
	private static Map<Key, Injector> bindServiceEntry(ConfigDataServiceEntry entry, Injector parent_injector, boolean addExtraDependencies) throws Exception {
		Map<Key, Injector> injectorMap = new HashMap<Key, Injector>();
		entry = new ConfigDataServiceEntry(entry.annotationName, entry.interfaceName, entry.serviceName, entry.isDefault || serviceDefaults.contains(entry.annotationName));
		logger.info("BINDING: " + entry.annotationName + " " + entry.interfaceName + " " + entry.serviceName + " " + entry.isDefault + " " + addExtraDependencies );
				
		Class serviceClazz = Class.forName(entry.serviceName);		
		List<Module> modules = new ArrayList<Module>();
		if ( addExtraDependencies )
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
			logger.debug("Loading Extra Dependency Modules");
			List<Module> modules = new ArrayList<Module>();
			Class[] param_types = new Class[0];
			Object[] params = new Object[0];			
			try {
				modules.addAll((List<Module>) serviceClazz.getMethod("getExtraDependencyModules", param_types).invoke(null, params));
			} catch (IllegalAccessException | IllegalArgumentException
					| InvocationTargetException | NoSuchMethodException
					| SecurityException e) {
				logger.error(ErrorUtils.getLongForm("Module: " + serviceClazz.getSimpleName() + " implemented IExtraDependencyModule but forgot to create the static method getExtraDependencyModules():List<Module> double check you have this set up correctly. {0}",e));
				throw new Exception("Module: " + serviceClazz.getSimpleName() + " implemented IExtraDependencyModule but forgot to create the static method getExtraDependencyModules():List<Module> double check you have this set up correctly. \n" + e.getMessage());
			}
			
			return modules;
		}
		return Collections.emptyList();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static Key getKey(Class serviceClazz, Optional<String> serviceName) {		
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
	public static <I> I getService(Class<I> serviceClazz, Optional<String> serviceName) {
		return Optional.ofNullable(getServiceProvider(serviceClazz, serviceName)).map(i -> i.get()).orElse(null);
	}
	
	/** FOR CIRCULAR DEPENDENCY CASES
	 * Returns back an instance of the requested serviceClazz/annotation
	 * if an injector exists for it.  If the injectors have not yet been
	 * created will try to load them from the default config.
	 * 
	 * @param serviceClazz
	 * @param serviceName
	 * @return
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <I> Provider<I> getServiceProvider(Class<I> serviceClazz, Optional<String> serviceName) {
		if ( serviceInjectors == null ) {
			try {
				loadModulesFromConfig(ConfigFactory.load());
			} catch (Exception e) {
				logger.error("Error loading modules", e);
			}
		}
		Key key = getKey(serviceClazz, serviceName);		
		Injector injector = serviceInjectors.get(key);		
		if ( injector != null ) {			
			//return (I) getInstance_onceOnly(injector, key);
			return (Provider<I>) getInstance.apply(injector, key);
			//return (I) injector.getInstance(key);
		}
		else 
			return null;
	}
	
	/**
	 * Helper function to make creating an instance of our memoized bifunction easier to read.
	 * 
	 * @param function
	 * @return
	 */
	private static <T, U, R> BiFunction<T, U, R> memoize(final BiFunction<T, U, R> function) {
		return new BiFunctionMemoize<T, U, R>().doMemoizeIgnoreSecondArg(function);
	}
	
	/**
	 * Class to handle a bifunction memoize.
	 * 
	 * @author Burch
	 *
	 * @param <T>
	 * @param <U>
	 * @param <R>
	 */
	private static class BiFunctionMemoize<T, U, R> {
		protected BiFunctionMemoize() {}
		private final Map<T, R> instance_cache = new ConcurrentHashMap<T, R>();

		/**
		 * If T is in the cache, returns the instance, otherwise calls function with T,U.
		 * 
		 * @param function
		 * @return
		 */
		public BiFunction<T, U, R> doMemoizeIgnoreSecondArg(final BiFunction<T, U, R> function) {
			return (input1, input2) -> {
				if (instance_cache.keySet().contains(input1)) {
					return instance_cache.get(input1);
				}
				else {
					final R res = function.apply(input1, input2);
					instance_cache.put(input1, res);
					return res;
				}
			};
			//computeIfAbsent cannot be called recursively currently (oracle claims to fix that up later)
			//if they fix it, this function can be minimized to this:
			//return (input1, input2) -> instance_cache.computeIfAbsent(input1, ___ -> function.apply(input1, input2));
		}
	}
	
	/**
	 * Returns an instance of the key given an injector
	 * 
	 * @param injector
	 * @param key
	 * @return
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static Object getInstance_onceOnly(Injector injector, Key key) {		
		return new CachingProvider(injector.getProvider(key));
	}
	
	/**
	 * Initializes the module utils class.
	 * 
	 * This includes reading in the properties and setting up all the initial bindings
	 * found in the config.
	 * 
	 * @param config
	 * @throws Exception
	 */
	private static void initialize(Config config, Optional<Function<List<Module>, Injector>> injector_builder_lambda) throws Exception {
		saved_config = config;
		final Config subconfig = PropertiesUtils.getSubConfig(config, GlobalPropertiesBean.PROPERTIES_ROOT).orElse(null);
		synchronized (ModuleUtils.class) {
			globals = BeanTemplateUtils.from(subconfig, GlobalPropertiesBean.class);
		}
		if ( parent_injector != null)
			logger.warn("Resetting default bindings, this could cause issues if it occurs after initialization and typically should not occur except during testing");
		interfaceHasDefault = new HashSet<Class<?>>();
		final ServiceModule service_module = new ServiceModule();
		parent_injector = injector_builder_lambda.map(f -> f.apply(Arrays.asList(service_module))).orElseGet(() -> Guice.createInjector(service_module));		
		serviceInjectors = loadServicesFromConfig(config, parent_injector);		
	}
	
	/** GENERIC - CALLED BY TEST / APP
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
	private static Injector createInjector(List<Module> modules, Optional<Config> config, Optional<Function<List<Module>, Injector>> injector_builder_lambda) throws Exception {		
			
		try {
		if ( parent_injector == null && !config.isPresent() )
			config = Optional.of(ConfigFactory.load());
		if ( config.isPresent() )
			initialize(config.get(), injector_builder_lambda);
		
		} catch (Throwable e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
			throw e;
			
		}
		return parent_injector.createChildInjector(modules);
	}
	

	/** THIS VERSION IS FOR TESTS - CAN BE RUN MULTIPLE TIMES
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
	public static Injector createTestInjector(List<Module> modules, Optional<Config> config) throws Exception {
		_test_mode.set(true); 
		final Injector i = createInjector(modules, config, Optional.empty());
		_test_injector.obtrudeValue(i);
		return i;
	}
	
	/** For tests not using Guice, this has to be called (or any modules that use the ModuleUtils.getAppInjector.thenRun() constructs will hang)
	 */
	public static void disableTestInjection() {
		_test_mode.set(true); 
		_test_injector.complete(null);
	}
	
	// Some application level global state
	private static AtomicBoolean _test_mode = new AtomicBoolean();
	private enum GlobalGuiceState { idle, initializing, complete };
	private static GlobalGuiceState _module_state = GlobalGuiceState.idle;
	private static final CompletableFuture<Injector> _test_injector = new CompletableFuture<>();
	private static final CompletableFuture<Injector> _app_injector = new CompletableFuture<>();
	private static final CompletableFuture<Boolean> _called_ctor = new CompletableFuture<>();
	
	/** APP LEVEL VERSION - ONLY CREATES STUFF ONCE. CANNOT NORMALLY BE CALLED FROM JUNIT TESTS - USE createTestInjector for that
	 * Creates a single application level injector and either injects members into application, or 
	 * If called multiple times, will wait for the first time to complete before performing the desired action.
	 * To get the created injector, use getAppInjector
	 * 
	 * @param modules Any modules you wanted added to your child injector (put your bindings in these)
	 * @param config If exists will reset injectors to create defaults via the config
	 * @param application - either a class to create, or an object to inject
	 * @return
	 * @throws Exception
	 */
	public static <T> T initializeApplication(final List<Module> modules, final Optional<Config> config, final Either<Class<T>, T> application) throws Exception {
		return initializeApplication(modules, config, application, Optional.empty());
		
	}
	/** APP LEVEL VERSION - ONLY CREATES STUFF ONCE. CANNOT NORMALLY BE CALLED FROM JUNIT TESTS - USE createTestInjector for that
	 * Creates a single application level injector and either injects members into application, or 
	 * If called multiple times, will wait for the first time to complete before performing the desired action.
	 * To get the created injector, use getAppInjector
	 * 
	 * @param modules Any modules you wanted added to your child injector (put your bindings in these)
	 * @param config If exists will reset injectors to create defaults via the config
	 * @param application - either a class to create, or an object to inject
	 * @return
	 * @throws Exception
	 */
	public static <T> T initializeApplication(final List<Module> modules, final Optional<Config> config, final Either<Class<T>, T> application,
			Optional<Function<List<Module>, Injector>> injector_builder_lambda
			) throws Exception
	{
		_test_mode.set(false); 
		boolean initializing = false;
		T return_val = null;
		
		final Supplier<T> on_complete = () -> {
			try {
				return application.either(app_clazz -> _app_injector.join().getInstance(app_clazz), app_obj -> {
					_app_injector.join().injectMembers(app_obj);
					return app_obj;
				});
			}
			catch (Throwable t) { // These will normally be guice errors so will break on wrap 
				logger.error(ErrorUtils.getLongForm("ModuleUtils.initializeApplication.onComplete {0}", t));
				throw t;
			}				
		};
		
		synchronized (ModuleUtils.class) {
			if (GlobalGuiceState.idle == _module_state) { // winner!
				_module_state = GlobalGuiceState.initializing; 
			}
			else if (GlobalGuiceState.initializing == _module_state) { // wait for the initial guice module to complete
				initializing = true;
			}
			else { // (active)
				return Lambdas.wrap_u(on_complete).get();
			}
		}
		if (initializing) { // if here just wait for it to complete
			for (;;) {
				Thread.sleep(250L);
				synchronized (ModuleUtils.class) {
					if (GlobalGuiceState.complete == _module_state) break;
				}
			}
			return_val = Lambdas.wrap_u(on_complete).get();
		}
		else { // this version actually gets to complete it
			try {
				// If here then we're the only person that gets to initialize guice
				_app_injector.complete(createInjector(modules, config, injector_builder_lambda));
				return_val = Lambdas.wrap_u(on_complete).get();
				_called_ctor.complete(true);
			}
			catch (Throwable t) {
				_app_injector.completeExceptionally(t);
				_called_ctor.completeExceptionally(t);
				throw t;
			}
			finally {
				synchronized (ModuleUtils.class) {
					_module_state = GlobalGuiceState.complete;
				}			
			}
		}
		return return_val;
	}
	
	/** Returns a future to an app injector that is only valid after the app injector has been created *and* the app has been initialized
	 *  (via at least one call to app initialization)
	 * @return
	 */
	public static CompletableFuture<Injector> getAppInjector() {		
		return _test_mode.get()
				? _test_injector
				: _app_injector.thenCombine(_called_ctor, (i, b) -> i);
	}
	
	/** Caching wrapper for Provider
	 * @author Alex
	 *
	 * @param <I>
	 */
	public static class CachingProvider<I> implements Provider<I> {
		final SetOnce<I> _cache = new SetOnce<>();
		final Provider<I> _parent;

		/** User c'tor
		 * @param provider
		 */
		public CachingProvider(Provider<I> provider) {
			_parent = provider;
		}
		
		@Override
		public I get() {
			synchronized (CachingProvider.class) {
				if (!_cache.isSet()) {
					_cache.set(_parent.get());				
				}
				return _cache.get();
			}
		}
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
		public <I extends IUnderlyingService> Optional<I> getService(Class<I> serviceClazz,
				Optional<String> serviceName) {
			return getServiceProvider(serviceClazz, serviceName).map(s -> s.get());
		}

		@Override
		public <I extends IUnderlyingService> Optional<Provider<I>> getServiceProvider(Class<I> serviceClazz,
				Optional<String> serviceName) {
			return Optional.ofNullable(ModuleUtils.getServiceProvider(serviceClazz, serviceName));
		}
		
		@Override
		public Optional<IColumnarService> getColumnarService() {
			return getColumnarServiceProvider().map(s -> s.get());
		}
		@Override
		public Optional<Provider<IColumnarService>> getColumnarServiceProvider() {
			return getServiceProvider(IColumnarService.class, Optional.empty());
		}


		@Override
		public Optional<IDocumentService> getDocumentService() {
			return getDocumentServiceProvider().map(s -> s.get());
		}
		@Override
		public Optional<Provider<IDocumentService>> getDocumentServiceProvider() {
			return getServiceProvider(IDocumentService.class, Optional.empty());
		}

		@Override
		public Optional<IGeospatialService> getGeospatialService() {
			return getGeospatialServiceProvider().map(s -> s.get());
		}
		@Override
		public Optional<Provider<IGeospatialService>> getGeospatialServiceProvider() {
			return getServiceProvider(IGeospatialService.class, Optional.empty());
		}


		@Override
		public Optional<IGraphService> getGraphService() {
			return getGraphServiceProvider().map(s -> s.get());
		}
		@Override
		public Optional<Provider<IGraphService>> getGraphServiceProvider() {
			return getServiceProvider(IGraphService.class, Optional.empty());
		}


		@Override
		public IManagementDbService getCoreManagementDbService() {
			return getCoreManagementDbServiceProvider().get();
		}
		@Override
		public Provider<IManagementDbService> getCoreManagementDbServiceProvider() {
			return getServiceProvider(IManagementDbService.class, IManagementDbService.CORE_MANAGEMENT_DB).get();
		}

		@Override
		public Optional<ISearchIndexService> getSearchIndexService() {
			return getSearchIndexServiceProvider().map(s -> s.get());
		}
		@Override
		public Optional<Provider<ISearchIndexService>> getSearchIndexServiceProvider() {
			return getServiceProvider(ISearchIndexService.class, Optional.empty());
		}

		@Override
		public IStorageService getStorageService() {
			return getStorageServiceProvider().get();
		}
		@Override
		public Provider<IStorageService> getStorageServiceProvider() {
			return getServiceProvider(IStorageService.class, Optional.empty()).get();
		}


		@Override
		public Optional<ITemporalService> getTemporalService() {
			return getTemporalServiceProvider().map(s -> s.get());
		}
		@Override
		public Optional<Provider<ITemporalService>> getTemporalServiceProvider() {
			return getServiceProvider(ITemporalService.class, Optional.empty());
		}

		@Override
		public ISecurityService getSecurityService() {
			return getSecurityServiceProvider().get();
		}
		@Override
		public Provider<ISecurityService> getSecurityServiceProvider() {
			return getServiceProvider(ISecurityService.class, Optional.empty()).get();
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
		public ServiceBinderModule(Class serviceClazz, Optional<Class> interfaceClazz, Optional<String> annotationName) {
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
			// I want to do this, but had to back out because too much of the test code generates (harmless) circular dependencies currently
			// Will consider reintroducing once the code gets a bit more tidied up 
			//binder().disableCircularProxies();
			
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
			// I want to do this, but had to back out because too much of the test code generates (harmless) circular dependencies currently
			// Will consider reintroducing once the code gets a bit more tidied up 			
			//binder().disableCircularProxies();
			
			bind(IServiceContext.class).to(ServiceContext.class).in(Scopes.SINGLETON);
			bind(IUuidService.class).toInstance(UuidUtils.get());
			bind(GlobalPropertiesBean.class).toInstance(globals);
		}
		
	}
}
