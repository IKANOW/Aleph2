package com.ikanow.aleph2.data_model.interfaces.data_access;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_model.utils.ContextUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Class that sets up the guice bindings so the AccessContext can be instantiated.  This
 * classes initAccessContext() should be called once so ContextUtils has an instance to return.
 * 
 * It will attempt to call this class the first time someone tries to get the AccessContext and it does
 * not exist, so nothing actually needs to be done.
 * 
 * @author Burch
 *
 */
public class AccessModule extends AbstractModule {
	
	private final String ACCESS_CONTEXT_CONFIG_NAME = "access_manager.service"; //config name for the access manager
	private final Logger logger = Logger.getLogger(this.getClass().getName());
	
	/**
	 * This should only be called once, it sets up the AccessContext binding and creates an instance
	 * that can be retrieved from {@link ContextUtils.getAccessModule()}
	 */	
	public static void initAccessContext() {
		Injector injector = Guice.createInjector(new AccessModule());
		injector.getInstance(ContextUtils.class); //force creation of the ContextUtils object so accessContext gets set
	}
	
	/**
	 * Sets up the guice bindings, reads them in from a config file.
	 * 
	 */
	@Override
	protected void configure() {
		try {
			loadAccessContextFromConfig();
		} catch (Exception e){
			logger.log(Level.ALL, e.getMessage(), e);
		}
	}
	
	/**
	 * Loads the config from disk/properties and then loads the access context
	 * from the config supplied.
	 * 
	 * @throws Exception
	 */
	private void loadAccessContextFromConfig() throws Exception {
		Config config = ConfigFactory.load();		
		loadAccessContextFromConfig(config);	
	}
	
	/**
	 * Takes a config object and loads the config from ACCESS_CONTEXT_CONFIG_NAME.
	 * 
	 * @param config
	 * @throws Exception
	 */
	private void loadAccessContextFromConfig(@NonNull Config config) throws Exception {
		String service = config.getString(ACCESS_CONTEXT_CONFIG_NAME);
		//Convert string class name to actual class and bind to IAccessContext
		@SuppressWarnings("unchecked")
		Class<? extends IAccessContext> class2 = (Class<? extends IAccessContext>) Class.forName((String) service);
		logger.fine("Binding IAccessContext to " + class2.getCanonicalName());
		bind(IAccessContext.class).to(class2);
	}

}
