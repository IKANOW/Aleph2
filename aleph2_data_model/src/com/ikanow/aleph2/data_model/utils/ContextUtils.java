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

import java.util.logging.Level;
import java.util.logging.Logger;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_model.interfaces.data_access.IAccessContext;
import com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class ContextUtils extends AbstractModule  {
	private static IAccessContext accessContext = null;
	private final String ACCESS_CONTEXT_CONFIG_NAME = "access_manager.service";
	private final Logger logger = Logger.getLogger(this.getClass().getName());
	
	/**
	 * The first time this class is called it will create the access context
	 */
	static {
		Injector injector = Guice.createInjector(new ContextUtils());
		accessContext = injector.getInstance(IAccessContext.class);
	}
	
	/** Returns the configured context object, for use in modules not part of the Aleph2 dependency injection
	 * @param signature can either be the fully qualified class name, or "<FQ class name>:arbitrary_config_string", which is then passed to the context via IHarvestContext.initializeNewContext 
	 * @return
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 */
	public static @NonNull IHarvestContext getHarvestContext(@NonNull String signature) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		String[] clazz_and_config = signature.split(":", 2);
		@SuppressWarnings("unchecked")
		Class<IHarvestContext> harvest_clazz = (Class<IHarvestContext>) Class.forName(clazz_and_config[0]);
		IHarvestContext context = harvest_clazz.newInstance();
		if (clazz_and_config.length > 1) {
			context.initializeNewContext(clazz_and_config[1]);
		}
		return context;
	}
	//TODO getAnalyticsContext
	
	/**
	 * Kicks off the injection by reading which access context we
	 * should bind via the config file and creating an instance of that context.
	 * 
	 * @return
	 */
	public static IAccessContext getAccessContext() {
		return accessContext;
	}
	
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
	private void loadAccessContextFromConfig(Config config) throws Exception {
		String service = config.getString(ACCESS_CONTEXT_CONFIG_NAME);
		//Convert string class name to actual class and bind to IAccessContext
		@SuppressWarnings("unchecked")
		Class<? extends IAccessContext> class2 = (Class<? extends IAccessContext>) Class.forName((String) service);
		logger.fine("Binding IAccessContext to " + class2.getCanonicalName());
		bind(IAccessContext.class).to(class2);
	}
}
