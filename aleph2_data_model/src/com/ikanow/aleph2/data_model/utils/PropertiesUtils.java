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
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;


import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.ikanow.aleph2.data_model.objects.shared.ConfigDataServiceEntry;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigObject;

/**
 * Utility functions related to accessing the properties file
 * 
 * @author Burch
 *
 */
public class PropertiesUtils {	
	
	/** Returns the sub-config of a config, or empty if
	 * @param config the configuration object
	 * @param key the root key of the sub object
	 * @return
	 */
	public static Optional<Config> getSubConfig(final Config config, final String key) {
		try {
			return Optional.of(config.getConfig(key));
		}
		catch (Exception e) {
			return Optional.empty();
		}
	}
	
	/** Returns the sub-object of a config, or empty if
	 * @param config the configuration object
	 * @param key the root key of the sub object
	 * @return
	 */
	public static Optional<ConfigObject> getSubConfigObject(final Config config, final String key) {
		try {
			return Optional.of(config.getObject(key));
		}
		catch (Exception e) {
			return Optional.empty();
		}
	}
	
	/**
	 * Helper function that returns a config value or the default if it can't be found (used for strings)
	 * 
	 * @param config
	 * @param key
	 * @param defaultValue
	 * @return
	 */
	private static String getConfigValue(final Config config, final String key, final String defaultValue) {
		String value;
		try {
			value = config.getString(key);
		} catch (ConfigException ex) {
			value = defaultValue;
		}
		return value;
	}
	
	/**
	 * Helper function that returns a config value or the default if it can't be found (used for booleans)
	 * 
	 * @param config
	 * @param key
	 * @param defaultValue
	 * @return
	 */
	private static boolean getConfigValue(final Config config, final String key, final boolean defaultValue) {
		boolean value = defaultValue;
		try {
			value = config.getBoolean(key);
		} catch (ConfigException ex) {
			value = defaultValue;
		}
		return value;
	}

	/**
	 * Reads in the config file and sends back a list of a config data service entries based on the
	 * given configPrefix.  This is used by the ModuleUtils to get the requested modules to load.
	 * 
	 * @param config
	 * @param configPrefix
	 * @return
	 * @throws IOException 
	 * @throws JsonMappingException 
	 * @throws JsonParseException 
	 */
	public static List<ConfigDataServiceEntry> getDataServiceProperties(final Config config, final String configPrefix) {
		Optional<ConfigObject> sub_config = getSubConfigObject(config, configPrefix);
		if ( sub_config.isPresent() ) {
			final ConfigObject dataServiceConfig = sub_config.get(); //config.getObject(configPrefix);			
			List<ConfigDataServiceEntry> toReturn = dataServiceConfig.entrySet()
				.stream()
				.map(entry -> 
					new ConfigDataServiceEntry(
						entry.getKey(), 
						Optional.ofNullable(PropertiesUtils.getConfigValue(config, configPrefix+"."+entry.getKey()+".interface", null)), 
						PropertiesUtils.getConfigValue(config, configPrefix+"."+entry.getKey()+".service", null),
						PropertiesUtils.getConfigValue(config, configPrefix+"."+entry.getKey()+".default", false))
				)
				.collect( Collectors.toList());		
			return toReturn;
		} else {
			return Collections.emptyList();
		}
	}
}
