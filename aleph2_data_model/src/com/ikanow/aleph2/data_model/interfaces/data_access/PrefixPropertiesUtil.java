package com.ikanow.aleph2.data_model.interfaces.data_access;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.Properties;
import java.util.Set;

/**
 * Reads properties file in and sorts into a map based on a prefix.suffix.  If a field does not
 * have a period, will be put into the map as prefix = { "" : value }.  Can be called
 * multiple times with sub properties to get nested properties
 * 
 * e.g. config example:
 * prefix.intermediate_prefix1.suffix = value		1st PrefixPropertiesUtil: prefix = { intermediate_prefix1.suffix : value }
 * 													2nd PrefixPropertiesUtil(prefix): intermediate_prefix1 = { suffix : value }
 * 
 * @author Burch
 *
 */
public class PrefixPropertiesUtil {
	private final Logger logger = Logger.getLogger(PrefixPropertiesUtil.class.getName());
	public Map<String, Map<String, String>> properties_map;
	
	public PrefixPropertiesUtil(String property_file_path) throws FileNotFoundException, IOException {
		properties_map = new HashMap<String, Map<String, String>>();
		Properties properties = new Properties();
		properties.load(new FileInputStream(property_file_path));
		Map<String,String> map = properties.entrySet().stream()
			.collect(Collectors.toMap(entry -> (String)entry.getKey(), entry -> (String)entry.getValue()));
		loadProperties(map.entrySet());
	}
	
	public PrefixPropertiesUtil(Map<String, String> map) {
		properties_map = new HashMap<String, Map<String, String>>();
		loadProperties(map.entrySet());
	}

	private void loadProperties(Set<Entry<String,String>> properties) {
		properties.stream()
			.forEach( entry -> {
				String key = (String)entry.getKey(); 
				String prefix;
				String suffix;
				if ( key.contains(".") )
				{
					prefix = key.substring(0, key.indexOf("."));
					suffix = key.substring(key.indexOf(".")+1);					
				}
				else
				{
					prefix = key;
					suffix = ""; //empty value
				}
				logger.fine("storing: " + prefix + " " + suffix + " " + (String)entry.getValue());
				Map<String, String> values = properties_map.get(prefix);
				if ( values == null )
					values = new HashMap<String,String>();
				values.put(suffix, (String)entry.getValue());
				properties_map.put(prefix, values);
			});
	}
	
	public Map<String, String> getProperties(String prefix) {
		return properties_map.get(prefix);
	}
	
	public Map<String, Map<String, String>> getAllProperties() {
		return properties_map;
	}
	
}
