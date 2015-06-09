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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
/**
 * 
 * @author Joern Freydank (jfreydank@ikanow.com)
 *
 */
public class PrefixPropertyUtils {

	static final Logger logger = LogManager.getLogger(PrefixPropertyUtils.class);

	public static void setProperty(Properties properties, Object target, String propertyName, String fieldName, String prefix) {
		if (properties != null && target != null && propertyName != null) {
			String matchKey = prefix != null ? prefix + "." + propertyName : propertyName;
			matchKey = matchKey.toUpperCase();
			for (Iterator<Entry<Object, Object>> it = properties.entrySet().iterator(); it.hasNext();) {
				Entry<Object, Object> entry = it.next();
				if (entry.getKey().toString().toUpperCase().startsWith(matchKey)) {
					try {
						String attributeName = fieldName != null ? fieldName : propertyName;
						BeanUtils.setProperty(target, attributeName, entry.getValue());
					} catch (Exception e) {
						logger.error("setProperty caught exception", e);
					}
					break;
				} // if
			} // for
		} //
	} // setProperty

	public static void setProperty(Properties properties, Object target, String propertyName, String prefix) {
		setProperty(properties, target, propertyName, null, prefix);
	}

	public static void setProperty(Properties properties, Object target, String propertyName) {
		setProperty(properties, target, propertyName, null, null);
	}
	
	/** 
	 * This method splits properties into a map wjhere each entry consists of the prefix and the correspondign set.
	 * Properties without prefix are copied into the entry. Additonally the prefixed property names are stipped and also copied into the entry.
	 * @param properties Map<String,Properties>
	 * @return
	 */
	public static Map<String, Properties> splitPropertiesByPrefix(Properties properties) {
		Map<String, Properties> propMap = new HashMap<String, Properties>();		
		if (properties != null) {
			// extract nonPrefixProps props
			Map<Object, Object> nonPrefixProps = new HashMap<Object, Object>();		
			for (Iterator<Entry<Object, Object>> it = properties.entrySet().iterator(); it.hasNext();) {
				Entry<Object, Object> entry = it.next();
				String propName = entry.getKey().toString();
				if (!propName.contains(".")) {
					nonPrefixProps.put(entry.getKey(), entry.getValue());
				}
			} // for
			
			// map prefix properties
			for (Iterator<Entry<Object, Object>> it = properties.entrySet().iterator(); it.hasNext();) {
				Entry<Object, Object> entry = it.next();
				String propName = entry.getKey().toString();
				int dotIndex = propName.indexOf(".");
				if (dotIndex>-1) {
					String prefix = propName.substring(0,dotIndex);
					String restPropName = propName.substring(dotIndex+1);
					Properties p = propMap.get(prefix);
					if(p==null){
						p = new Properties();
						// add nonPrefix properties
						p.putAll(nonPrefixProps);
						propMap.put(prefix, p);
					} // if
					p.put(entry.getKey(), entry.getValue());
					p.put(restPropName, entry.getValue());
				}
			} // for

		} // if !=null
		return propMap;
	}

	/** 
	 * This method sets potentially all properties. 
	 * @param properties
	 * @param target
	 */
	public static void setAllProperties(Properties properties, Object target, boolean prefixOnly) {
		if (properties != null && target != null ) {
			for (Iterator<Entry<Object, Object>> it = properties.entrySet().iterator(); it.hasNext();) {
				Entry<Object, Object> entry = it.next();
				String propName = entry.getKey().toString();
				if (!prefixOnly || propName.contains(".")) {
					try {
						String attributeName = propName.contains(".")? propName.substring(propName.indexOf(".")+1) : propName;
						BeanUtils.setProperty(target, attributeName, entry.getValue());
					} catch (Exception e) {
						logger.error("setAllProperties caught exception", e);
					}
				} // if
			} // for
		} //
	} // setProperty
	
}