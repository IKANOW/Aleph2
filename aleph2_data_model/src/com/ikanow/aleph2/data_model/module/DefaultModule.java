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
package com.ikanow.aleph2.data_model.module;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * 
 * @author Joern Freydank (jfreydank@ikanow.com)
 *
 */
public class DefaultModule  {
	protected String configFileName = "";
	protected Properties properties = null;
	protected String jsonStyleProperties;

	private static final Logger logger = Logger.getLogger(DefaultModule.class);

	public DefaultModule(String jsonStyleProperties){
		this.jsonStyleProperties = jsonStyleProperties;
	}

	public DefaultModule(){
		this(null);
	}
	
	protected Properties getProperties() {
		if (this.properties == null) {
			this.properties = new Properties();

			String configPath = System.getProperty("configPath", "module.properties");
			String propertiesFileName = configPath + "/"+configFileName;
			try {
				InputStream inStream = DefaultModule.class.getResourceAsStream(propertiesFileName);
				if (inStream == null) {
					// second try
					inStream = new FileInputStream(propertiesFileName);
				}
				if (inStream != null) {
					properties.load(inStream);						
				}

			} catch (Throwable t) {
				logger.error("Caught exception loading properties:", t);
			}
			// json Style properties override the properties in the properties file
			if(jsonStyleProperties!=null){
				jsonStyleProperties = jsonStyleProperties.replace("{", "");
				jsonStyleProperties = jsonStyleProperties.replace("}", "");
				for (String nvPair : jsonStyleProperties.split(",")){
					String[] propValues = nvPair.split(":", 2);
					
					if(propValues!=null && propValues.length>1){
						String propName =  propValues[0].trim();
						// remove " from beginning and end
						propName = propName.substring(1,propName.length()-1);
						String propValue =  propValues[1].trim();
						propValue = propValue.substring(1,propValue.length()-1);
						properties.setProperty(propName, propValue);
					}
				}
				
			}
		} // if null
		return this.properties;
	}

	/**
	 * @return the configFileName
	 */
	public String getConfigFileName() {
		return configFileName;
	}

	/**
	 * @param configFileName
	 *            the configFileName to set
	 */
	public void setConfigFileName(String configFileName) {
		this.configFileName = configFileName;
	}


}
