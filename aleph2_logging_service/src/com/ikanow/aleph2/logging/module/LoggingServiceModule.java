/*******************************************************************************
 * Copyright 2016, The IKANOW Open Source Project.
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
package com.ikanow.aleph2.logging.module;

import com.google.inject.AbstractModule;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.data_model.utils.PropertiesUtils;
import com.ikanow.aleph2.logging.data_model.LoggingServiceConfigBean;
import com.typesafe.config.Config;

/**
 * Just pulls in the config properties.
 * @author Burch
 *
 */
public class LoggingServiceModule extends AbstractModule {

	/* (non-Javadoc)
	 * @see com.google.inject.AbstractModule#configure()
	 */
	@Override
	protected void configure() {
		final Config config = ModuleUtils.getStaticConfig();				
		LoggingServiceConfigBean bean;
		try {
			bean = BeanTemplateUtils.from(PropertiesUtils.getSubConfig(config, LoggingServiceConfigBean.PROPERTIES_ROOT).orElse(null), LoggingServiceConfigBean.class);
		} 
		catch (Exception e) {
			throw new RuntimeException(ErrorUtils.get(ErrorUtils.INVALID_CONFIG_ERROR,
					LoggingServiceConfigBean.class.toString(),
					config.getConfig(LoggingServiceConfigBean.PROPERTIES_ROOT)
					), e);
		}
		
		this.bind(LoggingServiceConfigBean.class).toInstance(bean);
	}

}
