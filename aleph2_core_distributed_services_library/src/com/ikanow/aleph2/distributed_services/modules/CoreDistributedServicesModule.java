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
package com.ikanow.aleph2.distributed_services.modules;

import java.io.File;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.data_model.utils.PropertiesUtils;
import com.ikanow.aleph2.distributed_services.data_model.DistributedServicesPropertyBean;
import com.ikanow.aleph2.distributed_services.utils.ZookeeperUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;

public class CoreDistributedServicesModule extends AbstractModule {

	/** User constructor when called from StandaloneModuleManagement/the app - will just load the core service
	 * Also the guice constructor in this case
	 */
	@Inject
	public CoreDistributedServicesModule() {
	}

	/* (non-Javadoc)
	 * @see com.google.inject.AbstractModule#configure()
	 */
	@Override
	protected void configure() {
		
		final Config config = ModuleUtils.getStaticConfig();				
		
		try {
			final DistributedServicesPropertyBean bean = Lambdas.wrap_u(() -> { 
					
				final DistributedServicesPropertyBean tmp_bean = BeanTemplateUtils.from(
						PropertiesUtils.getSubConfig(config, DistributedServicesPropertyBean.PROPERTIES_ROOT).orElse(null), 
						DistributedServicesPropertyBean.class);
			
				if (null == tmp_bean.zookeeper_connection()) { //try getting it from zoo.cfg)
					final File f = new File(ModuleUtils.getGlobalProperties().local_yarn_config_dir() + File.separator + "zoo.cfg");
					if (f.exists() && f.canRead()) {
						return BeanTemplateUtils.clone(tmp_bean).with(DistributedServicesPropertyBean::zookeeper_connection, 
								ZookeeperUtils.buildConnectionString(
										ConfigFactory.parseFile(f, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.PROPERTIES)))).done();
					}
					else return tmp_bean;
				}
				else return tmp_bean;
			}).get();
			
			this.bind(DistributedServicesPropertyBean.class).toInstance(bean);
		} 
		catch (Exception e) {
			throw new RuntimeException(ErrorUtils.get(ErrorUtils.INVALID_CONFIG_ERROR,
					DistributedServicesPropertyBean.class.toString(),
					config.getConfig(DistributedServicesPropertyBean.PROPERTIES_ROOT)
					), e);
		}
	}

}
