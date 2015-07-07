/*******************************************************************************
* Copyright 2015, The IKANOW Open Source Project.
* 
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License, version 3,
* as published by the Free Software Foundation.
* 
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Affero General Public License for more details.
* 
* You should have received a copy of the GNU Affero General Public License
* along with this program. If not, see <http://www.gnu.org/licenses/>.
******************************************************************************/
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
