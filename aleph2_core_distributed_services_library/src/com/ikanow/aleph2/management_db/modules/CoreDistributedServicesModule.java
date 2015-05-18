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
package com.ikanow.aleph2.management_db.modules;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.management_db.data_model.DistributedServicesPropertyBean;
import com.ikanow.aleph2.management_db.utils.ErrorUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

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
		
		Config config = ConfigFactory.load();				
		DistributedServicesPropertyBean bean;
		try {
			bean = BeanTemplateUtils.from(config.getConfig(DistributedServicesPropertyBean.PROPERTIES_ROOT), DistributedServicesPropertyBean.class);
		} catch (Exception e) {
			throw new RuntimeException(ErrorUtils.get(ErrorUtils.INVALID_CONFIG_ERROR,
					DistributedServicesPropertyBean.class.toString(),
					config.getConfig(DistributedServicesPropertyBean.PROPERTIES_ROOT)
					), e);
		}
		this.bind(DistributedServicesPropertyBean.class).toInstance(bean);
	}

}
