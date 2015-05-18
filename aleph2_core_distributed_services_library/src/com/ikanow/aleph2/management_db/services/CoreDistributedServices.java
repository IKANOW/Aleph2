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
package com.ikanow.aleph2.management_db.services;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICoreDistributedServices;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IExtraDependencyLoader;
import com.ikanow.aleph2.management_db.data_model.DistributedServicesPropertyBean;
import com.ikanow.aleph2.management_db.modules.CoreDistributedServicesModule;

/** Implementation class for full Curator service
 * @author acp
 *
 */
public class CoreDistributedServices implements ICoreDistributedServices, IExtraDependencyLoader {

	protected final CuratorFramework _curator_framework;
	
	/** Guice-invoked constructor
	 * @throws Exception 
	 */
	@Inject
	public CoreDistributedServices(@NonNull DistributedServicesPropertyBean config_bean) throws Exception {
		String connection_string = Optional.ofNullable(config_bean.zookeeper_connection())
									.orElse(DistributedServicesPropertyBean.__DEFAULT_ZOOKEEPER_CONNECTION);
		
		RetryPolicy retry_policy = new ExponentialBackoffRetry(1000, 3);
		_curator_framework = CuratorFrameworkFactory.newClient(connection_string, retry_policy);
		_curator_framework.start();		
	}
	
	/** Returns a connection to the Curator server
	 * @return
	 */
	@NonNull
	public CuratorFramework getCuratorFramework() {
		return _curator_framework;
	}

	/** Pass the local bindings module to the parent
	 * @return
	 */
	public List<AbstractModule> getExtraDependencyModules() {
		return Arrays.asList(new CoreDistributedServicesModule());
	}
	
	@Override
	public void youNeedToImplementTheStaticFunctionCalled_getExtraDependencyModules() {
		// done!
		
	}
}
