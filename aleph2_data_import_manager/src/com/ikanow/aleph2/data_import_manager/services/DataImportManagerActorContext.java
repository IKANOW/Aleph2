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
package com.ikanow.aleph2.data_import_manager.services;

import java.util.Optional;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.interfaces.data_access.IServiceContext;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;

import akka.actor.ActorSystem;

/** Possibly temporary class to provide minimal actor context, pending moving to Guice
 * @author acp
 */
public class DataImportManagerActorContext {
	
	/** Creates a new actor context
	 */
	@Inject
	public DataImportManagerActorContext(IServiceContext service_context)
	{
		_service_context = service_context;
		_distributed_services = service_context.getService(ICoreDistributedServices.class, Optional.empty());
		_singleton = this;
	}

	/** Returns the global properties bean
	 * @return the global properties bean
	 */
	@NonNull
	public GlobalPropertiesBean getGlobalProperties() {
		return _service_context.getGlobalProperties();
	}
	
	
	
	/** Returns the global service context
	 * @return the global service context
	 */
	@NonNull
	public IServiceContext getServiceContext() {
		return _service_context;
	}
	
	/** Returns the global actor system for the data import manager
	 * @return the actor system
	 */
	@NonNull
	public ActorSystem getActorSystem() {
		return _distributed_services.getAkkaSystem();
	}
	
	/** Returns the various distributed services present 
	 * @return the distributed services
	 */
	@NonNull
	public ICoreDistributedServices getDistributedServices() {
		return _distributed_services;
	}
	
	/** Gets the actor context
	 * @return the actor context
	 */
	public static DataImportManagerActorContext get() {
		// (This will only not be set if guice injection has failed, in which case there are deeper problems...)
		return _singleton;		
	}
	
	protected static DataImportManagerActorContext _singleton = null;
	protected final ICoreDistributedServices _distributed_services;
	protected final IServiceContext _service_context;
}
