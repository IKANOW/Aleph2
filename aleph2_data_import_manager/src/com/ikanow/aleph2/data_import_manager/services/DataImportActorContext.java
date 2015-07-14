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

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_import.services.HarvestContext;
import com.ikanow.aleph2.data_import.services.StreamingEnrichmentContext;
import com.ikanow.aleph2.data_import_manager.stream_enrichment.services.IStormController;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;

import akka.actor.ActorSystem;

/** Possibly temporary class to provide minimal actor context, pending moving to Guice
 * @author acp
 */
public class DataImportActorContext {
	protected static DataImportActorContext _singleton = null;
	protected final ICoreDistributedServices _distributed_services;
	protected final IServiceContext _service_context;
	protected final GeneralInformationService _information_service;
	protected final IStormController _storm_controller;
	
	@Inject 
	protected Injector _injector; // (used to generate harvest contexts)
	
	/** Creates a new actor context
	 */
	@Inject
	public DataImportActorContext(final IServiceContext service_context, final GeneralInformationService information_service, final IStormController storm_controller)
	{
		_service_context = service_context;
		_distributed_services = service_context.getService(ICoreDistributedServices.class, Optional.empty()).get();
		_singleton = this;
		_information_service = information_service;
		_storm_controller = storm_controller;
	}

	/** Returns the global properties bean
	 * @return the global properties bean
	 */
	public GlobalPropertiesBean getGlobalProperties() {
		return _service_context.getGlobalProperties();
	}
	
	/** Returns a new (non singleton) instance of a streaming enrichment context
	 * @return the new harvest context
	 */
	public StreamingEnrichmentContext getNewStreamingEnrichmentContext() {
		return _injector.getInstance(StreamingEnrichmentContext.class);
	}
	
	/** Returns a new (non singleton) instance of a harvest context
	 * @return the new harvest context
	 */
	public HarvestContext getNewHarvestContext() {
		return _injector.getInstance(HarvestContext.class);
	}
	
	/** Returns the information service providing eg hostname and process information
	 * @return
	 */
	public GeneralInformationService getInformationService() {
		return _information_service;
	}
	
	/** Returns the global service context
	 * @return the global service context
	 */
	public IServiceContext getServiceContext() {
		return _service_context;
	}
	
	/** Returns the global actor system for the data import manager
	 * @return the actor system
	 */
	public ActorSystem getActorSystem() {
		return _distributed_services.getAkkaSystem();
	}
	
	/** Returns the various distributed services present 
	 * @return the distributed services
	 */
	public ICoreDistributedServices getDistributedServices() {
		return _distributed_services;
	}
	
	/**
	 * Returns the storm controller
	 * @return the storm controller
	 */
	public IStormController getStormController() {
		return _storm_controller;
	}
	
	/** Gets the actor context
	 * @return the actor context
	 */
	public static DataImportActorContext get() {
		// (This will only not be set if guice injection has failed, in which case there are deeper problems...)
		return _singleton;		
	}
}
