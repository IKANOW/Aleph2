package com.ikanow.aleph2.management_db.services;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICoreDistributedServices;

import akka.actor.ActorSystem;

/** Possibly temporary class to provide minimal actor context, pending moving to Guice
 * @author acp
 */
public class ManagementDbActorContext {

	
	/** Creates a new actor context
	 */
	@Inject
	protected ManagementDbActorContext(ICoreDistributedServices distributed_services) {
		_distributed_services = distributed_services;
		if (null == _actor_system) {
			_actor_system = ActorSystem.create("com.ikanow.aleph2.management_db.services.CoreManagementDbService");
		}
		_singleton = this;
	}

	/** Returns the global actor system for the core management db service
	 * @return the actor system
	 */
	@NonNull
	public ActorSystem getActorSystem() {
		return _actor_system;
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
	public static ManagementDbActorContext get() {
		// (This will only not be set if guice injection has failed, in which case there are deeper problems...)
		return _singleton;		
	}
	
	protected static ActorSystem _actor_system;
	protected static ManagementDbActorContext _singleton = null;
	protected final ICoreDistributedServices _distributed_services;
}
