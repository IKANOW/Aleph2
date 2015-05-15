package com.ikanow.aleph2.management_db.services;

import akka.actor.ActorSystem;

/** Possibly temporary class to provide minimal actor context, pending moving to Guice
 * @author acp
 */
public class ManagementDbActorContext {

	
	/** Creates a new actor context (should only be called once_
	 */
	protected ManagementDbActorContext() {
		_actor_system = ActorSystem.create("com.ikanow.aleph2.management_db.services.CoreManagementDbService");
	}

	/** Returns the global actor system for the core management db service
	 * @return the actor system
	 */
	public ActorSystem getActorSystem() {
		return _actor_system;
	}
	
	/** Gets the actor context
	 * @return the actor context
	 */
	public static ManagementDbActorContext get() {
		if (null == _singleton) {
			_singleton = new ManagementDbActorContext();
		}
		return _singleton;		
	}
	
	protected final ActorSystem _actor_system;
	protected static ManagementDbActorContext _singleton = null;
	
}
