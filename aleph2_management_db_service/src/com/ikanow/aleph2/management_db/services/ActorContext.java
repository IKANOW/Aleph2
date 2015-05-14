package com.ikanow.aleph2.management_db.services;

import akka.actor.ActorSystem;

public class ActorContext {

	//TODO java doc
	
	protected static ActorContext _singleton = null;
	
	protected ActorContext() {
		_actor_system = ActorSystem.create("com.ikanow.aleph2.management_db.services.CoreManagementDbService");
	}

	protected ActorSystem getActorSystem() {
		return _actor_system;
	}
	
	protected final ActorSystem _actor_system;
	
	public static ActorContext get() {
		if (null == _singleton) {
			_singleton = new ActorContext();
		}
		return _singleton;		
	}
}
