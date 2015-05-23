package com.ikanow.aleph2.management_db.services;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.management_db.controllers.actors.BucketActionSupervisor;
import com.ikanow.aleph2.management_db.utils.ActorUtils;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

/** Possibly temporary class to provide minimal actor context, pending moving to Guice
 * @author acp
 */
public class ManagementDbActorContext {
	
	/** Creates a new actor context
	 */
	@Inject
	public ManagementDbActorContext(ICoreDistributedServices distributed_services, 
								IServiceContext service_context,
										BucketActionMessageBus bucket_action_message_bus)
	{
		_service_context = service_context;
		_distributed_services = distributed_services;
		_bucket_action_bus = bucket_action_message_bus;
		_singleton = this;
	}

	/** Returns the global service context
	 * @return the global service context
	 */
	@NonNull
	public IServiceContext getServiceContext() {
		return _service_context;
	}
	
	/** Returns the global actor system for the core management db service
	 * @return the actor system
	 */
	@NonNull
	public ActorSystem getActorSystem() {
		return _distributed_services.getAkkaSystem();
	}
	
	public ActorRef getBucketActionSupervisor() {
		if (null == _bucket_action_supervisor) {
			_bucket_action_supervisor = _distributed_services.getAkkaSystem().actorOf(Props.create(BucketActionSupervisor.class), ActorUtils.BUCKET_ACTION_SUPERVISOR);
		}
		return _bucket_action_supervisor;
	}

	/** Returns a static accessor to the bucket action message bus
	 * @return the bucket action message bus
	 */
	public BucketActionMessageBus getBucketActionMessageBus() {
		return _bucket_action_bus;
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
	
	protected static ActorRef _bucket_action_supervisor;
	protected static ManagementDbActorContext _singleton = null;
	protected final ICoreDistributedServices _distributed_services;
	protected final BucketActionMessageBus _bucket_action_bus;
	protected final IServiceContext _service_context;
}
