package com.ikanow.aleph2.management_db.services;

import java.util.Optional;

import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.management_db.controllers.actors.BucketActionSupervisor;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage.BucketActionEventBusWrapper;
import com.ikanow.aleph2.management_db.utils.ActorUtils;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.event.japi.LookupEventBus;

/** Possibly temporary class to provide minimal actor context, pending moving to Guice
 * @author acp
 */
public class ManagementDbActorContext {
	
	/** Creates a new actor context
	 */
	@Inject
	public ManagementDbActorContext(final IServiceContext service_context)
	{
		_service_context = service_context;
		_distributed_services = service_context.getService(ICoreDistributedServices.class, Optional.empty()).get();
		_bucket_action_bus = _distributed_services.getBroadcastMessageBus(BucketActionEventBusWrapper.class, BucketActionMessage.class, ActorUtils.BUCKET_ACTION_EVENT_BUS);
		_streaming_enrichment_bus = _distributed_services.getBroadcastMessageBus(BucketActionEventBusWrapper.class, BucketActionMessage.class, ActorUtils.STREAMING_ENRICHMENT_EVENT_BUS);
		_singleton = this;
	}

	/** Returns the global service context
	 * @return the global service context
	 */
	public IServiceContext getServiceContext() {
		return _service_context;
	}
	
	/** Returns the global actor system for the core management db service
	 * @return the actor system
	 */
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
	public LookupEventBus<BucketActionEventBusWrapper, ActorRef, String> getBucketActionMessageBus() {
		return _bucket_action_bus;
	}
	
	/** Returns a static accessor to the streaming enrichment message bus
	 * @return the streaming enrichment message bus
	 */
	public LookupEventBus<BucketActionEventBusWrapper, ActorRef, String> getStreamingEnrichmentMessageBus() {
		return _streaming_enrichment_bus;
	}
	
	/** Returns a static accessor to the designated message bus
	 * @return the designated message bus
	 */
	public LookupEventBus<BucketActionEventBusWrapper, ActorRef, String> getMessageBus(final String bus_name) {
		if (bus_name.equals(ActorUtils.STREAMING_ENRICHMENT_EVENT_BUS)) {
			return _streaming_enrichment_bus;
		}
		else if (bus_name.equals(ActorUtils.BUCKET_ACTION_EVENT_BUS)) {
			return _bucket_action_bus;
		}
		else {
			throw new RuntimeException(""); //TODO
		}
	}
	
	/** Returns the various distributed services present 
	 * @return the distributed services
	 */
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
	protected final LookupEventBus<BucketActionEventBusWrapper, ActorRef, String> _bucket_action_bus;
	protected final LookupEventBus<BucketActionEventBusWrapper, ActorRef, String> _streaming_enrichment_bus;
	protected final IServiceContext _service_context;
}
