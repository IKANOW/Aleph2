package com.ikanow.aleph2.management_db.services;

import java.util.Optional;






import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.SetOnce;
import com.ikanow.aleph2.distributed_services.data_model.DistributedServicesPropertyBean;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.management_db.controllers.actors.BucketActionSupervisor;
import com.ikanow.aleph2.management_db.controllers.actors.BucketDeletionSingletonActor;
import com.ikanow.aleph2.management_db.controllers.actors.BucketTestCycleSingletonActor;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage.BucketActionEventBusWrapper;
import com.ikanow.aleph2.management_db.utils.ActorUtils;
import com.ikanow.aleph2.management_db.utils.ManagementDbErrorUtils;






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
		_singleton = this;
		
		_distributed_services.runOnAkkaJoin(() -> {
			_distributed_services.createSingletonActor(ActorUtils.BUCKET_TEST_CYCLE_SINGLETON_ACTOR, 
					ImmutableSet.<String>builder().add(DistributedServicesPropertyBean.ApplicationNames.DataImportManager.toString()).build(), 
					Props.create(BucketTestCycleSingletonActor.class));
			_distributed_services.createSingletonActor(ActorUtils.BUCKET_DELETION_SINGLETON_ACTOR, 
					ImmutableSet.<String>builder().add(DistributedServicesPropertyBean.ApplicationNames.DataImportManager.toString()).build(), 
					Props.create(BucketDeletionSingletonActor.class));
		});
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
	
	public synchronized ActorRef getBucketActionSupervisor() {
		if (null == _bucket_action_supervisor) {
			_bucket_action_supervisor = _distributed_services.getAkkaSystem().actorOf(Props.create(BucketActionSupervisor.class), ActorUtils.BUCKET_ACTION_SUPERVISOR);
		}
		return _bucket_action_supervisor;
	}

	/** Returns a static accessor to the bucket action message bus
	 * @return the bucket action message bus
	 */
	public synchronized LookupEventBus<BucketActionEventBusWrapper, ActorRef, String> getBucketActionMessageBus() {
		if (!_bucket_action_bus.isSet()) {
			_bucket_action_bus.set(_distributed_services.getBroadcastMessageBus(BucketActionEventBusWrapper.class, BucketActionMessage.class, ActorUtils.BUCKET_ACTION_EVENT_BUS));
		}
		return _bucket_action_bus.get();
	}
	
	/** Returns a static accessor to the streaming enrichment message bus
	 * @return the streaming enrichment message bus
	 */
	public synchronized LookupEventBus<BucketActionEventBusWrapper, ActorRef, String> getStreamingEnrichmentMessageBus() {
		if (!_streaming_enrichment_bus.isSet()) {
			_streaming_enrichment_bus.set(_distributed_services.getBroadcastMessageBus(BucketActionEventBusWrapper.class, BucketActionMessage.class, ActorUtils.STREAMING_ENRICHMENT_EVENT_BUS));
		}
		return _streaming_enrichment_bus.get();
	}
	
	/** Returns a static accessor to the designated message bus
	 * @return the designated message bus
	 */
	public LookupEventBus<BucketActionEventBusWrapper, ActorRef, String> getMessageBus(final String bus_name) {
		if (bus_name.equals(ActorUtils.STREAMING_ENRICHMENT_EVENT_BUS)) {
			return getStreamingEnrichmentMessageBus();
		}
		else if (bus_name.equals(ActorUtils.BUCKET_ACTION_EVENT_BUS)) {
			return getBucketActionMessageBus();
		}
		else {
			throw new RuntimeException(ErrorUtils.get(ManagementDbErrorUtils.NO_SUCH_MESSAGE_BUS, bus_name));
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
	protected final SetOnce<LookupEventBus<BucketActionEventBusWrapper, ActorRef, String>> _bucket_action_bus = new SetOnce<>();
	protected final SetOnce<LookupEventBus<BucketActionEventBusWrapper, ActorRef, String>> _streaming_enrichment_bus = new SetOnce<>();
	protected final IServiceContext _service_context;
}
