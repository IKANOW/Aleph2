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
import com.ikanow.aleph2.management_db.controllers.actors.BucketDeletionActor;
import com.ikanow.aleph2.management_db.controllers.actors.BucketDeletionSingletonActor;
import com.ikanow.aleph2.management_db.controllers.actors.BucketTestCycleSingletonActor;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage.BucketActionEventBusWrapper;
import com.ikanow.aleph2.management_db.data_model.BucketMgmtMessage;
import com.ikanow.aleph2.management_db.data_model.BucketMgmtMessage.BucketMgmtEventBusWrapper;
import com.ikanow.aleph2.management_db.utils.ActorUtils;
import com.ikanow.aleph2.management_db.utils.ManagementDbErrorUtils;










import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.event.japi.LookupEventBus;

/** Possibly temporary class to provide minimal actor context, pending moving to Guice
 *  (Note: has some ugly singleton/mutable code in here - because it's not part of Guice it gets constructed (currently) 3 times:
 *   - manually from V1SyncModule (?! need to address that at some point), and from the DataBucketCrud(status)Service objects via guice injection
 *   This class is a good illustration of why we need to inject guice into Akka! 
 * @author acp
 */
public class ManagementDbActorContext {
	
	protected static final SetOnce<ActorRef> _bucket_action_supervisor = new SetOnce<>();
	protected static final SetOnce<ManagementDbActorContext> _singleton = new SetOnce<>();
	
	protected final IServiceContext _service_context;
	protected final ICoreDistributedServices _distributed_services;
	
	protected final SetOnce<LookupEventBus<BucketActionEventBusWrapper, ActorRef, String>> _bucket_action_bus;
	protected final SetOnce<LookupEventBus<BucketActionEventBusWrapper, ActorRef, String>> _streaming_enrichment_bus;
	protected final SetOnce<LookupEventBus<BucketMgmtEventBusWrapper, ActorRef, String>> _delete_round_robin_bus;
	
	// Some mutable state just used for cleaning up in tests
	private Optional<ActorRef> _delete_singleton = Optional.empty();
	private Optional<ActorRef> _delete_worker = Optional.empty();
	private Optional<ActorRef> _test_singleton = Optional.empty();
	
	/** Creates a new actor context
	 */
	@Inject
	public ManagementDbActorContext(final IServiceContext service_context)
	{
		boolean first_time = false; //(WARNING: internal mutable state, v briefly used because of sync clause)
		synchronized (_singleton) {
			if (!_singleton.isSet()) {
				first_time = true;
				_singleton.set(this);
			}
		}
		if (first_time) { // First time - actually create the object			
			_service_context = service_context;
			_distributed_services = service_context.getService(ICoreDistributedServices.class, Optional.empty()).get();
			
			_bucket_action_bus = new SetOnce<>();
			_streaming_enrichment_bus = new SetOnce<>();
			_delete_round_robin_bus = new SetOnce<>();
			
			_distributed_services.getApplicationName()
			.filter(name -> name.equals(DistributedServicesPropertyBean.ApplicationNames.DataImportManager.toString()))
			.ifPresent(__ -> {
				_distributed_services.runOnAkkaJoin(() -> {
					_delete_singleton = _distributed_services.createSingletonActor(ActorUtils.BUCKET_TEST_CYCLE_SINGLETON_ACTOR, 
							ImmutableSet.<String>builder().add(DistributedServicesPropertyBean.ApplicationNames.DataImportManager.toString()).build(), 
							Props.create(BucketTestCycleSingletonActor.class));
					_test_singleton = _distributed_services.createSingletonActor(ActorUtils.BUCKET_DELETION_SINGLETON_ACTOR, 
							ImmutableSet.<String>builder().add(DistributedServicesPropertyBean.ApplicationNames.DataImportManager.toString()).build(), 
							Props.create(BucketDeletionSingletonActor.class));
		
					// subscriber one worker per node
					_delete_worker = Optional.of(_distributed_services.getAkkaSystem().actorOf(Props.create(BucketDeletionActor.class), ActorUtils.BUCKET_DELETION_WORKER_ACTOR));
				});
			});
		}
		else { // Just a copy of the object (note only mutable state are the Optionals, only used for tests, these can be ignored):
			_service_context = _singleton.get()._service_context;
			_distributed_services = _singleton.get()._distributed_services;
			
			_bucket_action_bus = _singleton.get()._bucket_action_bus;
			_streaming_enrichment_bus = _singleton.get()._streaming_enrichment_bus;
			_delete_round_robin_bus = _singleton.get()._delete_round_robin_bus;			
		}		
	}

	/** Intended for testing: removes the singleton actors before the test/session shuts down
	 *  (note will only work with MCDS because true singletons don't currently return an addressable ActorRef)
	 */
	public void onTestComplete() {
		_delete_singleton.ifPresent(actor -> actor.tell(akka.actor.PoisonPill.getInstance(), actor));
		_test_singleton.ifPresent(actor -> actor.tell(akka.actor.PoisonPill.getInstance(), actor));
		_delete_worker.ifPresent(actor -> actor.tell(akka.actor.PoisonPill.getInstance(), actor));
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
		synchronized (_singleton) {
			if (!_bucket_action_supervisor.isSet()) {
				_bucket_action_supervisor.set(_distributed_services.getAkkaSystem().actorOf(Props.create(BucketActionSupervisor.class), ActorUtils.BUCKET_ACTION_SUPERVISOR));
			}
			return _bucket_action_supervisor.get();
		}
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
	
	/** Returns a static accessor to the deletion round robin message bus
	 * @return the deletion round robin message bus
	 */
	public synchronized LookupEventBus<BucketMgmtEventBusWrapper, ActorRef, String> getDeletionMgmtBus() {
		if (!_delete_round_robin_bus.isSet()) {
			_delete_round_robin_bus.set(_distributed_services.getRoundRobinMessageBus(BucketMgmtEventBusWrapper.class, BucketMgmtMessage.class, ActorUtils.BUCKET_DELETION_BUS));
		}
		return _delete_round_robin_bus.get();
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
		return _singleton.get();		
	}
}
