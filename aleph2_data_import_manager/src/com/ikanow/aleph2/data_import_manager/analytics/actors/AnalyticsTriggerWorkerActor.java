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
package com.ikanow.aleph2.data_import_manager.analytics.actors;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ikanow.aleph2.data_import_manager.analytics.utils.AnalyticTriggerCoreUtils;
import com.ikanow.aleph2.data_import_manager.analytics.utils.AnalyticTriggerUtils;
import com.ikanow.aleph2.data_import_manager.services.DataImportActorContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean.AnalyticThreadComplexTriggerBean.TriggerType;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.BucketUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Patterns;
import com.ikanow.aleph2.data_model.utils.SetOnce;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.management_db.controllers.actors.BucketActionSupervisor;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerMessage;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerStateBean;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerMessage.AnalyticsTriggerActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.services.ManagementDbActorContext;

import akka.actor.UntypedActor;

/** This actor is responsible for checking the state of the various active and inactive triggers in the system
 * @author Alex
 */
public class AnalyticsTriggerWorkerActor extends UntypedActor {
	protected static final Logger _logger = LogManager.getLogger();	

	public final static long ACTIVE_CHECK_FREQ_SECS = 10L;
	
	final DataImportActorContext _actor_context;
	final IServiceContext _service_context;
	final ICoreDistributedServices _distributed_services;
	
	public AnalyticsTriggerWorkerActor() {
		_actor_context = DataImportActorContext.get();
		_service_context = _actor_context.getServiceContext();
		_distributed_services = _service_context.getService(ICoreDistributedServices.class, Optional.empty()).get();
	}
	
	//TODO (ALEPH-12): Why am I locking on a per-bucket basic and not a per-job basis?!
	
	/* (non-Javadoc)
	 * @see akka.actor.UntypedActor#onReceive(java.lang.Object)
	 */
	@Override
	public void onReceive(Object message) throws Exception {		
		Patterns.match(message).andAct()
			// Bucket deletion
			.when(BucketActionMessage.DeleteBucketActionMessage.class, msg -> onBucketDelete(msg))
			
			// Test complete
			.when(BucketActionMessage.UpdateBucketActionMessage.class,
					msg -> !msg.is_enabled() && BucketUtils.isTestBucket(msg.bucket()),
						msg -> onBucketChanged(msg))
						
			// Other bucket update
			.when(BucketActionMessage.class, msg -> onBucketChanged(msg))
			
			// Regular trigger message
			.when(AnalyticTriggerMessage.class, 
					msg -> null != msg.trigger_action_message(), 
						msg -> onAnalyticTrigger(msg.trigger_action_message()))
						
			// Trigger event from elsewhere in the system
			.when(AnalyticTriggerMessage.class, 
					msg -> null != msg.bucket_action_message(), 
						msg -> onAnalyticBucketEvent(msg.bucket_action_message()))
			;		
	}	
	
	/** The bucket has changed (or been created) so update (or create) the relevant entries in the trigger state database
	 * @param message
	 */
	protected void onBucketChanged(final BucketActionMessage message) {
		_logger.info(ErrorUtils.get("Received analytic trigger request for bucket {0}", message.bucket().full_name()));
		
		// Create the state objects
		
		final Stream<AnalyticTriggerStateBean> state_beans = 
			AnalyticTriggerUtils.generateTriggerStateStream(message.bucket(), 
					Optional.ofNullable(message.bucket().multi_node_enabled())
										.filter(enabled -> enabled)
										.map(__ -> _actor_context.getInformationService().getHostname()));

		// Handle bucket collisions
		final Consumer<String> on_collision = path -> {			
			//TODO (ALEPH-12): store this to retry queue
			_logger.error(ErrorUtils.get("FAILED TO OBTAIN MUTEX FOR {0} THIS CURRENTLY RESULTS IN A SERIOUS LOGIC ERROR - NEED TO IMPLEMENT RETRY STRATEGY", message.bucket().full_name()));			
		};
		//(should be able to decrease this once we have a saner retry strategy)
		final Duration max_time_to_decollide = Duration.ofMinutes(2L); 

		Map<String, List<AnalyticTriggerStateBean>> triggers_in = state_beans.collect(
				Collectors.groupingBy(state -> state.bucket_name() + ":" + state.locked_to_host()));
		
		final SetOnce<Collection<String>> path_names = new SetOnce<>();
		try {
			// Grab the mutex
			final Map<String, List<AnalyticTriggerStateBean>> triggers = 
					AnalyticTriggerCoreUtils.registerOwnershipOfTriggers(triggers_in, 
							_actor_context.getInformationService().getProcessUuid(), _distributed_services.getCuratorFramework(),  
							Tuples._2T(max_time_to_decollide, on_collision));
			
			path_names.trySet(triggers.keySet());
			
			// Output them
			
			final ICrudService<AnalyticTriggerStateBean> trigger_crud = 
					_service_context.getCoreManagementDbService().getAnalyticBucketTriggerState(AnalyticTriggerStateBean.class);
			
			AnalyticTriggerCoreUtils.storeOrUpdateTriggerStage(trigger_crud, triggers);
		}
		finally { // ie always run this:
			// Unset the mutexes
			if (path_names.isSet()) AnalyticTriggerCoreUtils.deregisterOwnershipOfTriggers(path_names.get(), _distributed_services.getCuratorFramework());
		}
	}
	
	/** Handles 2 cases:
	 *  - a bucket deletion message
	 *  - a suspend message sent to a test bucket
	 * @param message
	 */
	protected void onBucketDelete(final BucketActionMessage message) {
		//TODO (ALEPH-12): special case of bucket deletion... just delete all the state beans (and worry about it later!)
		//TODO (ALEPH-12): similarly look for a suspend message sent to a test analytic .. means the same thing				
		
		//TODO (ALEPH-12): hmm what about a suspend call?
	}
	
	/** Regular trigger event messages, check for things we're supposed to check
	 * @param message
	 */
	protected void onAnalyticTrigger(final AnalyticsTriggerActionMessage message) {
		
		final ICrudService<AnalyticTriggerStateBean> trigger_crud = 
				_service_context.getCoreManagementDbService().getAnalyticBucketTriggerState(AnalyticTriggerStateBean.class);
		
		// 1) Get all state beans that need to be checked, update their "next time"
		
		final CompletableFuture<Map<String, List<AnalyticTriggerStateBean>>> triggers_in = AnalyticTriggerCoreUtils.getTriggersToCheck(trigger_crud);		
		
		triggers_in.thenAccept(triggers_to_check -> {
			
			final Consumer<String> on_collision = path -> {			
				_logger.warn("Failed to grab trigger on {0}", path);
			};
			final Duration max_time_to_decollide = Duration.ofSeconds(1L); 
			
			final SetOnce<Collection<String>> path_names = new SetOnce<>();
			try {
				// Grab the mutex
				final Map<String, List<AnalyticTriggerStateBean>> triggers = 
						AnalyticTriggerCoreUtils.registerOwnershipOfTriggers(triggers_to_check, 
								_actor_context.getInformationService().getProcessUuid(), _distributed_services.getCuratorFramework(),  
								Tuples._2T(max_time_to_decollide, on_collision));
				
				path_names.trySet(triggers.keySet());
				
				// 2) Issue checks to each bean
				
				triggers.values().stream().flatMap(trigger -> trigger.stream()).forEach(trigger_in ->
					Patterns.match(trigger_in).andAct()
						.when(trigger -> TriggerType.none == trigger.trigger_type(), trigger -> {
									
							// This is an active job, want to know if the job is complete
							
							//TODO (ALEPH-12): build message from bucket with harvest removed .. hmm need help with locked_to_host
							
							BucketActionSupervisor.askBucketActionActor(Optional.of(false), // (single node only) 
									ManagementDbActorContext.get().getBucketActionSupervisor(), 
									ManagementDbActorContext.get().getActorSystem(), null/**/, Optional.empty());
							
							//(don't care about a reply, will come asynchronously)
						})
						.otherwise(trigger -> {
							
							// This is an inactive job, want to determine if the trigger has fired
							
							//TODO (ALEPH-12)
						})
					);
			}			
			finally { // ie always run this:
				// Unset the mutexes
				if (path_names.isSet()) AnalyticTriggerCoreUtils.deregisterOwnershipOfTriggers(path_names.get(), _distributed_services.getCuratorFramework());
			}			
		});
		
		// (don't wait for replies, these will come in asynchronously)
	}

	/** Instruction to check or update a specific state 
	 *  Supported messages are: 
	 *   - 1+ analytic jobs from a given bucket have started (manual trigger)
	 *   - An analytic job has ended 
	 * @param message
	 */
	protected void onAnalyticBucketEvent(final BucketActionMessage message) {
		final ICrudService<AnalyticTriggerStateBean> trigger_crud = 
				_service_context.getCoreManagementDbService().getAnalyticBucketTriggerState(AnalyticTriggerStateBean.class);
		
		Patterns.match(message).andAct()
			.when(BucketActionMessage.BucketActionAnalyticJobMessage.class, 
					msg -> BucketActionMessage.BucketActionAnalyticJobMessage.JobMessageType.starting == msg.type(),
						msg -> {
							// 1) 1+ jobs have been manually triggered:
							
							msg.jobs().stream().forEach(job -> { // (note don't need to worry about locking here)
							
								// 1.1) Create an active entry for that job
								
								final Optional<String> locked_to_host = Optional.ofNullable(msg.handling_clients())
																			.flatMap(s -> s.stream().findFirst());
								
								final AnalyticTriggerStateBean new_entry =
										BeanTemplateUtils.build(AnalyticTriggerStateBean.class)
											.with(AnalyticTriggerStateBean::_id,
													AnalyticTriggerStateBean.buildId(msg.bucket().full_name(), job.name(), locked_to_host, Optional.empty())
											)
											.with(AnalyticTriggerStateBean::is_active, true)
											.with(AnalyticTriggerStateBean::bucket_id, msg.bucket()._id())
											.with(AnalyticTriggerStateBean::bucket_name, msg.bucket().full_name())
											.with(AnalyticTriggerStateBean::job_name, job.name())
											.with(AnalyticTriggerStateBean::trigger_type, TriggerType.none)
											.with(AnalyticTriggerStateBean::last_checked, Date.from(Instant.now()))
											.with(AnalyticTriggerStateBean::next_check, Date.from(Instant.now().plusSeconds(ACTIVE_CHECK_FREQ_SECS)))
											.with(AnalyticTriggerStateBean::locked_to_host, locked_to_host.orElse(null))
										.done().get();
								
								trigger_crud.storeObject(new_entry, true); //(fire and forget - don't use a list because storeObjects with replace can be problematic for some DBs)
							});
						})
			.when(BucketActionMessage.BucketActionAnalyticJobMessage.class, 
					msg -> BucketActionMessage.BucketActionAnalyticJobMessage.JobMessageType.stopping == msg.type(),
						msg -> { // (note don't need to worry about locking here)
							
							// 2) A previous request for the status of a job has come back telling me it has stopped
							
							// 2.1) Check whether the completion of that job is a trigger anywhere
							
							//TODO (ALEPH-12) - this seems like a good case for a call to some util							
							
							// 2.2) Remove the active entry for that job
							
							trigger_crud.deleteObjectsBySpec(CrudUtils.allOf(AnalyticTriggerStateBean.class)
																.when(AnalyticTriggerStateBean::bucket_name, msg.bucket().full_name())
																.withAny(AnalyticTriggerStateBean::job_name, msg.jobs().stream().map(j -> j.name()).collect(Collectors.toList()))
																.when(AnalyticTriggerStateBean::trigger_type, TriggerType.none)
																);
						})
			.otherwise(__ -> {}); //(ignore)
		;
	}
}
