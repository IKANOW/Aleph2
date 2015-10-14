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
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple2;

import com.google.common.collect.ImmutableSet;
import com.ikanow.aleph2.data_import_manager.analytics.services.AnalyticStateTriggerCheckFactory.AnalyticStateChecker;
import com.ikanow.aleph2.data_import_manager.analytics.utils.AnalyticTriggerCoreUtils;
import com.ikanow.aleph2.data_import_manager.analytics.utils.AnalyticTriggerUtils;
import com.ikanow.aleph2.data_import_manager.services.DataImportActorContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean.AnalyticThreadComplexTriggerBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean.AnalyticThreadComplexTriggerBean.TriggerType;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.BucketUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.Patterns;
import com.ikanow.aleph2.data_model.utils.SetOnce;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.management_db.controllers.actors.BucketActionSupervisor;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerMessage;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerStateBean;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerMessage.AnalyticsTriggerActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage.BucketActionAnalyticJobMessage.JobMessageType;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.services.ManagementDbActorContext;

import akka.actor.UntypedActor;

//TODO (ALEPH-12) One complication is that we may not care about all triggers all the time?
// eg if I have job X is dependent on input Y in bucket A, but _isn't_ an external dependency
// then don't care ... ah i see, so i need to be getting
// - _all_ _external_ dependencies (these have job==null)
// - _any_ _internal_ depedencies for _active_ _buckets_ 
// Hmm ok more complications
// 1) ony want to reset triggers that result in job activation

/** This actor is responsible for checking the state of the various active and inactive triggers in the system
 * @author Alex
 */
/**
 * @author Alex
 *
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
						msg -> onBucketDelete(msg))
						
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
	
	///////////////////////////////////////////////////////////////////////////////
	
	// MANUAL CHANGES TO BUCKETS 
	
	/** The bucket has changed (or been created) so update (or create) the relevant entries in the trigger state database
	 * @param message
	 */
	protected void onBucketChanged(final BucketActionMessage message) {
		_logger.info(ErrorUtils.get("Received analytic trigger request for bucket {0}", message.bucket().full_name()));
		
		// Create the state objects

		final boolean is_suspended = Patterns.match(message).<Boolean>andReturn()
				.when(BucketActionMessage.UpdateBucketActionMessage.class, msg -> !msg.is_enabled(), __ -> true)
				.otherwise(__ -> false);
		
		final Stream<AnalyticTriggerStateBean> state_beans = 
			AnalyticTriggerUtils.generateTriggerStateStream(message.bucket(), 
					is_suspended,
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

		// (group by buckets, can choose to use a more granular mutex down in registerOwnershipOfTriggers, though currently don't)
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
		final ICrudService<AnalyticTriggerStateBean> trigger_crud = 
				_service_context.getCoreManagementDbService().getAnalyticBucketTriggerState(AnalyticTriggerStateBean.class);
		
		AnalyticTriggerCoreUtils.deleteTriggers(trigger_crud, message.bucket());
	}
	
	///////////////////////////////////////////////////////////////////////////////
	
	// TRIGGERING
	
	/** Regular trigger event messages, check for things we're supposed to check
	 * @param message
	 */
	protected void onAnalyticTrigger(final AnalyticsTriggerActionMessage message) {
		
		final ICrudService<AnalyticTriggerStateBean> trigger_crud = 
				_service_context.getCoreManagementDbService().getAnalyticBucketTriggerState(AnalyticTriggerStateBean.class);
		
		// 1) Get all state beans that need to be checked, update their "next time"
		
		final CompletableFuture<Map<String, List<AnalyticTriggerStateBean>>> triggers_in = 
				AnalyticTriggerCoreUtils.getTriggersToCheck(trigger_crud);		
		
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
				
				triggers.entrySet().stream().parallel()
					.forEach(kv -> {
						kv.getValue().stream().findFirst().ifPresent(trigger -> {

							final Optional<DataBucketBean> bucket_to_check_reply = 
									_service_context.getCoreManagementDbService().readOnlyVersion().getDataBucketStore().getObjectById(trigger.bucket_id(),
											Arrays.asList(BeanTemplateUtils.from(DataBucketBean.class).field(DataBucketBean::harvest_technology_name_or_id)),
											false
											)
											.join(); // (annoyingly can't chain CFs because need to block this thread until i'm ready to release the 
							//(I've excluded the harvest component so any core management db messages only go to the analytics engine, not the harvest engine)  
								
							final LinkedList<AnalyticTriggerStateBean> mutable_active_jobs = new LinkedList<>();
							final LinkedList<AnalyticTriggerStateBean> mutable_external_triggers_active = new LinkedList<>();
							final LinkedList<AnalyticTriggerStateBean> mutable_internal_triggers_active = new LinkedList<>();
							final LinkedList<AnalyticTriggerStateBean> mutable_external_triggers_dormant = new LinkedList<>();
							final LinkedList<AnalyticTriggerStateBean> mutable_internal_triggers_dormant = new LinkedList<>();
							
							bucket_to_check_reply.ifPresent(bucket_to_check -> {
								kv.getValue().stream().forEach(trigger_in -> {
									
									Patterns.match().andAct()
										.when(__ -> TriggerType.none == trigger_in.trigger_type(), __ -> {
											
											// 1) This is an active job, want to know if the job is complete
											
											final Optional<AnalyticThreadJobBean> analytic_job_opt = 
													Optionals.of(() -> bucket_to_check.analytic_thread().jobs().stream().filter(j -> j.name().equals(trigger_in.job_name())).findFirst().get());
											
											analytic_job_opt.ifPresent(analytic_job -> onAnalyticTrigger_checkActiveJob(bucket_to_check, analytic_job, trigger_in));
											
											//(don't care about a reply, will come asynchronously)
											mutable_active_jobs.add(trigger_in);
										})
										.when(__ -> !trigger_in.is_bucket_active() && (null == trigger_in.job_name()), __ -> {
											
											// 2) Inactive bucket, check external depdendency
											
											onAnalyticTrigger_checkExternalTriggers(bucket_to_check, trigger_in, mutable_external_triggers_active, mutable_external_triggers_dormant);
										})
										.when(__ -> trigger_in.is_bucket_active() && (null != trigger_in.job_name()), __ -> {
											
											// 3) Inactive job, active bucket
											
											final Optional<AnalyticThreadJobBean> analytic_job_opt = 
													Optionals.of(() -> bucket_to_check.analytic_thread().jobs().stream().filter(j -> j.name().equals(trigger_in.job_name())).findFirst().get());
											
											analytic_job_opt.ifPresent(analytic_job -> onAnalyticTrigger_checkInactiveJobs(bucket_to_check, analytic_job, trigger_in, mutable_internal_triggers_active, mutable_internal_triggers_dormant));
										})
										;
										//(don't care about any other cases)
								});

								//TODO (ALEPH-12): put all this in a separate function:
								
								// 0) Nice and quick, just update all the active beans
								// (there are no decisions to make because we receive the replies asyncronously via bucket action analytic event messages)
								
								//TODO util?
								
								if (!mutable_active_jobs.isEmpty()) {
									final QueryComponent<AnalyticTriggerStateBean> update_query = 
											CrudUtils.allOf(AnalyticTriggerStateBean.class)
													.when(AnalyticTriggerStateBean::trigger_type, TriggerType.none)
													.when(AnalyticTriggerStateBean::bucket_name, bucket_to_check.full_name())
													;
											
									final Instant now = Instant.now();
									
									final UpdateComponent<AnalyticTriggerStateBean> update = 
											CrudUtils.update(AnalyticTriggerStateBean.class)
													.set(AnalyticTriggerStateBean::last_checked, Date.from(now))
													.set(AnalyticTriggerStateBean::next_check, Date.from(now.plusSeconds(ACTIVE_CHECK_FREQ_SECS)))
													;												
									
									trigger_crud.updateObjectsBySpec(update_query, Optional.of(false), update);
								}
								
								// 1) OK (in theory only one of these 2 things should occur)
								
								// 1.1) should we activate a bucket based on external dependencies
								
								if (!mutable_external_triggers_active.isEmpty()) {
									
									final Date next_check = AnalyticTriggerUtils.getNextCheckTime(Date.from(Instant.now()), bucket_to_check);
									
									final Optional<AnalyticThreadComplexTriggerBean> trigger_checker = 
											AnalyticTriggerUtils.getManualOrAutomatedTrigger(bucket_to_check);
																		
									final boolean external_bucket_activate =
											trigger_checker.map(checker -> {
												final Set<Tuple2<String, String>> resources_dataservices = 
														mutable_external_triggers_active.stream()
														.map(t -> Tuples._2T(t.input_resource_combined(), t.input_data_service()))
														.collect(Collectors.toSet())
														;
														
												return AnalyticTriggerUtils.checkTrigger(checker,resources_dataservices);
											})
											.orElse(false);
									
									if (external_bucket_activate) {
										// 2 things to do
										
										// 1) Send a notification to the technologies
									
										final BucketActionMessage.BucketActionAnalyticJobMessage new_message =
												new BucketActionMessage.BucketActionAnalyticJobMessage(bucket_to_check, null, JobMessageType.starting);
										
										BucketActionSupervisor.askBucketActionActor(Optional.of(false), // (single node only) 
												ManagementDbActorContext.get().getBucketActionSupervisor(), 
												ManagementDbActorContext.get().getActorSystem(), new_message, Optional.empty());
										//(don't wait for a reply or anything)
										
										// 2) Update all the jobs
										
										final QueryComponent<AnalyticTriggerStateBean> update_query = 
												CrudUtils.allOf(AnalyticTriggerStateBean.class)
														.when(AnalyticTriggerStateBean::bucket_name, bucket_to_check.full_name());
										
										final UpdateComponent<AnalyticTriggerStateBean> update = 
												CrudUtils.update(AnalyticTriggerStateBean.class)
														.set(AnalyticTriggerStateBean::is_bucket_active, true)
														;
										
										trigger_crud.updateObjectsBySpec(update_query, Optional.of(false), update);
										
										// 3) Also update the states:
										mutable_external_triggers_active.stream().parallel().forEach(t -> {
											final UpdateComponent<AnalyticTriggerStateBean> trigger_update =
													CrudUtils.update(AnalyticTriggerStateBean.class)
														.set(AnalyticTriggerStateBean::curr_resource_size, t.curr_resource_size())
														.set(AnalyticTriggerStateBean::last_checked, Date.from(Instant.now()))
														.set(AnalyticTriggerStateBean::next_check, next_check)
														;
											trigger_crud.updateObjectById(t._id(), trigger_update);
										});
									}
									else { // Treat these as if they never triggered at all:
										mutable_external_triggers_dormant.addAll(mutable_external_triggers_active);
									}
								}
								if (!mutable_external_triggers_dormant.isEmpty()) {
									//TODO: update last/next checked and 
								}
								
								// 1.2) should we activate a job from an active bucket based on internal dependencies
								
								// 2) Update all the triggers 
								
								//TODO (ALEPH-12): once we've updated all the triggers, time to work out which buckets to update
							});
							
						});
					});			
			}			
			finally { // ie always run this:
				// Unset the mutexes
				if (path_names.isSet()) AnalyticTriggerCoreUtils.deregisterOwnershipOfTriggers(path_names.get(), _distributed_services.getCuratorFramework());
			}			
		});
		
		// (don't wait for replies, these will come in asynchronously)
	}

	/** If a job is active, want to know whether to clear it
	 */
	protected void onAnalyticTrigger_checkActiveJob(final DataBucketBean bucket, final AnalyticThreadJobBean job, final AnalyticTriggerStateBean trigger) {
		
		final BucketActionMessage.BucketActionAnalyticJobMessage new_message =
				new BucketActionMessage.BucketActionAnalyticJobMessage(bucket, Arrays.asList(job), JobMessageType.check_completion);
		
		final BucketActionMessage.BucketActionAnalyticJobMessage message_with_node_affinity =
				Lambdas.get(() -> {
					if (null == trigger.locked_to_host()) return new_message;
					else return BeanTemplateUtils.clone(new_message)
									.with(BucketActionMessage::handling_clients, ImmutableSet.builder().add(trigger.locked_to_host()).build())
								.done();
				});
		
		BucketActionSupervisor.askBucketActionActor(Optional.of(false), // (single node only) 
				ManagementDbActorContext.get().getBucketActionSupervisor(), 
				ManagementDbActorContext.get().getActorSystem(), message_with_node_affinity, Optional.empty());
		
	}
	
	/** If a bucket is inactive, want to know whether to trigger it
	 */
	protected void onAnalyticTrigger_checkExternalTriggers(final DataBucketBean bucket, final AnalyticTriggerStateBean trigger, 
			final List<AnalyticTriggerStateBean> mutable_trigger_list_active, final List<AnalyticTriggerStateBean> mutable_trigger_list_dormant)
	{
		onAnalyticTrigger_checkTrigger(bucket, Optional.empty(), trigger, mutable_trigger_list_active, mutable_trigger_list_dormant);
	}
	
	/** If a bucket is active but its job is inactive, want to know whether to start it
	 */
	protected void onAnalyticTrigger_checkInactiveJobs(final DataBucketBean bucket, final AnalyticThreadJobBean job, final AnalyticTriggerStateBean trigger, 
			final List<AnalyticTriggerStateBean> mutable_trigger_list_active, final List<AnalyticTriggerStateBean> mutable_trigger_list_dormant)
	{
		onAnalyticTrigger_checkTrigger(bucket, Optional.of(job), trigger, mutable_trigger_list_active, mutable_trigger_list_dormant);
	}
	
	/** Low level function for manipulating triggers
	 * @param bucket
	 * @param job
	 * @param trigger
	 * @param mutable_trigger_list -  a mutable results list for triggered entries
	 */
	protected void onAnalyticTrigger_checkTrigger(final DataBucketBean bucket, final Optional<AnalyticThreadJobBean> job, final AnalyticTriggerStateBean trigger, 
			final List<AnalyticTriggerStateBean> mutable_trigger_list_active, final List<AnalyticTriggerStateBean> mutable_trigger_list_dormant)
	{
		final boolean is_already_triggered = AnalyticTriggerUtils.checkTriggerLimits(trigger); 
			
		if (!is_already_triggered) {
			final AnalyticStateChecker checker = _actor_context.getAnalyticTriggerFactory().getChecker(trigger.trigger_type());
			
			final Tuple2<Boolean, Long> check_result = checker.check(bucket, job, trigger).join(); // (can't use the async nature because of the InterProcessMutex)
			
			if (check_result._1()) {
				mutable_trigger_list_active.add(
						BeanTemplateUtils.clone(trigger)
							.with(AnalyticTriggerStateBean::last_resource_size, check_result._2())
						.done());
			}
			else {
				mutable_trigger_list_dormant.add(
						BeanTemplateUtils.clone(trigger)
							.with(AnalyticTriggerStateBean::last_resource_size, check_result._2())
						.done());				
			}			
		}
		else { // (else going to see if it triggers this time...)
			mutable_trigger_list_active.add(trigger);			
		}
	}
		
	///////////////////////////////////////////////////////////////////////////////
	
	// BUCKET OR JOB RELATED STATE CHANGES 
	
	/** Instruction to check or update a specific state 
	 *  Supported messages are: 
	 *   - 1+ analytic jobs from a given bucket have started (manual trigger)
	 *   - An analytic job has ended 
	 * @param message
	 */
	protected void onAnalyticBucketEvent(final BucketActionMessage message) {
		final ICrudService<AnalyticTriggerStateBean> trigger_crud = 
				_service_context.getCoreManagementDbService().getAnalyticBucketTriggerState(AnalyticTriggerStateBean.class);
		
		///TODO (ALEPH-12): add queries to optimization list
		
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
											.with(AnalyticTriggerStateBean::is_bucket_active, true)
											.with(AnalyticTriggerStateBean::is_job_active, true)
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
							
							// 2.3) Update any pending entries for this job
							
							//TODO (ALEPH-12) - just call that other function
							
							// 2.4) Does this mean the entire bucket is complete?
							
							//TODO (ALEPH-12) - just look to see if the bucket has any empty entries		
						})
			.otherwise(__ -> {}); //(ignore)
		;
	}
}
