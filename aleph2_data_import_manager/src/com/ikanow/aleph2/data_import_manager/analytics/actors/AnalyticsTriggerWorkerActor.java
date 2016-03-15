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
 *******************************************************************************/
package com.ikanow.aleph2.data_import_manager.analytics.actors;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple2;

import com.ikanow.aleph2.data_import_manager.analytics.services.AnalyticStateTriggerCheckFactory.AnalyticStateChecker;
import com.ikanow.aleph2.data_import_manager.analytics.utils.AnalyticTriggerCoreUtils;
import com.ikanow.aleph2.data_import_manager.analytics.utils.AnalyticTriggerCrudUtils;
import com.ikanow.aleph2.data_import_manager.analytics.utils.AnalyticTriggerBeanUtils;
import com.ikanow.aleph2.data_import_manager.services.DataImportActorContext;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean.AnalyticThreadComplexTriggerBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.BucketUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.Patterns;
import com.ikanow.aleph2.data_model.utils.SetOnce;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerMessage;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerStateBean;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerMessage.AnalyticsTriggerActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage.BucketActionAnalyticJobMessage.JobMessageType;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketMgmtMessage.BucketTimeoutMessage;

import fj.Unit;
import akka.actor.UntypedActor;

/** This actor is responsible for checking the state of the various active and inactive triggers in the system
 * @author Alex
 */
public class AnalyticsTriggerWorkerActor extends UntypedActor {
	protected static final Logger _logger = LogManager.getLogger();	

	final DataImportActorContext _local_actor_context;
	final IServiceContext _service_context;
	final ICoreDistributedServices _distributed_services;
	
	final SetOnce<ICrudService<DataBucketStatusBean>> _bucket_status_crud = new SetOnce<>();
	final SetOnce<ICrudService<BucketTimeoutMessage>> _bucket_test_status_crud = new SetOnce<>();
	
	/** Akka c'tor
	 */
	public AnalyticsTriggerWorkerActor() {
		_local_actor_context = DataImportActorContext.get();
		_service_context = _local_actor_context.getServiceContext();
		_distributed_services = _service_context.getService(ICoreDistributedServices.class, Optional.empty()).get();
	}
	
	/** Lazy initialization
	 */
	protected void intializeStatusDatabase() {
		if (!_bucket_status_crud.isSet()) {
			_bucket_status_crud.set(
					_service_context.getService(IManagementDbService.class, Optional.empty()) // (must have been provided by now)
					.get() // (must exist)
					.getDataBucketStatusStore()
					);
		}
		if (!_bucket_test_status_crud.isSet()) {
			_bucket_test_status_crud.set(
					_service_context.getService(IManagementDbService.class, Optional.empty()) // (must have been provided by now)
					.get() // (must exist)
					.getBucketTestQueue(BucketTimeoutMessage.class)
					);
		}
	}
	
	/* (non-Javadoc)
	 * @see akka.actor.UntypedActor#onReceive(java.lang.Object)
	 */
	@Override
	public void onReceive(Object message) throws Exception {		
		try {
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
		catch (Throwable t) {
			_logger.error(ErrorUtils.getLongForm("Error receiving message type = {1} error = {0}", t, message.getClass().getSimpleName()));
		}
	}	
	
	///////////////////////////////////////////////////////////////////////////////
	
	// MANUAL CHANGES TO BUCKETS 
	
	/** The bucket has changed (or been created) so update (or create) the relevant entries in the trigger state database
	 * @param message
	 */
	protected void onBucketChanged(final BucketActionMessage message) {
		_logger.info(ErrorUtils.get("Received bucket action relay for bucket {0}: {1}", message.bucket().full_name(), message.getClass().getName()));
		
		// Create the state objects

		final boolean is_suspended = Patterns.match(message).<Boolean>andReturn()
				.when(BucketActionMessage.NewBucketActionMessage.class, msg -> msg.is_suspended())
				.when(BucketActionMessage.UpdateBucketActionMessage.class, msg -> !msg.is_enabled())
				.otherwise(__ -> false);
		
		final Stream<AnalyticTriggerStateBean> state_beans = 
			AnalyticTriggerBeanUtils.generateTriggerStateStream(message.bucket(), 
					is_suspended,
					Optional.empty());

		//TODO (ALEPH-12): don't support locked_to_host currently .. 

		// Handle bucket collisions
		final Consumer<String> on_collision = path -> {			
			//TODO (ALEPH-12): store this to retry queue
			_logger.error(ErrorUtils.get("FAILED TO OBTAIN MUTEX FOR {0} THIS CURRENTLY RESULTS IN A SERIOUS LOGIC ERROR - NEED TO IMPLEMENT RETRY STRATEGY", message.bucket().full_name()));			
		};
		//(should be able to decrease this once we have a saner retry strategy)
		final Duration max_time_to_decollide = Duration.ofMinutes(2L); 

		// (group by buckets, can choose to use a more granular mutex down in registerOwnershipOfTriggers, though currently don't)
		final Map<Tuple2<String, String>, List<AnalyticTriggerStateBean>> triggers_in = state_beans.collect(
				Collectors.groupingBy(state -> Tuples._2T(state.bucket_name(), state.locked_to_host())));
		
		final SetOnce<Collection<Tuple2<String, String>>> path_names = new SetOnce<>();
		try {
			// Grab the mutex
			final Map<Tuple2<String, String>, List<AnalyticTriggerStateBean>> triggers = 
					AnalyticTriggerCoreUtils.registerOwnershipOfTriggers(triggers_in, 
							_local_actor_context.getInformationService().getProcessUuid(), _distributed_services.getCuratorFramework(),  
							Tuples._2T(max_time_to_decollide, on_collision));
			
			path_names.trySet(triggers.keySet());
			
			_logger.info(ErrorUtils.get("Generated {0} trigger(s) ({1} group(s)) for bucket {2} ({3} job(s))", 
					triggers_in.values().stream().flatMap(l->l.stream()).count(),
					triggers_in.size(),
					message.bucket().full_name(), 
					Optionals.of(() -> message.bucket().analytic_thread().jobs()).map(j -> j.size()).orElse(0)));			

			// Output them
			
			final ICrudService<AnalyticTriggerStateBean> trigger_crud = 
					_service_context.getCoreManagementDbService().getAnalyticBucketTriggerState(AnalyticTriggerStateBean.class);
			
			AnalyticTriggerCrudUtils.storeOrUpdateTriggerStage(message.bucket(), trigger_crud, triggers).join();			
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
		
		AnalyticTriggerCrudUtils.deleteTriggers(trigger_crud, message.bucket()).join();
	}
	
	///////////////////////////////////////////////////////////////////////////////
	
	// TRIGGERING
	
	
	/** This is a temporary service until the distributed mutex is in place - at least prevents ticks from colliding against one another
	 * @author Alex
	 */
	protected static class TickSpacingService {
		public static long TIMEOUT_MS = 300L*1000L;
		protected AtomicLong _last_trigger_time = new AtomicLong(0);
	
		protected static SetOnce<Long> _spacing = new SetOnce<>();
		
		protected boolean grabMutex() {
			synchronized (this) {
				final long now = new Date().getTime();
				if (now <=_last_trigger_time.get()) {
					_logger.warn("Ticks too close to one another, ignoring");
					return false;
				}
				_last_trigger_time.set(now + TIMEOUT_MS);
			}
			return true;			
		}
		protected void releaseMutex() {
			_last_trigger_time.set(new Date().getTime() - 1L);			
		}
	}
	protected TickSpacingService _spacing_service = new TickSpacingService();
	
	/** Regular trigger event messages, check for things we're supposed to check
	 * @param message
	 */
	protected void onAnalyticTrigger(final AnalyticsTriggerActionMessage message) {
		// Quick block to ensure that ticks don't collide on a give machine
		if (!_spacing_service.grabMutex()) {
			return;
		}
		try {
			final Date now = new Date(); // a single global time used for this set of checks)
			
			final ICrudService<AnalyticTriggerStateBean> trigger_crud = 
					_service_context.getCoreManagementDbService().getAnalyticBucketTriggerState(AnalyticTriggerStateBean.class);
			
			// 1) Get all state beans that need to be checked, update their "next time"
			
			final CompletableFuture<Map<Tuple2<String, String>, List<AnalyticTriggerStateBean>>> triggers_in = 
					AnalyticTriggerCrudUtils.getTriggersToCheck(trigger_crud);		
			
			triggers_in.thenAccept(triggers_to_check -> {
	
				//DEBUG
//				System.out.println("??? " + triggers_to_check.values().stream().flatMap(s->s.stream())
//						.map(t -> BeanTemplateUtils.toJson(t).toString()).collect(Collectors.joining("\n")));
				
				final Consumer<String> on_collision = path -> {			
					_logger.warn("Failed to grab trigger on {0}", path);
				};
				final Duration max_time_to_decollide = Duration.ofSeconds(1L); 
				
				final SetOnce<Collection<Tuple2<String, String>>> path_names = new SetOnce<>();
				try {
					// Grab the mutex
					final Map<Tuple2<String, String>, List<AnalyticTriggerStateBean>> triggers = 
							AnalyticTriggerCoreUtils.registerOwnershipOfTriggers(triggers_to_check, 
									_local_actor_context.getInformationService().getProcessUuid(), _distributed_services.getCuratorFramework(),  
									Tuples._2T(max_time_to_decollide, on_collision));
					
					path_names.trySet(triggers.keySet());
					
					// 2) Issue checks to each bean
					
					triggers.entrySet().stream().parallel()
						.forEach(kv -> {
							//(discard bucket active records)
							kv.getValue().stream().findFirst().ifPresent(trigger -> {
	
								final Optional<DataBucketBean> bucket_to_check_reply = Lambdas.wrap_u(__ -> {
										return BucketUtils.isTestBucket(
												BeanTemplateUtils.build(DataBucketBean.class)
													.with(DataBucketBean::full_name, trigger.bucket_name())
												.done().get())
										?
										// Test bucket - get from test
										_service_context.getCoreManagementDbService().readOnlyVersion().getBucketTestQueue(BucketTimeoutMessage.class)
											.getObjectById(trigger.bucket_name()) //(test bucket use transformed full name as _id)
											.<Optional<DataBucketBean>>thenApply(bucket_msg -> 
												bucket_msg
													.map(msg -> msg.bucket()))
											.join()
										:
										// Normal bucket get from bucket store
										_service_context.getCoreManagementDbService().readOnlyVersion().getDataBucketStore().getObjectById(trigger.bucket_id())
												.join()
										;
										// (annoyingly can't chain CFs because need to block this thread until i'm ready to release the mutex)
									})
									.<Optional<DataBucketBean>>andThen(maybe_bucket -> {
										return maybe_bucket
												.map(bucket -> (null == bucket.analytic_thread()) 
														? DataBucketAnalyticsChangeActor.convertEnrichmentToAnalyticBucket(bucket)
														: bucket)
												.map(bucket ->
														BeanTemplateUtils.clone(bucket)
															.with(DataBucketBean::harvest_technology_name_or_id, null) // remove this so that only gets sent to analytics/enrichment engine
															.done()
														)
												.map(bucket -> {
														return !BucketUtils.isTestBucket(bucket) // for test buckets, override the scheduler to something short
														? 
														bucket
														:
														BeanTemplateUtils.clone(bucket)
															.with(DataBucketBean::analytic_thread,
																BeanTemplateUtils.clone(bucket.analytic_thread()) //(must exist by this point)
																	.with(AnalyticThreadBean::trigger_config,
																			BeanTemplateUtils.clone(
																					Optional.ofNullable(bucket.analytic_thread().trigger_config())
																							.orElse(BeanTemplateUtils.build(AnalyticThreadTriggerBean.class).done().get())
																			)
																				.with(AnalyticThreadTriggerBean::schedule, "10 seconds")
																			.done()
																	)
																.done()		
															)
														.done();
												})
												;
									})
									.apply(Unit.unit());
															
								//(I've excluded the harvest component so any core management db messages only go to the analytics engine, not the harvest engine)  
									
								final SetOnce<AnalyticTriggerStateBean> active_bucket_record = new SetOnce<>();
								final LinkedList<AnalyticTriggerStateBean> mutable_active_jobs = new LinkedList<>();
								final LinkedList<AnalyticTriggerStateBean> mutable_external_triggers_active = new LinkedList<>();
								final LinkedList<AnalyticTriggerStateBean> mutable_internal_triggers_active = new LinkedList<>();
								final LinkedList<AnalyticTriggerStateBean> mutable_external_triggers_dormant = new LinkedList<>();
								final LinkedList<AnalyticTriggerStateBean> mutable_internal_triggers_dormant = new LinkedList<>();
								
								bucket_to_check_reply.ifPresent(bucket_to_check -> {
									kv.getValue().stream().forEach(trigger_in -> {
										
										Patterns.match().andAct()
											.when(__ -> AnalyticTriggerBeanUtils.isActiveBucketOrJobRecord(trigger_in), __ -> {
												
												// 1) This is an active job, want to know if the job is complete
												
												final Optional<AnalyticThreadJobBean> analytic_job_opt = 
														AnalyticTriggerBeanUtils.isActiveBucketRecord(trigger_in)
														? Optional.empty()
														: Optionals.of(() -> bucket_to_check.analytic_thread().jobs()
																							.stream().filter(j -> j.name().equals(trigger_in.job_name())).findFirst().get());
												
												analytic_job_opt.ifPresent(analytic_job -> onAnalyticTrigger_checkActiveJob(bucket_to_check, analytic_job, trigger_in));
												
												//(don't care about a reply, will come asynchronously)
												if (AnalyticTriggerBeanUtils.isActiveJobRecord(trigger_in)) {
													mutable_active_jobs.add(trigger_in);
												}
												else { // bucket must be active since the bucket record exists
													active_bucket_record.set(trigger_in); // (can call multiple times, will ignore all but the first)
												}
											})
											.when(__ -> !trigger_in.is_bucket_active() && AnalyticTriggerBeanUtils.isExternalTrigger(trigger_in), __ -> {
												
												// 2) Inactive bucket, check external dependency											
												onAnalyticTrigger_checkExternalTriggers(bucket_to_check, trigger_in, mutable_external_triggers_active, mutable_external_triggers_dormant, now);
											})
											.when(__ -> trigger_in.is_bucket_active() && AnalyticTriggerBeanUtils.isInternalTrigger(trigger_in), __ -> {
	
												// 3) Inactive job, active bucket
												
												final Optional<AnalyticThreadJobBean> analytic_job_opt = 
														Optionals.of(() -> bucket_to_check.analytic_thread().jobs().stream().filter(j -> j.name().equals(trigger_in.job_name())).findFirst().get());
												
												analytic_job_opt
													.ifPresent(analytic_job -> 
														onAnalyticTrigger_checkInactiveJobs(bucket_to_check, analytic_job, trigger_in, 
																							mutable_internal_triggers_active, mutable_internal_triggers_dormant, now));											
											})
											;
											//(don't care about any other cases)
									});
	
									triggerChecks_processResults(bucket_to_check, Optional.ofNullable(kv.getKey()._2()),
											active_bucket_record.optional(), mutable_active_jobs, 
											mutable_external_triggers_active, mutable_internal_triggers_active,
											mutable_external_triggers_dormant, mutable_internal_triggers_dormant, now);
								});
								
							});
						});			
				}			
				finally { // ie always run this:
					// Unset the mutexes
					if (path_names.isSet()) AnalyticTriggerCoreUtils.deregisterOwnershipOfTriggers(path_names.get(), _distributed_services.getCuratorFramework());
				}			
			})
			.join();
			
			// (don't wait for replies, these will come in asynchronously)
		}
		finally {
			_spacing_service.releaseMutex();
		}
	}

	/** If a job is active, want to know whether to clear it
	 */
	protected void onAnalyticTrigger_checkActiveJob(final DataBucketBean bucket, final AnalyticThreadJobBean job, final AnalyticTriggerStateBean trigger) {
		
		//TODO (ALEPH-12): might need to reduce the chattiness of this (can I check once every 5 minutes or something? use a google cache)
		// (but also then need to reduce the chattiness of logging for the choose and distribution actor, else there's no point...)
		_logger.info(ErrorUtils.get("Check completion status of active job = {0}:{1}{2}", bucket.full_name(), job.name(), 
				Optional.ofNullable(trigger.locked_to_host()).map(s->" (host="+s+")").orElse("")));
		
		final BucketActionMessage new_message = 
				AnalyticTriggerBeanUtils.buildInternalEventMessage(bucket, Arrays.asList(job), JobMessageType.check_completion, Optional.ofNullable(trigger.locked_to_host()));		
		
		AnalyticTriggerBeanUtils.sendInternalEventMessage(new_message, _bucket_status_crud.get(), _bucket_test_status_crud.get());
	}
	
	/** If a bucket is inactive, want to know whether to trigger it
	 */
	protected void onAnalyticTrigger_checkExternalTriggers(final DataBucketBean bucket, final AnalyticTriggerStateBean trigger, 
			final List<AnalyticTriggerStateBean> mutable_trigger_list_active, final List<AnalyticTriggerStateBean> mutable_trigger_list_dormant, final Date at)
	{
		onAnalyticTrigger_checkTrigger(bucket, Optional.empty(), trigger, mutable_trigger_list_active, mutable_trigger_list_dormant, at);
	}
	
	/** If a bucket is active but its job is inactive, want to know whether to start it
	 * @return true if the bucket is to be activated
	 */
	protected void onAnalyticTrigger_checkInactiveJobs(final DataBucketBean bucket, final AnalyticThreadJobBean job, final AnalyticTriggerStateBean trigger, 
			final List<AnalyticTriggerStateBean> mutable_trigger_list_active, final List<AnalyticTriggerStateBean> mutable_trigger_list_dormant, final Date at)
	{
		onAnalyticTrigger_checkTrigger(bucket, Optional.of(job), trigger, mutable_trigger_list_active, mutable_trigger_list_dormant, at);
	}
	
	/** Low level function for manipulating triggers
	 * @param bucket
	 * @param job
	 * @param trigger
	 * @param mutable_trigger_list -  a mutable results list for triggered entries
	 */
	protected void onAnalyticTrigger_checkTrigger(final DataBucketBean bucket, final Optional<AnalyticThreadJobBean> job, final AnalyticTriggerStateBean trigger, 
			final List<AnalyticTriggerStateBean> mutable_trigger_list_active, final List<AnalyticTriggerStateBean> mutable_trigger_list_dormant, final Date at)
	{
		final boolean is_already_triggered = AnalyticTriggerBeanUtils.checkTriggerLimits(trigger); 
		
		//DEBUG
		//System.out.println("? " + trigger.job_name() + " / " + trigger.input_resource_name_or_id() + ": " + trigger.curr_resource_size() + ": " + is_already_triggered);
		
		if (!is_already_triggered) {
			final AnalyticStateChecker checker = 
					_local_actor_context.getAnalyticTriggerFactory()
						.getChecker(trigger.trigger_type(), Optional.ofNullable(trigger.input_data_service()));
			
			final Tuple2<Boolean, Long> check_result = checker.check(bucket, job, trigger, at).join(); // (can't use the async nature because of the InterProcessMutex)
		
			if (check_result._1()) {
				mutable_trigger_list_active.add(
						BeanTemplateUtils.clone(trigger)
							.with(AnalyticTriggerStateBean::curr_resource_size, check_result._2())
						.done());
			}
			else {
				mutable_trigger_list_dormant.add(
						BeanTemplateUtils.clone(trigger)
							.with(AnalyticTriggerStateBean::curr_resource_size, check_result._2())
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
		this.intializeStatusDatabase();
		
		final ICrudService<AnalyticTriggerStateBean> trigger_crud = 
				_service_context.getCoreManagementDbService().getAnalyticBucketTriggerState(AnalyticTriggerStateBean.class);
		
		Patterns.match(message).andAct()
			.when(BucketActionMessage.BucketActionAnalyticJobMessage.class, 
					msg -> BucketActionMessage.BucketActionAnalyticJobMessage.JobMessageType.starting == msg.type(),
						msg -> { // (note don't need to worry about locking here)

							_logger.info(ErrorUtils.get("Bucket:(jobs) {0}:({1}): received message {2}", 
									msg.bucket().full_name(), 
									Optionals.ofNullable(msg.jobs()).stream().map(j -> j.name()).collect(Collectors.joining(";")),
									msg.type()
									));							
							
							// 1) 1+ jobs have been confirmed/manually started by the technology:
							// (or just the bucket if msg.jobs()==null)
							
							final Optional<String> locked_to_host = Optional.ofNullable(msg.handling_clients())
									.flatMap(s -> s.stream().findFirst());

							// 1.0) Update bucket status if needed (ie for manually triggered jobs)
							
							//(this is only used for consistency inside the updateAnalyticThreadState call below, no message is actually sent anywhere)
							final BucketActionMessage new_message = 
									AnalyticTriggerBeanUtils.buildInternalEventMessage(msg.bucket(), msg.jobs(), msg.type(), locked_to_host);						
							AnalyticTriggerCrudUtils.updateAnalyticThreadState(new_message, msg.bucket(), _bucket_status_crud.get(), Optional.empty()).join(); 
							// (use the join to ensure that the status is updated before going any further - can't compose any futures because of the distributed mutex limitations)																					
							
							Optionals.ofNullable(msg.jobs()).stream().forEach(job -> { // (note don't need to worry about locking here)
							
								// 1.1) Create an active entry for that job
								
								AnalyticTriggerCrudUtils.createActiveBucketOrJobRecord(trigger_crud, msg.bucket(), Optional.of(job), locked_to_host).join();
							});
							
							// Always (re-)activ(at)e the bucket when I get a jobs message
							// (safe but inefficient way of handling multiple triggers)
							AnalyticTriggerCrudUtils.updateTriggersWithBucketOrJobActivation(trigger_crud, msg.bucket(), Optional.empty(), locked_to_host).join();
							Optional.ofNullable(msg.jobs()).ifPresent(jobs -> 
								AnalyticTriggerCrudUtils.updateTriggersWithBucketOrJobActivation(trigger_crud, msg.bucket(), Optional.of(jobs), locked_to_host).join()
							);
							
						})
			.when(BucketActionMessage.BucketActionAnalyticJobMessage.class, 
					msg -> BucketActionMessage.BucketActionAnalyticJobMessage.JobMessageType.stopping == msg.type(),
						msg -> { // (note don't need to worry about locking here)
							
							_logger.info(ErrorUtils.get("Bucket:(jobs) {0}:({1}): received message {2}", 
									msg.bucket().full_name(), 
									Optionals.ofNullable(msg.jobs()).stream().map(j -> j.name()).collect(Collectors.joining(";")),
									msg.type()
									));
							
							final Optional<String> locked_to_host = Optional.ofNullable(msg.handling_clients())
									.flatMap(s -> s.stream().findFirst());
							
							// 2) A previous request for the status of a job has come back telling me it has stopped
							
							// 2.0) Update bucket status
							
							//(this is only used for consistency inside the updateAnalyticThreadState call below, no message is actually sent anywhere)
							final BucketActionMessage new_message = 
									AnalyticTriggerBeanUtils.buildInternalEventMessage(msg.bucket(), msg.jobs(), msg.type(), locked_to_host);						
							AnalyticTriggerCrudUtils.updateAnalyticThreadState(new_message, msg.bucket(), _bucket_status_crud.get(), Optional.empty()).join(); 
							// (use the join to ensure that the status is updated before going any further - can't compose any futures because of the distributed mutex limitations)							
							
							// 2.1) Check whether the completion of that job is a trigger anywhere
							
							Optionals.ofNullable(msg.jobs()).stream().forEach(job -> {
							
								AnalyticTriggerCrudUtils.updateTriggerInputsWhenJobOrBucketCompletes(
										trigger_crud, msg.bucket(), Optional.of(job), locked_to_host).join();								
							});
														
							// [REMOVED - 2.2) Check whether the completion of that job completes a bucket's entire analytic thread:]
							
							// (actually - don't do this here. The problem is that the above trigger might be about to start the next stage
							//  of the bucket .. so we should wait until the next trigger check where the bucket isn't activated)

							// 2.3) Remove the active entry for that job
							
							AnalyticTriggerCrudUtils.deleteActiveJobEntries(trigger_crud, msg.bucket(), msg.jobs(), locked_to_host).join();
							
							// 2.4) Update any pending entries for this job
							
							Optionals.ofNullable(msg.jobs()).stream().forEach(job -> {
								
								AnalyticTriggerCrudUtils.updateCompletedJob(trigger_crud, msg.bucket().full_name(), job.name(), locked_to_host).join();							
							});							
						})
			.when(BucketActionMessage.BucketActionAnalyticJobMessage.class, 
					msg -> BucketActionMessage.BucketActionAnalyticJobMessage.JobMessageType.deleting == msg.type(),
						msg -> { // (note don't need to worry about locking here)
							
							_logger.info(ErrorUtils.get("Bucket:(jobs) {0}:({1}): received message {2}", 
									msg.bucket().full_name(), 
									Optionals.ofNullable(msg.jobs()).stream().map(j -> j.name()).collect(Collectors.joining(";")),
									msg.type()
									));
							
							final Optional<String> locked_to_host = Optional.ofNullable(msg.handling_clients())
									.flatMap(s -> s.stream().findFirst());							
							
							// This is a special message indicating that the bucket has been updated and some jobs have been removed
							// so just remove those jobs from the trigger database
							
							AnalyticTriggerCrudUtils.deleteOldTriggers(trigger_crud, msg.bucket().full_name(), 
									Optional.ofNullable(Optionals.ofNullable(msg.jobs()).stream().map(j -> j.name()).collect(Collectors.toList())), 
									locked_to_host, Date.from(Instant.now()));
						})						
			.otherwise(__ -> {
				_logger.warn(ErrorUtils.get("Bucket {0}: received unknown message: {1}", message.bucket().full_name(), message.getClass().getSimpleName()));				
			}); //(ignore)
		;
	}

	///////////////////////////////////////////////////////////////////////////////
	
	// HANDLING THE OUTCOME OF TRIGGER CHECKS 
	
	/** Top level control function for acting on the trigger processing
	 * @param bucket_to_check
	 * @param mutable_active_jobs
	 * @param mutable_external_triggers_active
	 * @param mutable_internal_triggers_active
	 * @param mutable_external_triggers_dormant
	 * @param mutable_internal_triggers_dormant
	 * @param mutable_newly_active_jobs
	 */
	public void triggerChecks_processResults(
			final DataBucketBean bucket_to_check, Optional<String> locked_to_host,
			final Optional<AnalyticTriggerStateBean> active_bucket_record,
			final LinkedList<AnalyticTriggerStateBean> mutable_active_jobs,
			final LinkedList<AnalyticTriggerStateBean> mutable_external_triggers_active,
			final LinkedList<AnalyticTriggerStateBean> mutable_internal_triggers_active,
			final LinkedList<AnalyticTriggerStateBean> mutable_external_triggers_dormant,
			final LinkedList<AnalyticTriggerStateBean> mutable_internal_triggers_dormant,
			final Date now)
	{
		this.intializeStatusDatabase();
		
		final ICrudService<AnalyticTriggerStateBean> trigger_crud = 
				_service_context.getCoreManagementDbService().getAnalyticBucketTriggerState(AnalyticTriggerStateBean.class);
		
		final Date check_active_buckets_every_trigger = Date.from(Instant.now());
		final Date next_inactive_bucket_check = AnalyticTriggerBeanUtils.getNextCheckTime(Date.from(Instant.now()), bucket_to_check);
		
		// 0) Nice and quick, just update all the active beans
		// (there are no decisions to make because we receive the replies asynchronously via bucket action analytic event messages)
		
		triggerChecks_processResults_currentlyActiveJobs(trigger_crud, bucket_to_check, locked_to_host, check_active_buckets_every_trigger, mutable_active_jobs);
		
		// 1) OK (in theory only one of these 2 things should occur)
		
		// 1.1) should we activate a bucket based on external dependencies
		
		triggerChecks_processResults_currentlyInactiveBuckets(trigger_crud, bucket_to_check, locked_to_host, next_inactive_bucket_check, now, mutable_external_triggers_active, mutable_external_triggers_dormant);		
		
		// 1.2) should we activate a job from an active bucket based on internal dependencies

		triggerChecks_processResults_currentlyInactiveJobs(trigger_crud, bucket_to_check, locked_to_host, check_active_buckets_every_trigger, now, mutable_internal_triggers_active, mutable_internal_triggers_dormant);
		
		// 1.3) if there are no activated jobs either in the data or from step 1.3 then might need to de-activate active buckets
				
		if (active_bucket_record.isPresent() && mutable_active_jobs.isEmpty()) {
			// It looks like the bucket should no longer be active - check for sure
			triggerChecks_processResults_currentActiveBuckets(trigger_crud, bucket_to_check, active_bucket_record.get(), locked_to_host, check_active_buckets_every_trigger, now, mutable_external_triggers_active, mutable_internal_triggers_active);
		}
		
		// 2) Update all the unused triggers (do these 2 steps separately, since the next check time is different) 
		if (!mutable_external_triggers_dormant.isEmpty()) {
			AnalyticTriggerCrudUtils.updateTriggerStatuses(trigger_crud, mutable_external_triggers_dormant.stream(), next_inactive_bucket_check, Optional.empty()).join();
		}
		if (!mutable_internal_triggers_dormant.isEmpty()) {
			AnalyticTriggerCrudUtils.updateTriggerStatuses(trigger_crud, mutable_internal_triggers_dormant.stream(), check_active_buckets_every_trigger, Optional.empty()).join();			
		}
	}
	
	/** Specifically handles currently active jobs - here we have requested status information via asynchronous messaging
	 *  so we can't actually do anything other than update the usual status fields
	 * @param trigger_crud
	 * @param bucket_to_check
	 * @param mutable_active_jobs
	 */
	public void triggerChecks_processResults_currentlyActiveJobs(
			final ICrudService<AnalyticTriggerStateBean> trigger_crud,
			final DataBucketBean bucket_to_check, Optional<String> locked_to_host,
			final Date next_check,
			final LinkedList<AnalyticTriggerStateBean> mutable_active_jobs)
	{
		// (the job statuses are updated as they are received)
		if (!mutable_active_jobs.isEmpty()) {
			AnalyticTriggerCrudUtils.updateActiveJobTriggerStatus(trigger_crud, bucket_to_check, next_check).join();
		}			
	}
	
	/** Specifically handles active buckets - decide whether to mark the bucket as complete
	 * @param trigger_crud
	 * @param bucket_to_check
	 * @param bucket_active_record
	 * @param locked_to_host
	 * @param next_check
	 * @param now  - single global time when all checks are performed this cycle 
	 * @param mutable_external_triggers_active
	 * @param mutable_internal_triggers_active
	 */
	public void triggerChecks_processResults_currentActiveBuckets(
			final ICrudService<AnalyticTriggerStateBean> trigger_crud,
			final DataBucketBean bucket_to_check, final AnalyticTriggerStateBean bucket_active_record, Optional<String> locked_to_host,
			final Date next_check, final Date now,
			final LinkedList<AnalyticTriggerStateBean> mutable_external_triggers_active,
			final LinkedList<AnalyticTriggerStateBean> mutable_internal_triggers_active)
	{
		// First off, not allowed to deactivate a bucket for 60s after it's been activated
		// That way if the jobs take a while to come through it won't de-activate itself
		if (0 == (Optional.ofNullable(bucket_active_record.last_resource_size()).orElse(0L))
			&&
			(bucket_active_record.last_checked().toInstant().plusSeconds(60L).isAfter(next_check.toInstant())))
		{
			_logger.info(ErrorUtils.get("Ignoring possible de-activation of newly activated bucket {0}", bucket_to_check.full_name()));
			return;
		}
		
		if (mutable_external_triggers_active.isEmpty() && mutable_internal_triggers_active.isEmpty()) {
			// OK so no new triggering is occurring here
			
			// Check if we have an active bucket but no active jobs
			// NOTE: one thing that is nice about this is that is handles "orphaned jobs" 
			// (something of a theoretical case, maybe if the deps has a typo and I don't catch it in validation?) since
			// the job just won't start but the bucket will be active so will fall through to this case anyway
			
			// If so:
			
			final boolean bucket_still_active = 
					AnalyticTriggerCrudUtils.areAnalyticJobsActive(trigger_crud, bucket_to_check.full_name(), Optional.empty(), locked_to_host).join();

			if (!bucket_still_active) {
				_logger.info(ErrorUtils.get("Bucket {0}: changed to inactive", bucket_to_check.full_name()));			
				
				// Send a message to the technology

				final BucketActionMessage new_message = 
						AnalyticTriggerBeanUtils.buildInternalEventMessage(bucket_to_check, null, JobMessageType.stopping, locked_to_host);						

				AnalyticTriggerCrudUtils.updateAnalyticThreadState(new_message, bucket_to_check, _bucket_status_crud.get(), Optional.of(now))
					.thenAccept(res -> { if (res) AnalyticTriggerBeanUtils.sendInternalEventMessage(new_message, _bucket_status_crud.get(), _bucket_test_status_crud.get()); });
				//(don't wait for a reply or anything)
				
				// Delete the bucket record
				
				AnalyticTriggerCrudUtils.deleteActiveBucketRecord(trigger_crud, bucket_to_check.full_name(), locked_to_host).join();
				
				// Also update triggers that might depend on this bucket:
				
				AnalyticTriggerCrudUtils.updateTriggerInputsWhenJobOrBucketCompletes(
						trigger_crud, bucket_to_check, Optional.empty(), locked_to_host).join();								
			}
		}
	}
	
	/** Specifically handles inactive buckets - decide whether to activate the bucket or not
	 * @param trigger_crud
	 * @param bucket_to_check
	 * @param locked_to_host
	 * @param next_check
	 * @param now  - single global time when all checks are performed this cycle 
	 * @param mutable_external_triggers_active
	 * @param mutable_external_triggers_dormant
	 */
	public void triggerChecks_processResults_currentlyInactiveBuckets(
			final ICrudService<AnalyticTriggerStateBean> trigger_crud,
			final DataBucketBean bucket_to_check, Optional<String> locked_to_host,
			final Date next_check, final Date now,
			final LinkedList<AnalyticTriggerStateBean> mutable_external_triggers_active,
			final LinkedList<AnalyticTriggerStateBean> mutable_external_triggers_dormant)
	{
		if (!mutable_external_triggers_active.isEmpty()) {
			
			final Optional<AnalyticThreadComplexTriggerBean> trigger_checker = 
					AnalyticTriggerBeanUtils.getManualOrAutomatedTrigger(bucket_to_check);
												
			final boolean external_bucket_activate =
					trigger_checker.map(checker -> {
						final Set<Tuple2<String, String>> resources_dataservices = 
								mutable_external_triggers_active.stream()
								.map(t -> Tuples._2T(t.input_resource_combined(), t.input_data_service()))
								.collect(Collectors.toSet())
								;
														
						boolean b = AnalyticTriggerBeanUtils.checkTrigger(checker, resources_dataservices, true);
						
						if (b) _logger.info(ErrorUtils.get("Bucket {0}: changed to active because of {1}", 
								bucket_to_check.full_name(),
								resources_dataservices.toString())
								);							
						
						return b;
					})
					.orElse(false);
			
			if (external_bucket_activate) {				
				// 2 things to do
				
				// 1) Send a notification to the technologies
			
				final BucketActionMessage new_message = 
						AnalyticTriggerBeanUtils.buildInternalEventMessage(bucket_to_check, null, JobMessageType.starting, locked_to_host);						

				AnalyticTriggerCrudUtils.updateAnalyticThreadState(new_message, bucket_to_check, _bucket_status_crud.get(), Optional.of(now))
					.thenAccept(res -> { if (res) AnalyticTriggerBeanUtils.sendInternalEventMessage(new_message, _bucket_status_crud.get(), _bucket_test_status_crud.get()); });
				//(don't wait for a reply or anything)
				
				// 2) Update all the jobs
				
				// (actually _don't_ do this unless I get a return from the tech via onAnalyticBucketEvent)
				
				// 3) Create an active bucket record
				AnalyticTriggerCrudUtils.createActiveBucketOrJobRecord(trigger_crud, bucket_to_check, Optional.empty(), locked_to_host).join();
				
				// 4) Also update the states:
				
				AnalyticTriggerCrudUtils.updateTriggerStatuses(trigger_crud, mutable_external_triggers_active.stream(), next_check, Optional.of(true)).join();				
			}
			else { // Treat these as if they never triggered at all:
				mutable_external_triggers_dormant.addAll(mutable_external_triggers_active);
			}
		}
	}

	 /** Specifically handles inactive jobs inside active bucketes - decide whether to activate the job or not
	 * @param trigger_crud
	 * @param bucket_to_check
	 * @param locked_to_host
	 * @param next_check
	 * @param now  - single global time when all checks are performed this cycle 
	 * @param mutable_internal_triggers_active
	 * @param mutable_internal_triggers_dormant
	 */
	public void triggerChecks_processResults_currentlyInactiveJobs(
			final ICrudService<AnalyticTriggerStateBean> trigger_crud,
			final DataBucketBean bucket_to_check, Optional<String> locked_to_host,
			final Date next_check, final Date now,
			final LinkedList<AnalyticTriggerStateBean> mutable_internal_triggers_active,
			final LinkedList<AnalyticTriggerStateBean> mutable_internal_triggers_dormant)
	{
		final LinkedList<AnalyticThreadJobBean> mutable_newly_active_jobs = new LinkedList<>();

		// Group all active triggers by job name
		final Map<String, List<AnalyticTriggerStateBean>> grouped_triggers = 
				mutable_internal_triggers_active.stream().collect(Collectors.groupingBy(t -> t.job_name()));
		
		mutable_internal_triggers_active.clear(); // (will add back in as needed)
		
		// Associate job name / active triggers
		final Map<String, AnalyticThreadJobBean> job_map = 
				Optionals.of(() -> bucket_to_check.analytic_thread().jobs()).orElse(Collections.emptyList())
					.stream()
					.collect(Collectors.toMap(job -> job.name(), job -> job));
		
		// Figure out which jobs might be able to start based on the active triggers
		
		grouped_triggers.entrySet().stream()
				.forEach(kv -> {
					final AnalyticThreadJobBean job = job_map.get(kv.getKey());
					if ((null == job) || Optionals.ofNullable(job.dependencies()).isEmpty()) { // stick the triggers in the dormant list
						mutable_internal_triggers_dormant.addAll(kv.getValue());
					}
					else { // we matched the job up 
						final HashSet<String> mutable_deps = new HashSet<>(Optionals.ofNullable(job.dependencies()));
						mutable_deps.removeAll(kv.getValue().stream().map(tr -> tr.input_resource_name_or_id()).collect(Collectors.toList()));
						if (mutable_deps.isEmpty()) { // All dependencies match!
							mutable_internal_triggers_active.addAll(kv.getValue());			
							mutable_newly_active_jobs.add(job);
						}
						else {
							mutable_internal_triggers_dormant.addAll(kv.getValue());							
						}
					}
				});

		// the jobs:
		
		if (!mutable_newly_active_jobs.isEmpty()) {
			final BucketActionMessage new_message = AnalyticTriggerBeanUtils.buildInternalEventMessage(bucket_to_check, mutable_newly_active_jobs, JobMessageType.starting, locked_to_host);

			AnalyticTriggerCrudUtils.updateAnalyticThreadState(new_message, bucket_to_check, _bucket_status_crud.get(), Optional.of(now))
				.thenAccept(res -> { if (res) AnalyticTriggerBeanUtils.sendInternalEventMessage(new_message, _bucket_status_crud.get(), _bucket_test_status_crud.get()); });
			//(don't wait for a reply or anything)
			
			_logger.info(ErrorUtils.get("Bucket {0}: triggered {1}", bucket_to_check.full_name(),
					mutable_newly_active_jobs.stream().map(j -> j.name()).collect(Collectors.joining(";"))
					));						
			
			// But do immediately set up the jobs as active - if the tech fails, then we'll find out when we poll them later
			
			mutable_newly_active_jobs.stream().parallel().forEach(job ->
				AnalyticTriggerCrudUtils.createActiveBucketOrJobRecord(trigger_crud, bucket_to_check, Optional.of(job), locked_to_host).join());
			
		}		
		// the triggers:
		// (note that all internal active triggers that remain at this point are "legit" (unlike external where you can have partial triggering)
		
		AnalyticTriggerCrudUtils.updateTriggerStatuses(trigger_crud, mutable_internal_triggers_active.stream(), next_check, Optional.of(true)).join();				
	}
}
