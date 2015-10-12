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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ikanow.aleph2.data_import_manager.analytics.utils.AnalyticTriggerCoreUtils;
import com.ikanow.aleph2.data_import_manager.analytics.utils.AnalyticTriggerUtils;
import com.ikanow.aleph2.data_import_manager.services.DataImportActorContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Patterns;
import com.ikanow.aleph2.data_model.utils.SetOnce;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerMessage;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerStateBean;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerMessage.AnalyticsTriggerActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;

import akka.actor.UntypedActor;

/** This actor is responsible for checking the 
 * @author Alex
 */
public class AnalyticsTriggerWorkerActor extends UntypedActor {
	protected static final Logger _logger = LogManager.getLogger();	

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
			.when(BucketActionMessage.class, msg -> onBucketChanged(msg))
			.when(AnalyticTriggerMessage.class, 
					msg -> null != msg.trigger_action_message(), 
						msg -> onAnalyticTrigger(msg.trigger_action_message()))
			.when(AnalyticTriggerMessage.class, 
					msg -> null != msg.bucket_action_message(), 
						msg -> onAnalyticBucketEvent(msg.bucket_action_message()))
			;		
	}	
	
	/** The bucket has changed so update the trigger state database
	 * @param message
	 */
	protected void onBucketChanged(final BucketActionMessage message) {
		
		// Create the state objects
		
		final Stream<AnalyticTriggerStateBean> state_beans = 
			AnalyticTriggerUtils.generateTriggerStateStream(message.bucket(), 
					Optional.ofNullable(message.bucket().multi_node_enabled())
										.filter(enabled -> enabled)
										.map(__ -> _actor_context.getInformationService().getHostname()));

		// Handle bucket collisions
		final Runnable on_collision = () -> {			
			//TODO (ALEPH-12): store this to retry queue
			_logger.error(ErrorUtils.get("FAILED TO OBTAIN MUTEX FOR {0} THIS CURRENTLY RESULTS IN A SERIOUS LOGIC ERROR - NEED TO IMPLEMENT RETRY STRATEGY", message.bucket().full_name()));			
		};
		//(should be able to decrease this once we have a saner retry strategy)
		final Duration max_time_to_decollide = Duration.ofMinutes(2L); 

		Map<String, List<AnalyticTriggerStateBean>> triggers_in = state_beans.collect(
				Collectors.groupingBy(state -> AnalyticTriggerStateBean.buildId(state.bucket_name(), state.job_name(), Optional.ofNullable(state.locked_to_host()), Optional.ofNullable(state.is_pending())) ));
		
		final SetOnce<Collection<String>> path_names = new SetOnce<>();
		try {
			// Grab the mutex
			final Map<String, List<AnalyticTriggerStateBean>> triggers = 
					AnalyticTriggerCoreUtils.registerOwnershipOfTriggers(triggers_in, 
							_actor_context.getInformationService().getProcessUuid(), _distributed_services.getCuratorFramework(),  
							Tuples._2T(max_time_to_decollide, on_collision));
			
			path_names.trySet(triggers.keySet());
			
			// Output them
			
			//TODO (ALEPH-12)
		}
		finally { // ie always run this:
			if (path_names.isSet()) AnalyticTriggerCoreUtils.deregisterOwnershipOfTriggers(path_names.get(), _distributed_services.getCuratorFramework());
		}
		
		//TODO (ALEPH-12)
	}
	
	/** Regular trigger event messages, check for things we're supposed to check
	 * @param message
	 */
	protected void onAnalyticTrigger(final AnalyticsTriggerActionMessage message) {
		
		//TODO (ALEPH-12)
		
		// 1) Get all state beans that need to be checked, update their "next time"
		
		// 2) Issue checks to each bean
		
		// (don't wait for replies, these will come in asynchronously)
	}

	/** Instruction to check or update a specific state
	 * @param message
	 */
	protected void onAnalyticBucketEvent(final BucketActionMessage message) {
		//TODO (ALEPH-12)		
	}
}
