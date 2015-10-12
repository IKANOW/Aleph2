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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ikanow.aleph2.data_model.utils.Patterns;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerMessage;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerMessage.AnalyticsTriggerActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;

import akka.actor.UntypedActor;

/** This actor is responsible for checking the 
 * @author Alex
 */
public class AnalyticsTriggerWorkerActor extends UntypedActor {
	protected static final Logger _logger = LogManager.getLogger();	

	// Messages I can receive:
	// "FullPing" (String) - scheduled check against all buckets
	// "FastPing" (String) - scheduled check against active buckets
	// BucketActionMessage - update that specific bucket's jobs
	
	//TODO (ALEPH-12) Implementation
	
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
		//TODO (ALEPH-12)
	}
	
	/** Regular trigger event messages, check for things we're supposed to check
	 * @param message
	 */
	protected void onAnalyticTrigger(final AnalyticsTriggerActionMessage message) {
		//TODO (ALEPH-12)		
	}

	/** Instruction to check a specific state
	 * @param message
	 */
	protected void onAnalyticBucketEvent(final BucketActionMessage message) {
		//TODO (ALEPH-12)		
	}
}
