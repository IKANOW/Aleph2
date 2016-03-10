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
package com.ikanow.aleph2.management_db.data_model;

import java.io.Serializable;

import akka.actor.ActorRef;

import com.ikanow.aleph2.distributed_services.data_model.IRoundRobinEventBusWrapper;

/** Wrapper for bulk or targeted triggers 
 * @author Alex
 */
public class AnalyticTriggerMessage implements Serializable {
	private static final long serialVersionUID = 5554439532398833149L;

	/** Jackson c'tor
	 */
	protected AnalyticTriggerMessage() {}

	/** User c'tor - a bucket has changed, recheck triggers
	 * @param bucket_action_msg
	 */
	public AnalyticTriggerMessage(final BucketActionMessage bucket_action_msg) {		
		bucket_action_message = bucket_action_msg;
	}
	
	/** User c'tor - recheck all triggers periodically
	 * @param trigger_action_msg
	 */
	public AnalyticTriggerMessage(final AnalyticsTriggerActionMessage trigger_action_msg) {		
		trigger_action_message = trigger_action_msg;
	}
	
	/** Only one of this and trigger_action_message can be specified - if it's this then indicates that a bucket has changed and its trigger should be updated
	 * @return if present then indicates that a bucket has changed and its trigger should be updated
	 */
	public BucketActionMessage bucket_action_message() { return bucket_action_message; }	
	
	/** Only one of this and bucket_action_message can be specified - if it's this then indicates that the set of triggers should be checked
	 * @return if present 
	 */
	public AnalyticsTriggerActionMessage trigger_action_message() { return trigger_action_message; }	
	
	protected BucketActionMessage bucket_action_message;
	protected AnalyticsTriggerActionMessage trigger_action_message;
	
	/** An internal class containing information about a trigger activity
	 *  Currently requires no fields
	 * @author Alex
	 *
	 */
	public static class AnalyticsTriggerActionMessage implements Serializable {
		private static final long serialVersionUID = -8494899932411053064L;

		/** User/Jackson ctor
		 */
		public AnalyticsTriggerActionMessage() {}
	}
	
	/** An internal class used to wrap broadcast event bus publications
	 * @author acp
	 */
	public static class AnalyticsTriggerEventBusWrapper implements IRoundRobinEventBusWrapper<AnalyticTriggerMessage>,Serializable {
		private static final long serialVersionUID = -1930975525984244358L;
		
		protected AnalyticsTriggerEventBusWrapper() { }
		/** User c'tor for wrapping a BucketActionMessage to be sent over the bus
		 * @param sender - the sender of the message
		 * @param message - the message to be wrapped
		 */
		public AnalyticsTriggerEventBusWrapper(final ActorRef sender, final AnalyticTriggerMessage message) {
			this.sender = sender;
			this.message = message;
		}	
		@Override
		public ActorRef sender() { return sender; };
		@Override
		public AnalyticTriggerMessage message() { return message; };
		
		protected ActorRef sender;
		protected AnalyticTriggerMessage message;
	}	

}
