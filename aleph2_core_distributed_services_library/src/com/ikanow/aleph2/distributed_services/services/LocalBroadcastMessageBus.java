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
package com.ikanow.aleph2.distributed_services.services;

import com.ikanow.aleph2.distributed_services.data_model.IBroadcastEventBusWrapper;

import akka.actor.ActorRef;
import akka.event.japi.LookupEventBus;

/** Local/"mock" broadcast message bus, for single node operation and testing
 * @author Alex
 */
public class LocalBroadcastMessageBus<M extends IBroadcastEventBusWrapper<?>> extends LookupEventBus<M, ActorRef, String> {

	protected final String _topic;
	
	public LocalBroadcastMessageBus(final String topic) {
		_topic = topic;
	}
	
	@Override
	public String classify(final M event) {
		return _topic;
	}

	/* (non-Javadoc)
	 * @see akka.event.japi.LookupEventBus#compareSubscribers(java.lang.Object, java.lang.Object)
	 */
	@Override
	public int compareSubscribers(ActorRef a, ActorRef b) {
		return a.compareTo(b);
	}

	/* (non-Javadoc)
	 * @see akka.event.japi.LookupEventBus#publish(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void publish(M event, ActorRef subscriber) {
		 subscriber.tell(event.message(), event.sender());
	}

	/* (non-Javadoc)
	 * @see akka.event.japi.LookupEventBus#mapSize()
	 */
	@Override
	public int mapSize() {
		return 32;
	}

}

