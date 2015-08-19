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

