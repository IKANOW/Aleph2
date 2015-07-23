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

import com.ikanow.aleph2.distributed_services.data_model.IRoundRobinEventBusWrapper;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.cluster.pubsub.DistributedPubSub;
import akka.cluster.pubsub.DistributedPubSubMediator;
import akka.event.japi.LookupEventBus;

/** Remote round-robin message bus
 * @author Alex
 *
 * @param <M>
 */
public class RemoteRoundRobinMessageBus<M extends IRoundRobinEventBusWrapper<?>> extends LookupEventBus<M, ActorRef, String> {

	final ActorRef _mediator;
	protected final String _topic;
	
	/** Guice/user c'tor 
	 * @param akka_system
	 * @param topic
	 */
	public RemoteRoundRobinMessageBus(ActorSystem akka_system, final String topic) {
		_mediator = DistributedPubSub.get(akka_system).mediator();
		_topic = topic;
	}
	
	@Override
	public boolean subscribe(ActorRef subscriber, String to) {
		//(or do Put here and .Send below, but that didn't work for me)
		_mediator.tell(new DistributedPubSubMediator.Subscribe(to, "round_robin", subscriber), subscriber);
		return true;
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
		_mediator.tell(new DistributedPubSubMediator.Publish(classify(event), event.message(), true), subscriber);
	}

	/* (non-Javadoc)
	 * @see akka.event.japi.LookupEventBus#publish(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void publish(M event) {
		_mediator.tell(new DistributedPubSubMediator.Publish(classify(event), event.message(), true), event.sender());
	}

	/* (non-Javadoc)
	 * @see akka.event.japi.LookupEventBus#mapSize()
	 */
	@Override
	public int mapSize() {
		return 32;
	}

}
