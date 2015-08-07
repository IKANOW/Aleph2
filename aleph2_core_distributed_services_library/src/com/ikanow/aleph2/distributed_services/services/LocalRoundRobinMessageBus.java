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

import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

import com.ikanow.aleph2.data_model.utils.Lambdas;


import com.ikanow.aleph2.distributed_services.data_model.IRoundRobinEventBusWrapper;

import akka.actor.ActorRef;
import akka.event.japi.LookupEventBus;

/** Local/"mock" round robin message bus, for single node operation and testing
 * @author Alex
 */
public class LocalRoundRobinMessageBus<M extends IRoundRobinEventBusWrapper<?>> extends LookupEventBus<M, ActorRef, String> {

	protected final static ConcurrentHashMap<String, LinkedList<ActorRef>> subscribers = new ConcurrentHashMap<>();
	protected final String _topic;
	
	public LocalRoundRobinMessageBus(final String topic) {
		_topic = topic;
	}
	
	@Override
	public String classify(final M event) {
		return _topic;
	}

	@Override
	public boolean unsubscribe(ActorRef subscriber, String from) {
		synchronized (LocalRoundRobinMessageBus.class) {
			return subscribers.get(_topic).remove(subscriber);
		}				
	}
	
	@Override
	public void unsubscribe(ActorRef subscriber) {
		this.unsubscribe(subscriber, "");
	}
	
	@Override
	public boolean subscribe(ActorRef subscriber, String to) {
		synchronized (LocalRoundRobinMessageBus.class) {
			subscribers.computeIfAbsent(_topic, __ -> new LinkedList<ActorRef>()).add(subscriber);
		}
		return true;
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
	public void publish(M event) {		
		ActorRef sub = Lambdas.get(() -> {
			synchronized (LocalRoundRobinMessageBus.class) {				
				ActorRef ret_val = subscribers.get(_topic).pop();
				subscribers.get(_topic).addLast(ret_val);
				return ret_val;
			}
		});
		publish(event, sub);
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

