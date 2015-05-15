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
package com.ikanow.aleph2.management_db.controllers.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;

/** This actor just exists to manage the child actors that actually do work
 * @author acp
 */
public class BucketActionParentActor extends UntypedActor {

	/** Factory method for getting a distribution actor
	 */
	public ActorRef getNewDistributionActor() {
		return this.getContext().actorOf(Props.create(BucketActionDistributionActor.class));
	}

	/* (non-Javadoc)
	 * @see akka.actor.UntypedActor#onReceive(java.lang.Object)
	 */
	@Override
	public void onReceive(Object untyped_message) throws Exception {
		this.unhandled(untyped_message);
	}
}
