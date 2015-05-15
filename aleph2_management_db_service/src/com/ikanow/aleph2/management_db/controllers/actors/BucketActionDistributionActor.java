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

import com.ikanow.aleph2.management_db.services.ManagementDbActorContext;
import com.ikanow.aleph2.management_db.utils.ActorUtils;

import akka.actor.ActorSelection;
import akka.actor.UntypedActor;

/** This actor's role is to send out the received bucket update messages, to marshal the replies
 *  and to send out a combined set of replies to the sender
 * @author acp
 *
 */
public class BucketActionDistributionActor extends UntypedActor {

	protected final ManagementDbActorContext _context;
	
	/** Should only ever be called by the actor system, not by users
	 */
	public BucketActionDistributionActor() {
		_context = ManagementDbActorContext.get();
	}
	
	/* (non-Javadoc)
	 * @see akka.actor.UntypedActor#onReceive(java.lang.Object)
	 */
	@Override
	public void onReceive(Object message) throws Exception {
		
		// 1) Get a list of actors, in two parts 
		
		// 1a) Check how many people are registered as listening from zookeeper/curator
		
		// 1b) Wait for that many replies
		
		// 2) Then message all of the actors who replied that they were interested and wait for the response
		
		//TODO or maybe do get an approx number via zookeeper? 
		ActorSelection bucket_action_actors = _context.getActorSystem().actorSelection(ActorUtils.BUCKET_ACTION_ACTOR);
		
		// TODO Auto-generated method stub
		
	}

}
