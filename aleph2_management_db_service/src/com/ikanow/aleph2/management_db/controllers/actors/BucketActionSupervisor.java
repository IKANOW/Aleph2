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

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.ikanow.aleph2.distributed_services.utils.AkkaFutureUtils;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage;
import com.ikanow.aleph2.management_db.utils.ActorUtils;

import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;
import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.pattern.Patterns;

/** This actor just exists to manage the child actors that actually do work
 * @author acp
 */
public class BucketActionSupervisor extends UntypedActor {

	//TODO (ALEPH-10): First sends a message to streaming enrichment (if enabled), only send to buckets if that returns success
	//TODO: only thing is that I don't want to add the streaming enrichment "id" to the others, right?
	
	//TODO (ALEPH-19): Need a scheduled thread that runs through the retries and checks each one
	
	//TODO (ALEPH-19): Need different timeouts to handle "found harvester it's taking its time" vs "waiting internally for harvesters"
	// this is larger than it needs to be to handle that case
	public static final FiniteDuration DEFAULT_TIMEOUT = Duration.create(60, TimeUnit.SECONDS);
	
	/** Internal request message for forwarding from the supervisor to its children
	 * @author acp
	 */
	private static class RequestMessage {
		protected RequestMessage(
				final Class<? extends Actor> actor_type,
				final BucketActionMessage message,
				final Optional<FiniteDuration> timeout)
		{
			this.actor_type = actor_type;
			this.message = message;
			this.timeout = timeout;
		}

		protected final Class<? extends Actor> actor_type;
		protected final BucketActionMessage message;
		protected final Optional<FiniteDuration> timeout;
	}
	
	/** Send an action message to the multi-node distribution actor, get a future containing the reply 
	 * @param supervisor - the (probably singleton
	 * @param message - the message to send 
	 * @param timeout - message timeout
	 * @return the future containing a collection of replies
	 */
	public static CompletableFuture<BucketActionReplyMessage.BucketActionCollectedRepliesMessage> 
				askDistributionActor(final ActorRef supervisor, final ActorSystem actor_context,
						final BucketActionMessage message, 
						final Optional<FiniteDuration> timeout)
	{
		if (null == message.bucket().harvest_technology_name_or_id()) {
			// Centralized check: if the harvest_technology_name_or_id isnt' present, nobody cares so short cut actually checking
			return CompletableFuture.completedFuture(
					new BucketActionReplyMessage.BucketActionCollectedRepliesMessage(
							Collections.emptyList(), Collections.emptySet()
							));
		}
		RequestMessage m = new RequestMessage(BucketActionDistributionActor.class, message, timeout);
		//(the 2* ensures that it is "always" normally the bucket that times out, which is more controlled)
		return AkkaFutureUtils.efficientWrap(Patterns.ask(supervisor, m, 2*timeout.orElse(DEFAULT_TIMEOUT).toMillis()), actor_context.dispatcher());
	}
	
	/** Send an action message to the multi-node distribution actor, get a future containing the reply 
	 * @param supervisor - the (probably singleton
	 * @param message - the message to send 
	 * @param timeout - message timeout
	 * @return the future containing a collection of replies
	 */
	public static CompletableFuture<BucketActionReplyMessage.BucketActionCollectedRepliesMessage> 
				askChooseActor(final ActorRef supervisor, final ActorSystem actor_context,
					final BucketActionMessage message, 
					final Optional<FiniteDuration> timeout)
	{
		if (null == message.bucket().harvest_technology_name_or_id()) {
			// Centralized check: if the harvest_technology_name_or_id isnt' present, nobody cares so short cut actually checking
			return CompletableFuture.completedFuture(
					new BucketActionReplyMessage.BucketActionCollectedRepliesMessage(
							Collections.emptyList(), Collections.emptySet()
							));
		}
		RequestMessage m = new RequestMessage(BucketActionChooseActor.class, message, timeout);
		//(the 5* ensures that it is "always" normally the bucket that times out, which is more controlled) (5* because there's an additional send-ack compared to choose actor)
		return AkkaFutureUtils.efficientWrap(Patterns.ask(supervisor, m, 5*timeout.orElse(DEFAULT_TIMEOUT).toMillis()), actor_context.dispatcher());
			//(choose has longer timeout because of retries)
	}
	/* (non-Javadoc)
	 * @see akka.actor.UntypedActor#onReceive(java.lang.Object)
	 */
	@Override
	public void onReceive(Object untyped_message) throws Exception {
		if (untyped_message instanceof RequestMessage) {
			RequestMessage message = (RequestMessage) untyped_message;
			ActorRef new_child = this.context().actorOf(Props.create(message.actor_type, message.timeout, ActorUtils.BUCKET_ACTION_ZOOKEEPER));

			new_child.forward(message.message, this.context());
		}
		else {
			this.unhandled(untyped_message);
		}
	}
}
