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

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.ikanow.aleph2.data_model.utils.FutureUtils;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage;

import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;
import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.pattern.Patterns;

/** This actor just exists to manage the child actors that actually do work
 * @author acp
 */
public class BucketActionSupervisor extends UntypedActor {

	public static final FiniteDuration DEFAULT_TIMEOUT = Duration.create(10, TimeUnit.SECONDS);
	
	/** Factory method for getting a distribution actor
	 */
	protected ActorRef getNewDistributionActor(final @NonNull FiniteDuration timeout) {
		return this.getContext().actorOf(Props.create(BucketActionDistributionActor.class, timeout));
	}

	/** Factory method for getting a "choose" actor (picks a destination randomly from a pool of replies)
	 */
	protected ActorRef getNewChooseActor(final @NonNull FiniteDuration timeout) {
		return this.getContext().actorOf(Props.create(BucketActionChooseActor.class, timeout));
	}

	/** Internal request message for forwarding from the supervisor to its children
	 * @author acp
	 */
	private static class RequestMessage {
		protected RequestMessage(
				final @NonNull Class<? extends Actor> actor_type,
				final @NonNull BucketActionMessage message,
				final @NonNull Optional<FiniteDuration> timeout)
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
				askDistributionActor(final @NonNull ActorRef supervisor, 
						final @NonNull BucketActionMessage message, 
						final @NonNull Optional<FiniteDuration> timeout)
	{
		RequestMessage m = new RequestMessage(BucketActionDistributionActor.class, message, timeout);
		//(the 2* ensures that it "always" normally the bucket that times out, which is more controlled)
		return FutureUtils.wrap(Patterns.ask(supervisor, m, 2*timeout.orElse(DEFAULT_TIMEOUT).toMillis()));
	}
	
	/** Send an action message to the multi-node distribution actor, get a future containing the reply 
	 * @param supervisor - the (probably singleton
	 * @param message - the message to send 
	 * @param timeout - message timeout
	 * @return the future containing a collection of replies
	 */
	public static CompletableFuture<BucketActionReplyMessage.BucketActionCollectedRepliesMessage> 
				askChooseActor(final @NonNull ActorRef supervisor, 
					final @NonNull BucketActionMessage message, 
					final @NonNull Optional<FiniteDuration> timeout)
	{
		RequestMessage m = new RequestMessage(BucketActionChooseActor.class, message, timeout);
		//(the 2* ensures that it "always" normally the bucket that times out, which is more controlled)
		return FutureUtils.wrap(Patterns.ask(supervisor, m, 2*timeout.orElse(DEFAULT_TIMEOUT).toMillis()));
	}
	/* (non-Javadoc)
	 * @see akka.actor.UntypedActor#onReceive(java.lang.Object)
	 */
	@Override
	public void onReceive(Object untyped_message) throws Exception {
		if (untyped_message instanceof RequestMessage) {
			RequestMessage message = (RequestMessage) untyped_message;
			ActorRef new_child = this.context().actorOf(Props.create(message.actor_type, message.timeout));
			
			new_child.forward(message.message, this.context());
		}
		else {
			this.unhandled(untyped_message);
		}
	}
}
