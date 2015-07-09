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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean.MasterEnrichmentType;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Optionals;
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

	//TODO (ALEPH-19): Need a scheduled thread that runs through the retries and checks each one
	
	//TODO (ALEPH-19): Need different timeouts to handle "found harvester it's taking its time" vs "waiting internally for harvesters"
	// this is larger than it needs to be to handle that case
	public static final FiniteDuration DEFAULT_TIMEOUT = Duration.create(120, TimeUnit.SECONDS);
	
	/** Internal request message for forwarding from the supervisor to its children
	 * @author acp
	 */
	private static class RequestMessage {
		protected RequestMessage(
				final Class<? extends Actor> actor_type,
				final BucketActionMessage message,
				final String message_type,
				final Optional<FiniteDuration> timeout)
		{
			this.actor_type = actor_type;
			this.message = message;
			this.message_type = message_type;
			this.timeout = timeout;
		}

		protected final Class<? extends Actor> actor_type;
		protected final BucketActionMessage message;
		protected final Optional<FiniteDuration> timeout;
		protected final String message_type; // ActorUtils.BUCKET_ACTION_ZOOKEEPER or STREAMING_ENRICHMENT_ZOOKEEPER 
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
		return controlLogic(supervisor, actor_context, message, BucketActionDistributionActor.class, timeout);
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
		return controlLogic(supervisor, actor_context, message, BucketActionChooseActor.class, timeout);
	}
	/* (non-Javadoc)
	 * @see akka.actor.UntypedActor#onReceive(java.lang.Object)
	 */
	@Override
	public void onReceive(final Object untyped_message) throws Exception {
		if (untyped_message instanceof RequestMessage) {
			RequestMessage message = (RequestMessage) untyped_message;

			ActorRef new_child = this.context().actorOf(Props.create(message.actor_type, message.timeout, message.message_type));

			new_child.forward(message.message, this.context());
		}
		else {
			this.unhandled(untyped_message);
		}
	}
	
	/** Control logic to handle either bucket type and streaming?/harvest?
	 * @param supervisor
	 * @param actor_context
	 * @param message
	 * @param actor_type
	 * @param timeout
	 * @return
	 */
	public static CompletableFuture<BucketActionReplyMessage.BucketActionCollectedRepliesMessage> controlLogic(
			final ActorRef supervisor, final ActorSystem actor_context,
			final BucketActionMessage message, final Class<? extends Actor> actor_type,
			final Optional<FiniteDuration> timeout)
	{
		final DataBucketBean bucket = message.bucket();
		final boolean is_streaming = isStreaming(bucket);
		final boolean has_harvester = hasHarvester(bucket);
		
		if (!is_streaming && !has_harvester) {
			// Centralized check: if the harvest_technology_name_or_id isnt' present, nobody cares so short cut actually checking
			return CompletableFuture.completedFuture(
					new BucketActionReplyMessage.BucketActionCollectedRepliesMessage(
							Collections.emptyList(), Collections.emptySet()
							));
		}
		else {
			return Lambdas.<Object, CompletableFuture<BucketActionReplyMessage.BucketActionCollectedRepliesMessage>>wrap_u(__ -> {
				if (is_streaming) { // (streaming + ??)
					final RequestMessage m = new RequestMessage(BucketActionChooseActor.class, message, ActorUtils.STREAMING_ENRICHMENT_ZOOKEEPER, timeout);
					return AkkaFutureUtils.<BucketActionReplyMessage.BucketActionCollectedRepliesMessage>efficientWrap(Patterns.ask(supervisor, m, 
							getTimeoutMultipler(BucketActionChooseActor.class)*timeout.orElse(DEFAULT_TIMEOUT).toMillis()), actor_context.dispatcher())
								.thenApply(stream -> {
									List<BasicMessageBean> replace = Optionals.ofNullable(stream.replies()).stream()
																		.map(r -> BeanTemplateUtils.clone(r)
																			.with(BasicMessageBean::command, ActorUtils.STREAMING_ENRICHMENT_ZOOKEEPER)
																			.done())
																		.collect(Collectors.toList());
									
									return new BucketActionReplyMessage.BucketActionCollectedRepliesMessage(replace, stream.timed_out());
								});
				}
				else { // (harvest only)
					return CompletableFuture.<BucketActionReplyMessage.BucketActionCollectedRepliesMessage>completedFuture(null);
				}				
			})
			.andThen(cf -> {
				if (has_harvester) { // (streaming + harvest) 
					final RequestMessage m = new RequestMessage(actor_type, message, ActorUtils.BUCKET_ACTION_ZOOKEEPER, timeout);
					return cf.<BucketActionReplyMessage.BucketActionCollectedRepliesMessage>thenCompose(stream -> {							
							// Check if the stream succeeded or failed, only call if the 
							if ((null == stream) ||
									(!stream.replies().isEmpty() && stream.replies().get(0).success()))
							{
								return AkkaFutureUtils.<BucketActionReplyMessage.BucketActionCollectedRepliesMessage>
									efficientWrap(Patterns.ask(supervisor, m, 
										getTimeoutMultipler(actor_type)*timeout.orElse(DEFAULT_TIMEOUT).toMillis()), actor_context.dispatcher())
											.thenApply(harvest -> {
												if (null != stream) {
													final java.util.List<BasicMessageBean> combined_replies = ImmutableList.<BasicMessageBean>builder()
																												.addAll(stream.replies())
																												.addAll(harvest.replies())
																											.build();
					
													final java.util.Set<String> timed_out = ImmutableSet.<String>builder()
																									.addAll(stream.timed_out())
																									.addAll(harvest.timed_out())
																								.build();
													return new BucketActionReplyMessage.BucketActionCollectedRepliesMessage(combined_replies, timed_out);
												}
												else {
													return harvest;
												}												
											});
							}
							else {
								return CompletableFuture.completedFuture(stream);
							}
						} );
				}
				else { // (streaming only)					
					return cf;
				}
			})
			.apply(null);		
		}
	}
	
	///////////////////////////////////////////////
	
	// LOW LEVEL UTILITIES
	
	/** Returns whether a bucket needs extra processing to set its streaming mode up
	 * @param bucket
	 */
	public static boolean isStreaming(final DataBucketBean bucket) {
		return Optional.ofNullable(bucket.master_enrichment_type())
						.map(type -> (type == MasterEnrichmentType.streaming) || (type == MasterEnrichmentType.streaming_and_batch))
						.orElse(false); // (ie if null)
	}
	
	/** Returns whether a bucket needs to be sent to 1+ harvest nodes 
	 * @param bucket
	 * @return
	 */
	public static boolean hasHarvester(final DataBucketBean bucket) {
		return (null != bucket.harvest_technology_name_or_id());
	}
	
	/** Extra multiplier of default timeout, to ensure that it is "always" (normally) the actor that times out, which is more controlled
	 *  Choose Actors are more painful because there's an extra send-ack, so use 5* instead of 2*
	 * @param actor_type
	 * @return
	 */
	public static int getTimeoutMultipler(final Class<? extends Actor> actor_type) {
		if (BucketActionDistributionActor.class.isAssignableFrom(actor_type)) {
			return 2;
		}
		if (BucketActionChooseActor.class.isAssignableFrom(actor_type)) {
			return 5;
		}
		else return 1;
	}
	
}
