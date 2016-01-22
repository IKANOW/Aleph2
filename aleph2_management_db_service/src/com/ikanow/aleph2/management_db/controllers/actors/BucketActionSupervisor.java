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
package com.ikanow.aleph2.management_db.controllers.actors;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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






import com.ikanow.aleph2.management_db.utils.AnalyticActorUtils;

import scala.Tuple3;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;
import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;

/** This actor just exists to manage the child actors that actually do work
 * @author acp
 */
public class BucketActionSupervisor extends UntypedActor {
	protected static final Logger _logger = LogManager.getLogger();	

	//TODO (ALEPH-19): Need a scheduled thread that runs through the retries and checks each one
	
	//TODO (ALEPH-19): Need different timeouts to handle "found harvester it's taking its time" vs "waiting internally for harvesters"
	// this is larger than it needs to be to handle that case
	public static final FiniteDuration DEFAULT_TIMEOUT = Duration.create(240, TimeUnit.SECONDS);
	
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
		protected final String message_type; // ActorUtils.BUCKET_ACTION_ZOOKEEPER or BUCKET_ANALYTICS_ZOOKEEPER 
	}
	
	/** Send an action message to the appropriate distribution actor, get a future containing the reply 
	 * @param supervisor - the (probably singleton
	 * @param message - the message to send 
	 * @param timeout - message timeout
	 * @return the future containing a collection of replies
	 */
	public static CompletableFuture<BucketActionReplyMessage.BucketActionCollectedRepliesMessage> 
				askBucketActionActor(final Optional<Boolean> multi_node_override,
						final ActorRef supervisor, final ActorSystem actor_context,
						final BucketActionMessage message, 
						final Optional<FiniteDuration> timeout)
	{
		return multi_node_override.orElseGet(() -> Optional.ofNullable(message.bucket().multi_node_enabled()).orElse(false))
				? 					
				askDistributionActor(supervisor, actor_context, message, timeout)
				:
				askChooseActor(supervisor, actor_context, message, timeout)
				;
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
	
	/** Control logic to handle either bucket type and analytics?/enrichment?/harvest?
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
		final boolean has_enrichment = hasEnrichment(bucket);
		final boolean has_analytics = !has_enrichment && bucketHasAnalytics(bucket);
		final boolean has_harvester = hasHarvester(bucket);
		
		if (!has_enrichment && !has_harvester && !has_analytics) {
			// Centralized check: if the harvest_technology_name_or_id isnt' present, nobody cares so short cut actually checking
			return CompletableFuture.completedFuture(
					new BucketActionReplyMessage.BucketActionCollectedRepliesMessage(BucketActionSupervisor.class.getSimpleName(),
							Collections.emptyList(), Collections.emptySet(), Collections.emptySet()
							));
		}
		else {
			return Lambdas.<Object, CompletableFuture<BucketActionReplyMessage.BucketActionCollectedRepliesMessage>>wrap_u(__ -> {
				if (has_enrichment) { // (enrichment + ??)
					return handleAnalyticsRequest(bucket, supervisor, actor_context, message, timeout);
				}
				else if (has_analytics) { // (analytics + ??)
					return handleAnalyticsRequests(bucket, supervisor, actor_context, message, timeout);
				}
				else { // (harvest only)
					return CompletableFuture.<BucketActionReplyMessage.BucketActionCollectedRepliesMessage>completedFuture(null);
				}				
			})
			.andThen(cf -> {
				if (has_harvester) { // (enrichment/analytics + harvest) 
					final RequestMessage m = new RequestMessage(actor_type, message, ActorUtils.BUCKET_ACTION_ZOOKEEPER, timeout);
					return cf.<BucketActionReplyMessage.BucketActionCollectedRepliesMessage>thenCompose(stream -> {							
							// Check if the stream succeeded or failed, only call if success when a create/update-enabled message
							if (!shouldStopOnAnalyticsError(message) 
								|| ((null == stream) 
									||
									(!stream.replies().isEmpty() && stream.replies().get(0).success())))
							{
								return AkkaFutureUtils.<BucketActionReplyMessage.BucketActionCollectedRepliesMessage>
									efficientWrap(akka.pattern.Patterns.ask(supervisor, m, 
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
													
													final java.util.Set<String> rejected = ImmutableSet.<String>builder()
															.addAll(stream.rejected())
															.addAll(harvest.rejected())
														.build();
													
													return new BucketActionReplyMessage.BucketActionCollectedRepliesMessage(BucketActionSupervisor.class.getSimpleName(), combined_replies, timed_out, rejected);
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
				else { // (analyics/enrichment only)					
					return cf;
				}
			})
			.apply(null);		
		}
	}
	
	///////////////////////////////////////////////
	
	// MIDDLE LEVEL UTILITIES
	
	/** Launches a single analytics job request
	 * @param bucket
	 * @param supervisor
	 * @param actor_context
	 * @param message
	 * @param timeout
	 * @return
	 */
	protected static CompletableFuture<BucketActionReplyMessage.BucketActionCollectedRepliesMessage> 
				handleAnalyticsRequest(
						DataBucketBean bucket,
						final ActorRef supervisor, final ActorSystem actor_context,
						final BucketActionMessage message, 
						final Optional<FiniteDuration> timeout)
	{
		// By construction, all the jobs have the same setting, so:
		final boolean lock_to_nodes = Optionals.of(() -> bucket.analytic_thread().jobs().stream().findAny().map(j -> j.lock_to_nodes()).get()).orElse(false);
		
		final RequestMessage m = new RequestMessage(BucketActionChooseActor.class,
				BeanTemplateUtils.clone(message).with(BucketActionMessage::handling_clients, 
						lock_to_nodes
						? message.handling_clients() // preserve node affinity for pure analytic bucket with node-locking enabled
						: Collections.emptySet() // strip node affinity (they always get distributed across available nodes)
						)
					.done(),
				ActorUtils.BUCKET_ANALYTICS_ZOOKEEPER, timeout);

		return AkkaFutureUtils.<BucketActionReplyMessage.BucketActionCollectedRepliesMessage>efficientWrap(akka.pattern.Patterns.ask(supervisor, m, 
				getTimeoutMultipler(BucketActionChooseActor.class)*timeout.orElse(DEFAULT_TIMEOUT).toMillis()), actor_context.dispatcher())
				.thenApply(stream -> {
					List<BasicMessageBean> replace = Optionals.ofNullable(stream.replies()).stream()
							.map(r -> BeanTemplateUtils.clone(r)
									.with(BasicMessageBean::command, ActorUtils.BUCKET_ANALYTICS_ZOOKEEPER)
									.done())
									.collect(Collectors.toList());

					return new BucketActionReplyMessage.BucketActionCollectedRepliesMessage(BucketActionSupervisor.class.getSimpleName(), replace, stream.timed_out(), stream.rejected());
				});
	}
														
	/** Launches a set of analytics requests and aggregates their responses
	 * @param bucket
	 * @param supervisor
	 * @param actor_context
	 * @param message
	 * @param timeout
	 * @return
	 */
	protected static CompletableFuture<BucketActionReplyMessage.BucketActionCollectedRepliesMessage> 
				handleAnalyticsRequests(
						DataBucketBean bucket,
						final ActorRef supervisor, final ActorSystem actor_context,
						final BucketActionMessage message, 
						final Optional<FiniteDuration> timeout)
	{

		final Optional<DataBucketBean> old_bucket = com.ikanow.aleph2.data_model.utils.Patterns.match(message).<Optional<DataBucketBean>>andReturn()
													.when(BucketActionMessage.UpdateBucketActionMessage.class, msg -> Optional.of(msg.old_bucket()))
													.otherwise(__ -> Optional.empty());
		
		// Split into sub-buckets
		final Map<Tuple3<String, String, MasterEnrichmentType>, DataBucketBean> sub_buckets = AnalyticActorUtils.splitAnalyticBuckets(bucket, old_bucket);
		
		// Create a stream of requests
		final List<CompletableFuture<BucketActionReplyMessage.BucketActionCollectedRepliesMessage>> results =
			sub_buckets.entrySet().stream()
					.filter(kv -> kv.getKey()._3() != MasterEnrichmentType.none)
					.map(kv -> handleAnalyticsRequest(kv.getValue(), supervisor, actor_context, message, timeout))
					.collect(Collectors.toList());

		// Aggregate the requests
		return CompletableFuture.allOf(results.toArray(new CompletableFuture<?>[0]))
			.thenApply(__ -> {
				final List<BasicMessageBean> all_replies = 
					results.stream().map(CompletableFuture::join)
							.flatMap(reply -> reply.replies().stream())
							.collect(Collectors.toList());
				
				final Set<String> all_timed_out =
						results.stream().map(CompletableFuture::join)
						.flatMap(reply -> reply.timed_out().stream())
							.collect(Collectors.toSet());
				
				final Set<String> all_rejected =
						results.stream().map(CompletableFuture::join)
						.flatMap(reply -> reply.rejected().stream())
							.collect(Collectors.toSet());
				
				return new BucketActionReplyMessage.BucketActionCollectedRepliesMessage(BucketActionSupervisor.class.getSimpleName(), all_replies, all_timed_out, all_rejected);
			});		
	}
	
	///////////////////////////////////////////////
	
	// LOW LEVEL UTILITIES
	
	/** If an enrichment/analytics message fails and the resulting action is "positive", ie "Test"
	 * @param message
	 * @return
	 */
	public static boolean shouldStopOnAnalyticsError(final BucketActionMessage message) {
		return com.ikanow.aleph2.data_model.utils.Patterns.match(message).<Boolean>andReturn()
				.when(BucketActionMessage.NewBucketActionMessage.class, __ -> true)
				.when(BucketActionMessage.TestBucketActionMessage.class, __ -> true)
				.when(BucketActionMessage.UpdateBucketActionMessage.class, msg -> msg.is_enabled(), __ -> true)
				.otherwise(() -> false);
	}
	
	/** Returns whether a bucket needs extra enrichment processing
	 * @param bucket
	 */
	public static boolean hasEnrichment(final DataBucketBean bucket) {
		return Optional.ofNullable(bucket.master_enrichment_type())
						.map(type -> type != MasterEnrichmentType.none)
						.orElse(false); // (ie if null)
	}
	
	/** Returns whether a bucket has any analytic components
	 *  Note mutually exclusive with hasEnrichment by construction
	 * @param bucket
	 * @return
	 */
	public static boolean bucketHasAnalytics(final DataBucketBean bucket) {
		return !hasEnrichment(bucket) && 
				Optional.ofNullable(bucket.analytic_thread())
						.map(analytic -> analytic.jobs())
						.flatMap(jobs -> jobs.stream()
											// (don't filter out by enabled:false ... those jobs get ignore or possibly suspended in the DataBucketChangeActor)
											.map(job -> Optional.ofNullable(job.analytic_type()).orElse(MasterEnrichmentType.none))
											.filter(type -> type != MasterEnrichmentType.none)
											.findFirst()											
							)
						.map(__ -> true)
						.orElse(false);
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
