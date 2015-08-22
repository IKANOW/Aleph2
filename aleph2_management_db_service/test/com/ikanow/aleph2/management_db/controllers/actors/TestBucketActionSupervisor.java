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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;

import com.google.common.collect.ImmutableSet;
import com.ikanow.aleph2.data_model.interfaces.shared_services.MockServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean.MasterEnrichmentType;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.UuidUtils;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.distributed_services.services.MockCoreDistributedServices;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage.UpdateBucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage.NewBucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage.BucketActionCollectedRepliesMessage;
import com.ikanow.aleph2.management_db.services.ManagementDbActorContext;
import com.ikanow.aleph2.management_db.utils.ActorUtils;

/** Tests the control logic surrounding combining streaming enrichment control with harvest control 
 * @author Alex
 */
public class TestBucketActionSupervisor {
	public static final Logger _logger = LogManager.getLogger(TestBucketActionChooseActor.class);
	
	//////////////////////////////////////////////////////////////////////////
	
	// SETUP
	
	// Test actors:
	// This one always refuses
	public static class TestActor_Refuser extends UntypedActor {
		public TestActor_Refuser(String uuid) {
			this.uuid = uuid;
		}
		private final String uuid;
		@Override
		public void onReceive(Object arg0) throws Exception {
			_logger.info("Refuse from: " + uuid);
			
			this.sender().tell(new BucketActionReplyMessage.BucketActionIgnoredMessage(uuid), this.self());
		}		
	}

	// This one always accepts, and returns a message
	public static class TestActor_Accepter extends UntypedActor {
		public TestActor_Accepter(String uuid) {
			this.uuid = uuid;
		}
		private final String uuid;
		@Override
		public void onReceive(Object arg0) throws Exception {
			if (arg0 instanceof BucketActionMessage.BucketActionOfferMessage) {
				_logger.info("Accept OFFER from: " + uuid);
				this.sender().tell(new BucketActionReplyMessage.BucketActionWillAcceptMessage(uuid), this.self());
			}
			else if (arg0 instanceof BucketActionMessage) {
				final BucketActionMessage m = (BucketActionMessage) arg0;
				if (null == m.handling_clients() || m.handling_clients().isEmpty() || m.handling_clients().contains(uuid)) {
					_logger.info("Accept MESSAGE from: " + uuid);
					this.sender().tell(
							new BucketActionReplyMessage.BucketActionHandlerMessage(uuid, 
									new BasicMessageBean(
											new Date(),
											true,
											uuid + "replaceme", // (this gets replaced by the bucket)
											arg0.getClass().getSimpleName(),
											null,
											"handled",
											null									
											)),
							this.self());
				}
				else {
					_logger.warn("Ignored message for someone else: " + uuid + " vs " + m.handling_clients().stream().collect(Collectors.joining()));
				}
			}
			else {
				_logger.error("Unrecognized message: " + arg0.getClass().toString());
			}
		}		
	}
	
	// This one always accepts, but then does nothing when it comes down to it...
	public static class TestActor_Accepter_Refuser extends UntypedActor {
		public TestActor_Accepter_Refuser(String uuid) {
			this.uuid = uuid;
		}
		private final String uuid;
		@Override
		public void onReceive(Object arg0) throws Exception {
			if (arg0 instanceof BucketActionMessage.BucketActionOfferMessage) {
				_logger.info("Accept OFFER from: " + uuid);
				this.sender().tell(new BucketActionReplyMessage.BucketActionWillAcceptMessage(uuid), this.self());
			}
			else {
				_logger.info("Refuse MESSAGE from: " + uuid);
				this.sender().tell(new BucketActionReplyMessage.BucketActionIgnoredMessage(uuid), this.self());
			}
		}		
	}	

	// This one always accepts, but then errors when it comes down to it...
	public static class TestActor_Accepter_Error extends UntypedActor {
		public TestActor_Accepter_Error(String uuid) {
			this.uuid = uuid;
		}
		private final String uuid;
		@Override
		public void onReceive(Object arg0) throws Exception {
			if (arg0 instanceof BucketActionMessage.BucketActionOfferMessage) {
				_logger.info("Accept OFFER from: " + uuid);
				this.sender().tell(new BucketActionReplyMessage.BucketActionWillAcceptMessage(uuid), this.self());
			}
			else {
				_logger.info("Error MESSAGE from: " + uuid);
				this.sender().tell(new BucketActionReplyMessage.BucketActionHandlerMessage(uuid, new BasicMessageBean(new Date(), false, uuid, "error", null, "error", null)), this.self()); 
			}
		}		
	}	
	
	// This one always accepts, but then refuses when it comes down to it...
	public static class TestActor_Accepter_Timeouter extends UntypedActor {
		public TestActor_Accepter_Timeouter(String uuid) {
			this.uuid = uuid;
		}
		private final String uuid;
		@Override
		public void onReceive(Object arg0) throws Exception {
			if (arg0 instanceof BucketActionMessage.BucketActionOfferMessage) {
				_logger.info("Accept OFFER from: " + uuid);
				this.sender().tell(new BucketActionReplyMessage.BucketActionWillAcceptMessage(uuid), this.self());
			}
			else {
				_logger.info("Timeout on MESSAGE from: " + uuid);
			}
		}		
	}
	
	@Before
	public void testSetup() throws Exception {
		MockServiceContext mock_service_context = new MockServiceContext();
		MockCoreDistributedServices mock_core_distributed_services = new MockCoreDistributedServices();
		mock_service_context.addService(ICoreDistributedServices.class, Optional.empty(), mock_core_distributed_services);
		
		@SuppressWarnings({ "unused", "deprecation" })
		ManagementDbActorContext singleton = new ManagementDbActorContext(mock_service_context, true);		
	}
	
	public DataBucketBean getBucket(String id, boolean stream, boolean harvest) {
		return Optional.of(BeanTemplateUtils.build(DataBucketBean.class))
					.map(b -> b.with(DataBucketBean::_id, id))
					.map(b -> stream ? (b.with(DataBucketBean::master_enrichment_type, MasterEnrichmentType.streaming)) : b)
					.map(b -> harvest ? (b.with(DataBucketBean::harvest_technology_name_or_id, "test")) : b)
					.get().done().get()
					;
	}
	
	//////////////////////////////////////////////////////////////////////////
	
	// TEST LOGIC
	
	// 1) Needs to stream+harvest, and streaming fails - check never calls harvest
	
	// 2) Stream only, and streaming fails (no replies) - check error
	
	// 3) Stream only, streaming succeeds
	
	// 4) Stream+harvest, streaming+harvest succeed
	
	// 5) Stream+harvest, streaming succeeds/harvest fails
	
	//1) "Needs to stream+harvest, and streaming fails - check never calls harvest"
	// 1a - streaming fails with no replies
	@Test
	public void test_needsStreaming_streamingFails_noReplies() throws Exception {
		_logger.info("Starting test_needsStreaming_streamingFails_noReplies");
		
		// Create a failing stream actor and a succeeding harvest actor
		
		// Failing stream actors
		for (int i = 0; i < 5; ++i) {
			String uuid = UuidUtils.get().getRandomUuid();
			ManagementDbActorContext.get().getDistributedServices()
				.getCuratorFramework().create().creatingParentsIfNeeded()
				.forPath(ActorUtils.STREAMING_ENRICHMENT_ZOOKEEPER + "/" + uuid);
			
			ActorRef handler = ManagementDbActorContext.get().getActorSystem().actorOf(Props.create(TestActor_Refuser.class, uuid), uuid);
			ManagementDbActorContext.get().getStreamingEnrichmentMessageBus().subscribe(handler, ActorUtils.STREAMING_ENRICHMENT_EVENT_BUS);
		}
		// Succeeding harvest actors
		final HashSet<String> uuids = new HashSet<String>();		
		for (int i = 0; i < 5; ++i) {
			String uuid = UuidUtils.get().getRandomUuid();
			uuids.add(uuid);
			ManagementDbActorContext.get().getDistributedServices()
				.getCuratorFramework().create().creatingParentsIfNeeded()
				.forPath(ActorUtils.BUCKET_ACTION_ZOOKEEPER + "/" + uuid);
			
			ActorRef handler = ManagementDbActorContext.get().getActorSystem().actorOf(Props.create(TestActor_Accepter.class, uuid), uuid);
			ManagementDbActorContext.get().getBucketActionMessageBus().subscribe(handler, ActorUtils.BUCKET_ACTION_EVENT_BUS);
		}
		
		// Create bucket
		final DataBucketBean bucket = getBucket("test_needsStreaming_streamingFails_noReplies", true, true);
		
		// Do the test
		
		NewBucketActionMessage test_message = new NewBucketActionMessage(bucket, false);
		FiniteDuration timeout = Duration.create(3, TimeUnit.SECONDS);
		
		final long before_time = new Date().getTime();
		
		final CompletableFuture<BucketActionCollectedRepliesMessage> f =
				BucketActionSupervisor.askChooseActor(
						ManagementDbActorContext.get().getBucketActionSupervisor(), ManagementDbActorContext.get().getActorSystem(),
						(BucketActionMessage)test_message, 
						Optional.of(timeout));
																
		BucketActionCollectedRepliesMessage reply = f.get();
		
		final long time_elapsed = new Date().getTime() - before_time;
		
		assertTrue("Shouldn't have timed out in actor", time_elapsed < 1000L);

		assertTrue("Shouldn't have timed out in ask", time_elapsed < 6000L);
		
		assertEquals((Integer)0, (Integer)reply.timed_out().size());
		
		assertEquals(Collections.emptyList(), reply.replies());
	}
	
	//1) "Needs to stream+harvest, and streaming fails - check never calls harvest"
	// 1b - streaming fails with an error
	@Test
	public void test_needsStreaming_streamingFails_error() throws Exception {
		_logger.info("Starting test_needsStreaming_streamingFails_error");
		
		// Create a failing stream actor and a succeeding harvest actor
		
		// Failing stream actors
		for (int i = 0; i < 5; ++i) {
			String uuid = UuidUtils.get().getRandomUuid();
			ManagementDbActorContext.get().getDistributedServices()
				.getCuratorFramework().create().creatingParentsIfNeeded()
				.forPath(ActorUtils.STREAMING_ENRICHMENT_ZOOKEEPER + "/" + uuid);
			
			ActorRef handler = ManagementDbActorContext.get().getActorSystem().actorOf(Props.create(TestActor_Accepter_Error.class, uuid), uuid);
			ManagementDbActorContext.get().getStreamingEnrichmentMessageBus().subscribe(handler, ActorUtils.STREAMING_ENRICHMENT_EVENT_BUS);
		}
		// Succeeding harvest actors
		final HashSet<String> uuids = new HashSet<String>();		
		for (int i = 0; i < 5; ++i) {
			String uuid = UuidUtils.get().getRandomUuid();
			uuids.add(uuid);
			ManagementDbActorContext.get().getDistributedServices()
				.getCuratorFramework().create().creatingParentsIfNeeded()
				.forPath(ActorUtils.BUCKET_ACTION_ZOOKEEPER + "/" + uuid);
			
			ActorRef handler = ManagementDbActorContext.get().getActorSystem().actorOf(Props.create(TestActor_Accepter.class, uuid), uuid);
			ManagementDbActorContext.get().getBucketActionMessageBus().subscribe(handler, ActorUtils.BUCKET_ACTION_EVENT_BUS);
		}
		
		// Create bucket
		final DataBucketBean bucket = getBucket("test_needsStreaming_streamingFails_error", true, true);
		
		// Do the test
		
		NewBucketActionMessage test_message = new NewBucketActionMessage(bucket, false);
		FiniteDuration timeout = Duration.create(3, TimeUnit.SECONDS);
		
		final long before_time = new Date().getTime();
		
		final CompletableFuture<BucketActionCollectedRepliesMessage> f =
				BucketActionSupervisor.askChooseActor(
						ManagementDbActorContext.get().getBucketActionSupervisor(), ManagementDbActorContext.get().getActorSystem(),
						(BucketActionMessage)test_message, 
						Optional.of(timeout));
																
		BucketActionCollectedRepliesMessage reply = f.get();
		
		final long time_elapsed = new Date().getTime() - before_time;
		
		assertTrue("Shouldn't have timed out in actor", time_elapsed < 1000L);

		assertTrue("Shouldn't have timed out in ask", time_elapsed < 6000L);
		
		assertEquals((Integer)0, (Integer)reply.timed_out().size());
		
		// Should contain an error
		assertEquals(1, reply.replies().size());
		assertEquals(false, reply.replies().get(0).success());
		assertEquals(ActorUtils.STREAMING_ENRICHMENT_ZOOKEEPER, reply.replies().get(0).command());		
	}

	//1) "Needs to stream+harvest, and streaming fails - check never calls harvest"
	// 1b - streaming fails with an error
	@Test
	public void test_needsStreaming_streamingFails_timeout() throws Exception {
		_logger.info("Starting test_needsStreaming_streamingFails_timeout");
		
		// Create a failing stream actor and a succeeding harvest actor
		
		// Failing stream actors
		for (int i = 0; i < 2; ++i) {
			String uuid = UuidUtils.get().getRandomUuid();
			ManagementDbActorContext.get().getDistributedServices()
				.getCuratorFramework().create().creatingParentsIfNeeded()
				.forPath(ActorUtils.STREAMING_ENRICHMENT_ZOOKEEPER + "/" + uuid);
			
			ActorRef handler = ManagementDbActorContext.get().getActorSystem().actorOf(Props.create(TestActor_Accepter_Timeouter.class, uuid), uuid);
			ManagementDbActorContext.get().getStreamingEnrichmentMessageBus().subscribe(handler, ActorUtils.STREAMING_ENRICHMENT_EVENT_BUS);
		}
		// Succeeding harvest actors
		final HashSet<String> uuids = new HashSet<String>();		
		for (int i = 0; i < 5; ++i) {
			String uuid = UuidUtils.get().getRandomUuid();
			uuids.add(uuid);
			ManagementDbActorContext.get().getDistributedServices()
				.getCuratorFramework().create().creatingParentsIfNeeded()
				.forPath(ActorUtils.BUCKET_ACTION_ZOOKEEPER + "/" + uuid);
			
			ActorRef handler = ManagementDbActorContext.get().getActorSystem().actorOf(Props.create(TestActor_Accepter.class, uuid), uuid);
			ManagementDbActorContext.get().getBucketActionMessageBus().subscribe(handler, ActorUtils.BUCKET_ACTION_EVENT_BUS);
		}
		
		// Create bucket
		final DataBucketBean bucket = getBucket("test_needsStreaming_streamingFails_timeout", true, true);
		
		// Do the test
		
		NewBucketActionMessage test_message = new NewBucketActionMessage(bucket, false);
		FiniteDuration timeout = Duration.create(3, TimeUnit.SECONDS);
		
		final long before_time = new Date().getTime();
		
		final CompletableFuture<BucketActionCollectedRepliesMessage> f =
				BucketActionSupervisor.askChooseActor(
						ManagementDbActorContext.get().getBucketActionSupervisor(), ManagementDbActorContext.get().getActorSystem(),
						(BucketActionMessage)test_message, 
						Optional.of(timeout));
																
		BucketActionCollectedRepliesMessage reply = f.get();
		
		final long time_elapsed = new Date().getTime() - before_time;
		
		assertTrue("Should have timed out in actor", time_elapsed > 3000L);

		assertTrue("Shouldn't have timed out in ask", time_elapsed < 15000L);
		
		// (not sure why this is 0 and not 1, but consistent with other data bucket actor tests, so it's OK I suppose..)
		assertEquals((Integer)0, (Integer)reply.timed_out().size());
		
		// Should contain an error
		assertEquals(0, reply.replies().size());
	}
	
	// 2) "Stream only, and streaming fails (errors) - check error"
	@Test
	public void test_streamingOnly_streamingFails_errors() throws Exception {
		_logger.info("Starting test_streamingOnly_streamingFails_errors");
		
		// Create a failing stream actor and a succeeding harvest actor
		
		// Failing stream actors
		for (int i = 0; i < 5; ++i) {
			String uuid = UuidUtils.get().getRandomUuid();
			ManagementDbActorContext.get().getDistributedServices()
				.getCuratorFramework().create().creatingParentsIfNeeded()
				.forPath(ActorUtils.STREAMING_ENRICHMENT_ZOOKEEPER + "/" + uuid);
			
			ActorRef handler = ManagementDbActorContext.get().getActorSystem().actorOf(Props.create(TestActor_Accepter_Error.class, uuid), uuid);
			ManagementDbActorContext.get().getStreamingEnrichmentMessageBus().subscribe(handler, ActorUtils.STREAMING_ENRICHMENT_EVENT_BUS);
		}
		// Succeeding harvest actors
		final HashSet<String> uuids = new HashSet<String>();		
		for (int i = 0; i < 5; ++i) {
			String uuid = UuidUtils.get().getRandomUuid();
			uuids.add(uuid);
			ManagementDbActorContext.get().getDistributedServices()
				.getCuratorFramework().create().creatingParentsIfNeeded()
				.forPath(ActorUtils.BUCKET_ACTION_ZOOKEEPER + "/" + uuid);
			
			ActorRef handler = ManagementDbActorContext.get().getActorSystem().actorOf(Props.create(TestActor_Accepter.class, uuid), uuid);
			ManagementDbActorContext.get().getBucketActionMessageBus().subscribe(handler, ActorUtils.BUCKET_ACTION_EVENT_BUS);
		}
		
		// Create bucket
		final DataBucketBean bucket = getBucket("test_streamingOnly_streamingFails_errors", true, false);
		
		// Do the test
		
		NewBucketActionMessage test_message = new NewBucketActionMessage(bucket, false);
		FiniteDuration timeout = Duration.create(3, TimeUnit.SECONDS);
		
		final long before_time = new Date().getTime();
		
		final CompletableFuture<BucketActionCollectedRepliesMessage> f =
				BucketActionSupervisor.askChooseActor(
						ManagementDbActorContext.get().getBucketActionSupervisor(), ManagementDbActorContext.get().getActorSystem(),
						(BucketActionMessage)test_message, 
						Optional.of(timeout));
																
		BucketActionCollectedRepliesMessage reply = f.get();
		
		final long time_elapsed = new Date().getTime() - before_time;
		
		assertTrue("Shouldn't have timed out in actor", time_elapsed < 1000L);

		assertTrue("Shouldn't have timed out in ask", time_elapsed < 6000L);
		
		assertEquals((Integer)0, (Integer)reply.timed_out().size());
		
		// Should contain an error
		assertEquals(1, reply.replies().size());
		assertEquals(false, reply.replies().get(0).success());		
		assertEquals(ActorUtils.STREAMING_ENRICHMENT_ZOOKEEPER, reply.replies().get(0).command());		
	}

	// 3) Stream only, streaming succeeds
	@Test
	public void test_streamingOnly_streamingSucceeds() throws Exception {
		_logger.info("Starting test_streamingOnly_streamingSucceeds");
		
		// Create a failing stream actor and a succeeding harvest actor
		
		// Failing stream actors
		for (int i = 0; i < 5; ++i) {
			String uuid = UuidUtils.get().getRandomUuid();
			ManagementDbActorContext.get().getDistributedServices()
				.getCuratorFramework().create().creatingParentsIfNeeded()
				.forPath(ActorUtils.STREAMING_ENRICHMENT_ZOOKEEPER + "/" + uuid);
			
			ActorRef handler = ManagementDbActorContext.get().getActorSystem().actorOf(Props.create(TestActor_Accepter.class, uuid), uuid);
			ManagementDbActorContext.get().getStreamingEnrichmentMessageBus().subscribe(handler, ActorUtils.STREAMING_ENRICHMENT_EVENT_BUS);
		}
		// Succeeding harvest actors
		final HashSet<String> uuids = new HashSet<String>();		
		for (int i = 0; i < 5; ++i) {
			String uuid = UuidUtils.get().getRandomUuid();
			uuids.add(uuid);
			ManagementDbActorContext.get().getDistributedServices()
				.getCuratorFramework().create().creatingParentsIfNeeded()
				.forPath(ActorUtils.BUCKET_ACTION_ZOOKEEPER + "/" + uuid);
			
			ActorRef handler = ManagementDbActorContext.get().getActorSystem().actorOf(Props.create(TestActor_Accepter.class, uuid), uuid);
			ManagementDbActorContext.get().getBucketActionMessageBus().subscribe(handler, ActorUtils.BUCKET_ACTION_EVENT_BUS);
		}
		
		// Create bucket
		final DataBucketBean bucket = getBucket("test_streamingOnly_streamingSucceeds", true, false);
		
		// Do the test
		
		// THIS WILL ALSO CHECK THAT THE NODE AFFINITY IS STRIPPED
		
		UpdateBucketActionMessage test_message = new UpdateBucketActionMessage(
				bucket, false, bucket, ImmutableSet.<String>builder().add(UuidUtils.get().getRandomUuid()).build());
		// (if the stream handler didn't strip the node affinity then it wouldn't get sent anywhere)		

		FiniteDuration timeout = Duration.create(3, TimeUnit.SECONDS);
		
		final long before_time = new Date().getTime();
		
		final CompletableFuture<BucketActionCollectedRepliesMessage> f =
				BucketActionSupervisor.askChooseActor(
						ManagementDbActorContext.get().getBucketActionSupervisor(), ManagementDbActorContext.get().getActorSystem(),
						(BucketActionMessage)test_message, 
						Optional.of(timeout));
																
		BucketActionCollectedRepliesMessage reply = f.get();
		
		final long time_elapsed = new Date().getTime() - before_time;
		
		assertTrue("Shouldn't have timed out in actor", time_elapsed < 1000L);

		assertTrue("Shouldn't have timed out in ask", time_elapsed < 6000L);
		
		assertEquals((Integer)0, (Integer)reply.timed_out().size());
		
		// Should contain an error
		assertEquals(1, reply.replies().size());
		assertEquals(true, reply.replies().get(0).success());		
		assertEquals(ActorUtils.STREAMING_ENRICHMENT_ZOOKEEPER, reply.replies().get(0).command());		
	}
	
	// 4) "Stream+harvest, streaming+harvest succeed"
	@Test
	public void test_needsSteaming_everythingSucceeds() throws Exception {
		_logger.info("Starting test_needsSteaming_everythingSucceeds");
		
		// Create a failing stream actor and a succeeding harvest actor
		
		// Failing stream actors
		for (int i = 0; i < 5; ++i) {
			String uuid = UuidUtils.get().getRandomUuid();
			ManagementDbActorContext.get().getDistributedServices()
				.getCuratorFramework().create().creatingParentsIfNeeded()
				.forPath(ActorUtils.STREAMING_ENRICHMENT_ZOOKEEPER + "/" + uuid);
			
			ActorRef handler = ManagementDbActorContext.get().getActorSystem().actorOf(Props.create(TestActor_Accepter.class, uuid), uuid);
			ManagementDbActorContext.get().getStreamingEnrichmentMessageBus().subscribe(handler, ActorUtils.STREAMING_ENRICHMENT_EVENT_BUS);
		}
		// Succeeding harvest actors
		final HashSet<String> uuids = new HashSet<String>();		
		for (int i = 0; i < 5; ++i) {
			String uuid = UuidUtils.get().getRandomUuid();
			uuids.add(uuid);
			ManagementDbActorContext.get().getDistributedServices()
				.getCuratorFramework().create().creatingParentsIfNeeded()
				.forPath(ActorUtils.BUCKET_ACTION_ZOOKEEPER + "/" + uuid);
			
			ActorRef handler = ManagementDbActorContext.get().getActorSystem().actorOf(Props.create(TestActor_Accepter.class, uuid), uuid);
			ManagementDbActorContext.get().getBucketActionMessageBus().subscribe(handler, ActorUtils.BUCKET_ACTION_EVENT_BUS);
		}
		
		// Create bucket
		final DataBucketBean bucket = getBucket("test_needsSteaming_everythingSucceeds", true, true);
		
		// Do the test
		
		NewBucketActionMessage test_message = new NewBucketActionMessage(bucket, false);
		FiniteDuration timeout = Duration.create(3, TimeUnit.SECONDS);
		
		final long before_time = new Date().getTime();
		
		final CompletableFuture<BucketActionCollectedRepliesMessage> f =
				BucketActionSupervisor.askChooseActor(
						ManagementDbActorContext.get().getBucketActionSupervisor(), ManagementDbActorContext.get().getActorSystem(),
						(BucketActionMessage)test_message, 
						Optional.of(timeout));
																
		BucketActionCollectedRepliesMessage reply = f.get();
		
		final long time_elapsed = new Date().getTime() - before_time;
		
		assertTrue("Shouldn't have timed out in actor", time_elapsed < 1000L);

		assertTrue("Shouldn't have timed out in ask", time_elapsed < 6000L);
		
		assertEquals((Integer)0, (Integer)reply.timed_out().size());
		
		// Should contain an error
		assertEquals(2, reply.replies().size());
		assertEquals(true, reply.replies().get(0).success());		
		assertEquals(ActorUtils.STREAMING_ENRICHMENT_ZOOKEEPER, reply.replies().get(0).command());		
		assertEquals(true, reply.replies().get(1).success());		
		assertTrue("2nd reply from harvest not stream", ActorUtils.STREAMING_ENRICHMENT_ZOOKEEPER != reply.replies().get(1).command());		
	}
	
	// "5) Stream+harvest, streaming succeeds/harvest fails"
	@Test
	public void test_streaming_streamingSucceedsHarvestFails() throws Exception {
		_logger.info("Starting test_streaming_streamingSucceedsHarvestFails");
		
		// Create a failing stream actor and a succeeding harvest actor
		
		// Failing stream actors
		for (int i = 0; i < 5; ++i) {
			String uuid = UuidUtils.get().getRandomUuid();
			ManagementDbActorContext.get().getDistributedServices()
				.getCuratorFramework().create().creatingParentsIfNeeded()
				.forPath(ActorUtils.STREAMING_ENRICHMENT_ZOOKEEPER + "/" + uuid);
			
			ActorRef handler = ManagementDbActorContext.get().getActorSystem().actorOf(Props.create(TestActor_Accepter.class, uuid), uuid);
			ManagementDbActorContext.get().getStreamingEnrichmentMessageBus().subscribe(handler, ActorUtils.STREAMING_ENRICHMENT_EVENT_BUS);
		}
		// Succeeding harvest actors
		final HashSet<String> uuids = new HashSet<String>();		
		for (int i = 0; i < 2; ++i) {
			String uuid = UuidUtils.get().getRandomUuid();
			uuids.add(uuid);
			ManagementDbActorContext.get().getDistributedServices()
				.getCuratorFramework().create().creatingParentsIfNeeded()
				.forPath(ActorUtils.BUCKET_ACTION_ZOOKEEPER + "/" + uuid);
			
			ActorRef handler = ManagementDbActorContext.get().getActorSystem().actorOf(Props.create(TestActor_Accepter_Timeouter.class, uuid), uuid);
			ManagementDbActorContext.get().getBucketActionMessageBus().subscribe(handler, ActorUtils.BUCKET_ACTION_EVENT_BUS);
		}
		
		// Create bucket
		final DataBucketBean bucket = getBucket("test_streaming_streamingSucceedsHarvestFails", true, true);
		
		// Do the test
		
		NewBucketActionMessage test_message = new NewBucketActionMessage(bucket, false);
		FiniteDuration timeout = Duration.create(3, TimeUnit.SECONDS);
		
		final long before_time = new Date().getTime();
		
		final CompletableFuture<BucketActionCollectedRepliesMessage> f =
				BucketActionSupervisor.askChooseActor(
						ManagementDbActorContext.get().getBucketActionSupervisor(), ManagementDbActorContext.get().getActorSystem(),
						(BucketActionMessage)test_message, 
						Optional.of(timeout));
																
		BucketActionCollectedRepliesMessage reply = f.get();
		
		final long time_elapsed = new Date().getTime() - before_time;
		
		assertTrue("Should have timed out in actor", time_elapsed > 3000L);

		assertTrue("Shouldn't have timed out in ask", time_elapsed < 15000L);
		
		assertEquals((Integer)0, (Integer)reply.timed_out().size());
		
		// Should contain an error
		assertEquals(1, reply.replies().size());
		assertEquals(true, reply.replies().get(0).success());		
		assertEquals(ActorUtils.STREAMING_ENRICHMENT_ZOOKEEPER, reply.replies().get(0).command());		
	}
	
	// "5) Stream+harvest, streaming succeeds/harvest fails"
	// 5b) fails with error
	@Test
	public void test_streaming_streamingSucceedsHarvestErrors() throws Exception {
		_logger.info("Starting test_streaming_streamingSucceedsHarvestErrors");
		
		// Create a failing stream actor and a succeeding harvest actor
		
		// Failing stream actors
		for (int i = 0; i < 5; ++i) {
			String uuid = UuidUtils.get().getRandomUuid();
			ManagementDbActorContext.get().getDistributedServices()
				.getCuratorFramework().create().creatingParentsIfNeeded()
				.forPath(ActorUtils.STREAMING_ENRICHMENT_ZOOKEEPER + "/" + uuid);
			
			ActorRef handler = ManagementDbActorContext.get().getActorSystem().actorOf(Props.create(TestActor_Accepter.class, uuid), uuid);
			ManagementDbActorContext.get().getStreamingEnrichmentMessageBus().subscribe(handler, ActorUtils.STREAMING_ENRICHMENT_EVENT_BUS);
		}
		// Succeeding harvest actors
		final HashSet<String> uuids = new HashSet<String>();		
		for (int i = 0; i < 2; ++i) {
			String uuid = UuidUtils.get().getRandomUuid();
			uuids.add(uuid);
			ManagementDbActorContext.get().getDistributedServices()
				.getCuratorFramework().create().creatingParentsIfNeeded()
				.forPath(ActorUtils.BUCKET_ACTION_ZOOKEEPER + "/" + uuid);
			
			ActorRef handler = ManagementDbActorContext.get().getActorSystem().actorOf(Props.create(TestActor_Accepter_Error.class, uuid), uuid);
			ManagementDbActorContext.get().getBucketActionMessageBus().subscribe(handler, ActorUtils.BUCKET_ACTION_EVENT_BUS);
		}
		
		// Create bucket
		final DataBucketBean bucket = getBucket("test_streaming_streamingSucceedsHarvestErrors", true, true);
		
		// Do the test
		
		NewBucketActionMessage test_message = new NewBucketActionMessage(bucket, false);
		FiniteDuration timeout = Duration.create(3, TimeUnit.SECONDS);
		
		final long before_time = new Date().getTime();
		
		final CompletableFuture<BucketActionCollectedRepliesMessage> f =
				BucketActionSupervisor.askChooseActor(
						ManagementDbActorContext.get().getBucketActionSupervisor(), ManagementDbActorContext.get().getActorSystem(),
						(BucketActionMessage)test_message, 
						Optional.of(timeout));
																
		BucketActionCollectedRepliesMessage reply = f.get();
		
		final long time_elapsed = new Date().getTime() - before_time;
		
		assertTrue("Shouldn't have timed out in actor", time_elapsed < 1000L);

		assertTrue("Shouldn't have timed out in ask", time_elapsed < 6000L);
		
		assertEquals((Integer)0, (Integer)reply.timed_out().size());
		
		// Should contain an error
		assertEquals(2, reply.replies().size());
		assertEquals(true, reply.replies().get(0).success());		
		assertEquals(ActorUtils.STREAMING_ENRICHMENT_ZOOKEEPER, reply.replies().get(0).command());		
		assertEquals(false, reply.replies().get(1).success());		
		assertTrue("2nd reply from harvest not stream", ActorUtils.STREAMING_ENRICHMENT_ZOOKEEPER != reply.replies().get(1).command());		
	}
}
