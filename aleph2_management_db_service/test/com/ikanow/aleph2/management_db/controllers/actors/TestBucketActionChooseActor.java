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

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import com.ikanow.aleph2.data_model.interfaces.shared_services.MockServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.UuidUtils;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.distributed_services.services.MockCoreDistributedServices;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage.NewBucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage.BucketActionCollectedRepliesMessage;
import com.ikanow.aleph2.management_db.services.ManagementDbActorContext;
import com.ikanow.aleph2.management_db.utils.ActorUtils;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;

public class TestBucketActionChooseActor {

	public static final Logger _logger = LogManager.getLogger(TestBucketActionChooseActor.class);
	
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
		public TestActor_Accepter(String uuid, boolean multi_reply) {
			this.uuid = uuid;
			this.multi_reply = multi_reply;
		}
		private final String uuid;
		private final boolean multi_reply;
		@Override
		public void onReceive(Object arg0) throws Exception {
			if (arg0 instanceof BucketActionMessage.BucketActionOfferMessage) {
				_logger.info("Accept OFFER from: " + uuid);
				this.sender().tell(new BucketActionReplyMessage.BucketActionWillAcceptMessage(uuid), this.self());
			}
			else {
				BucketActionReplyMessage.BucketActionHandlerMessage reply =
						new BucketActionReplyMessage.BucketActionHandlerMessage(uuid, 
								new BasicMessageBean(
										new Date(),
										true,
										uuid + "replaceme", // (this gets replaced by the bucket)
										arg0.getClass().getSimpleName(),
										null,
										"handled",
										null									
										));
				
				_logger.info("Accept MESSAGE from: " + uuid);
				this.sender().tell(
						multi_reply
						? new BucketActionReplyMessage.BucketActionCollectedRepliesMessage(uuid, Arrays.asList(reply.reply()), Collections.emptySet(), Collections.emptySet())
						: reply
						,
						this.self());
			}
		}		
	}
	
	// This one always accepts, but then refuses when it comes down to it...
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
	
	@Test
	public void test_distributionTest_noActors() throws InterruptedException, ExecutionException {
		
		NewBucketActionMessage test_message = new NewBucketActionMessage(
												BeanTemplateUtils.build(DataBucketBean.class).with(DataBucketBean::harvest_technology_name_or_id, "test").done().get()
												, false);
		FiniteDuration timeout = Duration.create(4, TimeUnit.SECONDS);
		
		final long before_time = new Date().getTime();
		_logger.info("sending message");		
		
		final CompletableFuture<BucketActionCollectedRepliesMessage> f =
				BucketActionSupervisor.askChooseActor(
						ManagementDbActorContext.get().getBucketActionSupervisor(), ManagementDbActorContext.get().getActorSystem(),
						(BucketActionMessage)test_message, 
						Optional.of(timeout));
																
		BucketActionCollectedRepliesMessage reply = f.get();
		_logger.info("received message reply");		
		
		final long time_elapsed = new Date().getTime() - before_time;
		
		assertTrue("Should have returned almost immediately, not timed out", time_elapsed < 4000L);
		
		assertEquals((Integer)0, (Integer)reply.timed_out().size());
		
		assertEquals(Collections.emptyList(), reply.replies());
	}

	@Test
	public void test_distributionTest_noActorsRespond() throws Exception {
		
		// Similar to the above, except this time we'll create some nodes as if there were nodes to listen on
		
		for (int i = 0; i < 5; ++i) {
			ManagementDbActorContext.get().getDistributedServices()
				.getCuratorFramework().create().creatingParentsIfNeeded()
				.forPath(ActorUtils.BUCKET_ACTION_ZOOKEEPER + "/" + UuidUtils.get().getRandomUuid());
		}
		
		// Now do the test
		
		NewBucketActionMessage test_message = new NewBucketActionMessage(
				BeanTemplateUtils.build(DataBucketBean.class).with(DataBucketBean::harvest_technology_name_or_id, "test").done().get()
				, false);
		FiniteDuration timeout = Duration.create(3, TimeUnit.SECONDS);
		
		final long before_time = new Date().getTime();
		
		final CompletableFuture<BucketActionCollectedRepliesMessage> f =
				BucketActionSupervisor.askChooseActor(
						ManagementDbActorContext.get().getBucketActionSupervisor(), ManagementDbActorContext.get().getActorSystem(),
						(BucketActionMessage)test_message, 
						Optional.of(timeout));
																
		BucketActionCollectedRepliesMessage reply = f.get();
		
		final long time_elapsed = new Date().getTime() - before_time;
		
		assertTrue("Should have timed out in actor", time_elapsed >= 3000L);

		assertTrue("Shouldn't have timed out in ask", time_elapsed < 6000L);
		
		assertEquals((Integer)5, (Integer)reply.timed_out().size());
		
		assertEquals(Collections.emptyList(), reply.replies());
	}

	@Test
	public void test_distributionTest_allActorsIgnore() throws Exception {
		
		// Similar to the above, except this time we'll create some nodes as if there were nodes to listen on
		
		for (int i = 0; i < 5; ++i) {
			String uuid = UuidUtils.get().getRandomUuid();
			ManagementDbActorContext.get().getDistributedServices()
				.getCuratorFramework().create().creatingParentsIfNeeded()
				.forPath(ActorUtils.BUCKET_ACTION_ZOOKEEPER + "/" + uuid);
			
			ActorRef handler = ManagementDbActorContext.get().getActorSystem().actorOf(Props.create(TestActor_Refuser.class, uuid), uuid);
			ManagementDbActorContext.get().getBucketActionMessageBus().subscribe(handler, ActorUtils.BUCKET_ACTION_EVENT_BUS);
		}
		
		// Now do the test
		
		NewBucketActionMessage test_message = new NewBucketActionMessage(
				BeanTemplateUtils.build(DataBucketBean.class).with(DataBucketBean::harvest_technology_name_or_id, "test").done().get()
				, false);
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
	
	/////////////////////////////////////////
	
	@Test
	public void test_distributionTest_allActorsHandle() throws Exception {
		
		// Similar to the above, except this time we'll create some nodes as if there were nodes to listen on
		
		final HashSet<String> uuids = new HashSet<String>();
		
		for (int i = 0; i < 5; ++i) {
			String uuid = UuidUtils.get().getRandomUuid();
			uuids.add(uuid);
			ManagementDbActorContext.get().getDistributedServices()
				.getCuratorFramework().create().creatingParentsIfNeeded()
				.forPath(ActorUtils.BUCKET_ACTION_ZOOKEEPER + "/" + uuid);
			
			ActorRef handler = ManagementDbActorContext.get().getActorSystem().actorOf(Props.create(TestActor_Accepter.class, uuid, true), uuid);
			ManagementDbActorContext.get().getBucketActionMessageBus().subscribe(handler, ActorUtils.BUCKET_ACTION_EVENT_BUS);
		}
		
		// Now do the test
		
		NewBucketActionMessage test_message = new NewBucketActionMessage(
				BeanTemplateUtils.build(DataBucketBean.class).with(DataBucketBean::harvest_technology_name_or_id, "test").done().get()
				, false);
		FiniteDuration timeout = Duration.create(3, TimeUnit.SECONDS);
		
		final long before_time = new Date().getTime();
		
		final CompletableFuture<BucketActionCollectedRepliesMessage> f =
				BucketActionSupervisor.askChooseActor(
						ManagementDbActorContext.get().getBucketActionSupervisor(), ManagementDbActorContext.get().getActorSystem(),
						(BucketActionMessage)test_message, 
						Optional.of(timeout));
																
		BucketActionCollectedRepliesMessage reply = f.get();
		
		final long time_elapsed = new Date().getTime() - before_time;
		
		assertTrue("Shouldn't have timed out in actor: ", time_elapsed < 1000L);

		assertTrue("Shouldn't have timed out in ask", time_elapsed < 6000L);
		
		assertEquals((Integer)0, (Integer)reply.timed_out().size());
		
		assertEquals(1, reply.replies().size());

		for (BasicMessageBean reply_bean: reply.replies()) {
		
			assertTrue("reply.source in UUID set", uuids.remove(reply_bean.source()));
			assertEquals("NewBucketActionMessage", reply_bean.command());
			assertEquals("handled", reply_bean.message());
			assertEquals(true, reply_bean.success());
		}
	}
	
	@Test
	public void test_distributionTest_handleIgnoreMix() throws Exception {
		
		final HashSet<String> accept_uuids = new HashSet<String>();
		
		for (int i = 0; i < 3; ++i) {
			String uuid = UuidUtils.get().getRandomUuid();
			accept_uuids.add(uuid);
			ManagementDbActorContext.get().getDistributedServices()
				.getCuratorFramework().create().creatingParentsIfNeeded()
				.forPath(ActorUtils.BUCKET_ACTION_ZOOKEEPER + "/" + uuid);
			
			ActorRef handler = ManagementDbActorContext.get().getActorSystem().actorOf(Props.create(TestActor_Accepter.class, uuid, false), uuid);
			ManagementDbActorContext.get().getBucketActionMessageBus().subscribe(handler, ActorUtils.BUCKET_ACTION_EVENT_BUS);
		}
		
		final HashSet<String> ignore_uuids = new HashSet<String>();
		
		for (int i = 0; i < 3; ++i) {
			String uuid = UuidUtils.get().getRandomUuid();
			ignore_uuids.add(uuid);
			ManagementDbActorContext.get().getDistributedServices()
				.getCuratorFramework().create().creatingParentsIfNeeded()
				.forPath(ActorUtils.BUCKET_ACTION_ZOOKEEPER + "/" + uuid);
			
			ActorRef handler = ManagementDbActorContext.get().getActorSystem().actorOf(Props.create(TestActor_Refuser.class, uuid), uuid);
			ManagementDbActorContext.get().getBucketActionMessageBus().subscribe(handler, ActorUtils.BUCKET_ACTION_EVENT_BUS);
		}
		
		// Now do the test
		
		NewBucketActionMessage test_message = new NewBucketActionMessage(
				BeanTemplateUtils.build(DataBucketBean.class).with(DataBucketBean::harvest_technology_name_or_id, "test").done().get()
				, false);
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
		
		assertEquals(1, reply.replies().size());

		for (BasicMessageBean reply_bean: reply.replies()) {
		
			assertTrue("reply.source in UUID set", accept_uuids.remove(reply_bean.source()));
			assertEquals("NewBucketActionMessage", reply_bean.command());
			assertEquals("handled", reply_bean.message());
			assertEquals(true, reply_bean.success());
		}
	}
	
	@Test
	public void test_distributionTest_ignoreHandleTimeoutMix() throws Exception {
		
		final HashSet<String> accept_uuids = new HashSet<String>();
		
		for (int i = 0; i < 3; ++i) {
			String uuid = UuidUtils.get().getRandomUuid();
			accept_uuids.add(uuid);
			ManagementDbActorContext.get().getDistributedServices()
				.getCuratorFramework().create().creatingParentsIfNeeded()
				.forPath(ActorUtils.BUCKET_ACTION_ZOOKEEPER + "/" + uuid);
			
			ActorRef handler = ManagementDbActorContext.get().getActorSystem().actorOf(Props.create(TestActor_Accepter.class, uuid, true), uuid);
			ManagementDbActorContext.get().getBucketActionMessageBus().subscribe(handler, ActorUtils.BUCKET_ACTION_EVENT_BUS);
		}
		
		for (int i = 0; i < 3; ++i) {
			String uuid = UuidUtils.get().getRandomUuid();
			ManagementDbActorContext.get().getDistributedServices()
				.getCuratorFramework().create().creatingParentsIfNeeded()
				.forPath(ActorUtils.BUCKET_ACTION_ZOOKEEPER + "/" + uuid);			
		}
		
		NewBucketActionMessage test_message = new NewBucketActionMessage(
				BeanTemplateUtils.build(DataBucketBean.class).with(DataBucketBean::harvest_technology_name_or_id, "test").done().get()
				, false);
		FiniteDuration timeout = Duration.create(3, TimeUnit.SECONDS);
		
		final long before_time = new Date().getTime();
		
		final CompletableFuture<BucketActionCollectedRepliesMessage> f =
				BucketActionSupervisor.askChooseActor(
						ManagementDbActorContext.get().getBucketActionSupervisor(), ManagementDbActorContext.get().getActorSystem(),
						(BucketActionMessage)test_message, 
						Optional.of(timeout));
																
		BucketActionCollectedRepliesMessage reply = f.get();
		
		final long time_elapsed = new Date().getTime() - before_time;
		
		assertTrue("Should have timed out in actor", time_elapsed >= 3000L);

		assertTrue("Shouldn't have timed out in ask", time_elapsed < 6000L);
		
		assertEquals((Integer)0, (Integer)reply.timed_out().size()); // (only get the timeout from the 0/1 requested nodes)
		
		assertEquals(1, reply.replies().size());

		for (BasicMessageBean reply_bean: reply.replies()) {
		
			assertTrue("reply.source in UUID set", accept_uuids.remove(reply_bean.source()));
			assertEquals("NewBucketActionMessage", reply_bean.command());
			assertEquals("handled", reply_bean.message());
			assertEquals(true, reply_bean.success());
		}
		
	}
	
	@Test
	public void test_distributionTest_allActorsHandle_butThenWelch() throws Exception {
		
		// Similar to the above, except this time we'll create some nodes as if there were nodes to listen on
		
		final HashSet<String> uuids = new HashSet<String>();
		
		for (int i = 0; i < 5; ++i) {
			String uuid = UuidUtils.get().getRandomUuid();
			uuids.add(uuid);
			ManagementDbActorContext.get().getDistributedServices()
				.getCuratorFramework().create().creatingParentsIfNeeded()
				.forPath(ActorUtils.BUCKET_ACTION_ZOOKEEPER + "/" + uuid);
			
			ActorRef handler = ManagementDbActorContext.get().getActorSystem().actorOf(Props.create(TestActor_Accepter_Refuser.class, uuid), uuid);
			ManagementDbActorContext.get().getBucketActionMessageBus().subscribe(handler, ActorUtils.BUCKET_ACTION_EVENT_BUS);
		}
		
		// Now do the test
		
		NewBucketActionMessage test_message = new NewBucketActionMessage(
				BeanTemplateUtils.build(DataBucketBean.class).with(DataBucketBean::harvest_technology_name_or_id, "test").done().get()
				, false);
		FiniteDuration timeout = Duration.create(3, TimeUnit.SECONDS);
		
		final long before_time = new Date().getTime();
		
		final CompletableFuture<BucketActionCollectedRepliesMessage> f =
				BucketActionSupervisor.askChooseActor(
						ManagementDbActorContext.get().getBucketActionSupervisor(), ManagementDbActorContext.get().getActorSystem(),
						(BucketActionMessage)test_message, 
						Optional.of(timeout));
																
		BucketActionCollectedRepliesMessage reply = f.get();
		
		final long time_elapsed = new Date().getTime() - before_time;
		
		assertTrue("Shouldn't have timed out in actor: ", time_elapsed < 3000L);

		assertTrue("Shouldn't have timed out in ask", time_elapsed < 6000L);
		
		assertEquals((Integer)0, (Integer)reply.timed_out().size());
		
		assertEquals(0, reply.replies().size());
	}
	
	@Test
	public void test_distributionTest_allActorsHandle_butThenTimeout() throws Exception {
		
		// Similar to the above, except this time we'll create some nodes as if there were nodes to listen on
		
		final HashSet<String> uuids = new HashSet<String>();
		
		for (int i = 0; i < 5; ++i) {
			String uuid = UuidUtils.get().getRandomUuid();
			uuids.add(uuid);
			ManagementDbActorContext.get().getDistributedServices()
				.getCuratorFramework().create().creatingParentsIfNeeded()
				.forPath(ActorUtils.BUCKET_ACTION_ZOOKEEPER + "/" + uuid);
			
			ActorRef handler = ManagementDbActorContext.get().getActorSystem().actorOf(Props.create(TestActor_Accepter_Timeouter.class, uuid), uuid);
			ManagementDbActorContext.get().getBucketActionMessageBus().subscribe(handler, ActorUtils.BUCKET_ACTION_EVENT_BUS);
		}
		
		// Now do the test
		
		NewBucketActionMessage test_message = new NewBucketActionMessage(
				BeanTemplateUtils.build(DataBucketBean.class).with(DataBucketBean::harvest_technology_name_or_id, "test").done().get()
				, false);
		FiniteDuration timeout = Duration.create(3, TimeUnit.SECONDS);
		
		final long before_time = new Date().getTime();
		
		final CompletableFuture<BucketActionCollectedRepliesMessage> f =
				BucketActionSupervisor.askChooseActor(
						ManagementDbActorContext.get().getBucketActionSupervisor(), ManagementDbActorContext.get().getActorSystem(),
						(BucketActionMessage)test_message, 
						Optional.of(timeout));
																
		BucketActionCollectedRepliesMessage reply = f.get();
		
		final long time_elapsed = new Date().getTime() - before_time;
		
		assertTrue("Should have timed out in actor: ", time_elapsed >= 3000L);

		assertTrue("Shouldn't have timed out in ask", time_elapsed < 15000L);
		
		assertEquals((Integer)0, (Integer)reply.timed_out().size());
		
		assertEquals(0, reply.replies().size());
	}
}
