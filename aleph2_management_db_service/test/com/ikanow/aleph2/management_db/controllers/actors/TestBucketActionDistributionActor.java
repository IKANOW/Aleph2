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

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.UuidUtils;
import com.ikanow.aleph2.distributed_services.services.MockCoreDistributedServices;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage.NewBucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage.DeleteBucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage.BucketActionCollectedRepliesMessage;
import com.ikanow.aleph2.management_db.services.LocalBucketActionMessageBus;
import com.ikanow.aleph2.management_db.services.ManagementDbActorContext;
import com.ikanow.aleph2.management_db.utils.ActorUtils;
import com.sun.istack.internal.logging.Logger;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;

public class TestBucketActionDistributionActor {

	public static final Logger _logger = Logger.getLogger(TestBucketActionDistributionActor.class);
	
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
			_logger.info("Accept from: " + uuid);
			
			this.sender().tell(
					new BucketActionReplyMessage.BucketActionHandlerMessage(uuid, 
							new BasicMessageBean(
									new Date(),
									true,
									uuid,
									arg0.getClass().getSimpleName(),
									null,
									"handled",
									null									
									)),
					this.self());
		}		
	}
	
	
	@Before
	public void testSetup() throws Exception {
		@SuppressWarnings("unused")
		ManagementDbActorContext singleton = new ManagementDbActorContext(
				new MockCoreDistributedServices(),
				null, //(need a mock ServiceContext)
				new LocalBucketActionMessageBus()
				);
	}
	
	@Test
	public void distributionTest_noActors() throws InterruptedException, ExecutionException {
		
		NewBucketActionMessage test_message = new NewBucketActionMessage(
												BeanTemplateUtils.build(DataBucketBean.class).done().get());
		FiniteDuration timeout = Duration.create(1, TimeUnit.SECONDS);
		
		final long before_time = new Date().getTime();
		
		final CompletableFuture<BucketActionCollectedRepliesMessage> f =
				BucketActionSupervisor.askDistributionActor(
						ManagementDbActorContext.get().getBucketActionSupervisor(), 
						(BucketActionMessage)test_message, 
						Optional.of(timeout));
																
		BucketActionCollectedRepliesMessage reply = f.get();
		
		final long time_elapsed = new Date().getTime() - before_time;
		
		assertTrue("Should have returned almost immediately, not timed out", time_elapsed < 1000L);
		
		assertEquals((Integer)0, (Integer)reply.timed_out());
		
		assertEquals(Collections.emptyList(), reply.replies());
	}

	@Test
	public void distributionTest_noActorsRespond() throws Exception {
		
		// Similar to the above, except this time we'll create some nodes as if there were nodes to listen on
		
		for (int i = 0; i < 5; ++i) {
			ManagementDbActorContext.get().getDistributedServices()
				.getCuratorFramework().create().creatingParentsIfNeeded()
				.forPath(ActorUtils.BUCKET_ACTION_ZOOKEEPER + "/" + UuidUtils.get().getRandomUuid());
		}
		
		// Now do the test
		
		NewBucketActionMessage test_message = new NewBucketActionMessage(
				BeanTemplateUtils.build(DataBucketBean.class).done().get());
		FiniteDuration timeout = Duration.create(1, TimeUnit.SECONDS);
		
		final long before_time = new Date().getTime();
		
		final CompletableFuture<BucketActionCollectedRepliesMessage> f =
				BucketActionSupervisor.askDistributionActor(
						ManagementDbActorContext.get().getBucketActionSupervisor(), 
						(BucketActionMessage)test_message, 
						Optional.of(timeout));
																
		BucketActionCollectedRepliesMessage reply = f.get();
		
		final long time_elapsed = new Date().getTime() - before_time;
		
		assertTrue("Should have timed out in actor", time_elapsed >= 1000L);

		assertTrue("Shouldn't have timed out in ask", time_elapsed < 2000L);
		
		assertEquals((Integer)5, (Integer)reply.timed_out());
		
		assertEquals(Collections.emptyList(), reply.replies());
	}

	@Test
	public void distributionTest_allActorsIgnore() throws Exception {
		
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
				BeanTemplateUtils.build(DataBucketBean.class).done().get());
		FiniteDuration timeout = Duration.create(1, TimeUnit.SECONDS);
		
		final long before_time = new Date().getTime();
		
		final CompletableFuture<BucketActionCollectedRepliesMessage> f =
				BucketActionSupervisor.askDistributionActor(
						ManagementDbActorContext.get().getBucketActionSupervisor(), 
						(BucketActionMessage)test_message, 
						Optional.of(timeout));
																
		BucketActionCollectedRepliesMessage reply = f.get();
		
		final long time_elapsed = new Date().getTime() - before_time;
		
		assertTrue("Shouldn't have timed out in actor", time_elapsed < 1000L);

		assertTrue("Shouldn't have timed out in ask", time_elapsed < 2000L);
		
		assertEquals((Integer)0, (Integer)reply.timed_out());
		
		assertEquals(Collections.emptyList(), reply.replies());
	}
	
	@Test
	public void distributionTest_allActorsHandle() throws Exception {
		
		// Similar to the above, except this time we'll create some nodes as if there were nodes to listen on
		
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
		
		// Now do the test
		
		NewBucketActionMessage test_message = new NewBucketActionMessage(
				BeanTemplateUtils.build(DataBucketBean.class).done().get());
		FiniteDuration timeout = Duration.create(1, TimeUnit.SECONDS);
		
		final long before_time = new Date().getTime();
		
		final CompletableFuture<BucketActionCollectedRepliesMessage> f =
				BucketActionSupervisor.askDistributionActor(
						ManagementDbActorContext.get().getBucketActionSupervisor(), 
						(BucketActionMessage)test_message, 
						Optional.of(timeout));
																
		BucketActionCollectedRepliesMessage reply = f.get();
		
		final long time_elapsed = new Date().getTime() - before_time;
		
		assertTrue("Shouldn't have timed out in actor: ", time_elapsed < 1000L);

		assertTrue("Shouldn't have timed out in ask", time_elapsed < 2000L);
		
		assertEquals((Integer)0, (Integer)reply.timed_out());
		
		assertEquals(5, reply.replies().size());

		for (BasicMessageBean reply_bean: reply.replies()) {
		
			assertTrue("reply.source in UUID set", uuids.remove(reply_bean.source()));
			assertEquals("NewBucketActionMessage", reply_bean.command());
			assertEquals("handled", reply_bean.message());
			assertEquals(true, reply_bean.success());
		}
		assertTrue("All replies received", uuids.isEmpty());
	}
	
	@Test
	public void distributionTest_handleIgnoreMix() throws Exception {
		
		final HashSet<String> accept_uuids = new HashSet<String>();
		
		for (int i = 0; i < 3; ++i) {
			String uuid = UuidUtils.get().getRandomUuid();
			accept_uuids.add(uuid);
			ManagementDbActorContext.get().getDistributedServices()
				.getCuratorFramework().create().creatingParentsIfNeeded()
				.forPath(ActorUtils.BUCKET_ACTION_ZOOKEEPER + "/" + uuid);
			
			ActorRef handler = ManagementDbActorContext.get().getActorSystem().actorOf(Props.create(TestActor_Accepter.class, uuid), uuid);
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
				BeanTemplateUtils.build(DataBucketBean.class).done().get());
		FiniteDuration timeout = Duration.create(1, TimeUnit.SECONDS);
		
		final long before_time = new Date().getTime();
		
		final CompletableFuture<BucketActionCollectedRepliesMessage> f =
				BucketActionSupervisor.askDistributionActor(
						ManagementDbActorContext.get().getBucketActionSupervisor(), 
						(BucketActionMessage)test_message, 
						Optional.of(timeout));
																
		BucketActionCollectedRepliesMessage reply = f.get();
		
		final long time_elapsed = new Date().getTime() - before_time;
		
		assertTrue("Shouldn't have timed out in actor", time_elapsed < 1000L);

		assertTrue("Shouldn't have timed out in ask", time_elapsed < 2000L);
		
		assertEquals((Integer)0, (Integer)reply.timed_out());
		
		assertEquals(3, reply.replies().size());

		for (BasicMessageBean reply_bean: reply.replies()) {
		
			assertTrue("reply.source in UUID set", accept_uuids.remove(reply_bean.source()));
			assertEquals("NewBucketActionMessage", reply_bean.command());
			assertEquals("handled", reply_bean.message());
			assertEquals(true, reply_bean.success());
		}
		assertTrue("All replies received", accept_uuids.isEmpty());
	}
	
	@Test
	public void distributionTest_ignoreHandleTimeoutMix() throws Exception {
		
		final HashSet<String> accept_uuids = new HashSet<String>();
		
		for (int i = 0; i < 3; ++i) {
			String uuid = UuidUtils.get().getRandomUuid();
			accept_uuids.add(uuid);
			ManagementDbActorContext.get().getDistributedServices()
				.getCuratorFramework().create().creatingParentsIfNeeded()
				.forPath(ActorUtils.BUCKET_ACTION_ZOOKEEPER + "/" + uuid);
			
			ActorRef handler = ManagementDbActorContext.get().getActorSystem().actorOf(Props.create(TestActor_Accepter.class, uuid), uuid);
			ManagementDbActorContext.get().getBucketActionMessageBus().subscribe(handler, ActorUtils.BUCKET_ACTION_EVENT_BUS);
		}
		
		for (int i = 0; i < 3; ++i) {
			String uuid = UuidUtils.get().getRandomUuid();
			ManagementDbActorContext.get().getDistributedServices()
				.getCuratorFramework().create().creatingParentsIfNeeded()
				.forPath(ActorUtils.BUCKET_ACTION_ZOOKEEPER + "/" + uuid);			
		}
		
		NewBucketActionMessage test_message = new NewBucketActionMessage(
				BeanTemplateUtils.build(DataBucketBean.class).done().get());
		FiniteDuration timeout = Duration.create(1, TimeUnit.SECONDS);
		
		final long before_time = new Date().getTime();
		
		final CompletableFuture<BucketActionCollectedRepliesMessage> f =
				BucketActionSupervisor.askDistributionActor(
						ManagementDbActorContext.get().getBucketActionSupervisor(), 
						(BucketActionMessage)test_message, 
						Optional.of(timeout));
																
		BucketActionCollectedRepliesMessage reply = f.get();
		
		final long time_elapsed = new Date().getTime() - before_time;
		
		assertTrue("Should have timed out in actor", time_elapsed >= 1000L);

		assertTrue("Shouldn't have timed out in ask", time_elapsed < 2000L);
		
		assertEquals((Integer)3, (Integer)reply.timed_out());
		
		assertEquals(3, reply.replies().size());

		for (BasicMessageBean reply_bean: reply.replies()) {
		
			assertTrue("reply.source in UUID set", accept_uuids.remove(reply_bean.source()));
			assertEquals("NewBucketActionMessage", reply_bean.command());
			assertEquals("handled", reply_bean.message());
			assertEquals(true, reply_bean.success());
		}
		assertTrue("All replies received", accept_uuids.isEmpty());
		
	}

	@Test
	public void distributionTest_noBroadcast() throws Exception {
		
		final HashSet<String> accept_uuids = new HashSet<String>();
		
		final HashSet<String> target_uuids = new HashSet<String>();
		
		for (int i = 0; i < 3; ++i) {
			String uuid = UuidUtils.get().getRandomUuid();
			if (target_uuids.size() < 2) {
				target_uuids.add(uuid);
			}
			
			accept_uuids.add(uuid);
			ManagementDbActorContext.get().getDistributedServices()
				.getCuratorFramework().create().creatingParentsIfNeeded()
				.forPath(ActorUtils.BUCKET_ACTION_ZOOKEEPER + "/" + uuid);
			
			ActorRef handler = ManagementDbActorContext.get().getActorSystem().actorOf(Props.create(TestActor_Accepter.class, uuid), uuid);
			ManagementDbActorContext.get().getBucketActionMessageBus().subscribe(handler, ActorUtils.BUCKET_ACTION_EVENT_BUS);
		}
		
		for (int i = 0; i < 3; ++i) {
			String uuid = UuidUtils.get().getRandomUuid();
			if (target_uuids.size() < 3) {
				target_uuids.add(uuid);
			}
			
			ManagementDbActorContext.get().getDistributedServices()
				.getCuratorFramework().create().creatingParentsIfNeeded()
				.forPath(ActorUtils.BUCKET_ACTION_ZOOKEEPER + "/" + uuid);			
			
			ActorRef handler = ManagementDbActorContext.get().getActorSystem().actorOf(Props.create(TestActor_Refuser.class, uuid), uuid);
			ManagementDbActorContext.get().getBucketActionMessageBus().subscribe(handler, ActorUtils.BUCKET_ACTION_EVENT_BUS);
		}
		
		DeleteBucketActionMessage test_message = new DeleteBucketActionMessage(
				BeanTemplateUtils.build(DataBucketBean.class).done().get(),
					target_uuids);
		
		FiniteDuration timeout = Duration.create(1, TimeUnit.SECONDS);
		
		final long before_time = new Date().getTime();
		
		final CompletableFuture<BucketActionCollectedRepliesMessage> f =
				BucketActionSupervisor.askDistributionActor(
						ManagementDbActorContext.get().getBucketActionSupervisor(), 
						(BucketActionMessage)test_message, 
						Optional.of(timeout));
																
		BucketActionCollectedRepliesMessage reply = f.get();
		
		final long time_elapsed = new Date().getTime() - before_time;
		
		assertTrue("Shouldn't have timed out in actor", time_elapsed < 1000L);

		assertTrue("Shouldn't have timed out in ask", time_elapsed < 2000L);
		
		assertEquals((Integer)0, (Integer)reply.timed_out());
		
		assertEquals(2, reply.replies().size());

		for (BasicMessageBean reply_bean: reply.replies()) {
		
			accept_uuids.remove(reply_bean.source());
			assertTrue("reply.source in UUID set: " + reply_bean.source(), target_uuids.remove(reply_bean.source()));
			assertEquals("DeleteBucketActionMessage", reply_bean.command());
			assertEquals("handled", reply_bean.message());
			assertEquals(true, reply_bean.success());
		}
		assertTrue("All expected replies received: " + Arrays.toString(target_uuids.toArray()), 1 == accept_uuids.size());
		assertTrue("All expected replies received: " + Arrays.toString(target_uuids.toArray()), 1 == target_uuids.size());
		
	}		

}
