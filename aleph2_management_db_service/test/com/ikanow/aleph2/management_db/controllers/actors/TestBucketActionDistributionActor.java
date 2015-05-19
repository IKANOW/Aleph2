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

import java.util.Collections;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.UuidUtils;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage.NewBucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage.BucketActionCollectedRepliesMessage;
import com.ikanow.aleph2.management_db.services.LocalBucketActionMessageBus;
import com.ikanow.aleph2.management_db.services.ManagementDbActorContext;
import com.ikanow.aleph2.management_db.services.MockCoreDistributedServices;
import com.ikanow.aleph2.management_db.utils.ActorUtils;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;

public class TestBucketActionDistributionActor {

	// Test actors:
	// This one always refuses
	public static class TestActor_Refuser extends UntypedActor {
		public TestActor_Refuser(String uuid) {
			this.uuid = uuid;
		}
		private final String uuid;
		@Override
		public void onReceive(Object arg0) throws Exception {
			this.sender().tell(new BucketActionReplyMessage.BucketActionIgnoredMessage(uuid), this.self());
		}		
	}

	@Before
	public void testSetup() throws Exception {
		@SuppressWarnings("unused")
		ManagementDbActorContext singleton = new ManagementDbActorContext(
				new MockCoreDistributedServices(),
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
	public void distributionTest_allActorsHandle() {
		//TODO (ALEPH-19)
	}
	
	@Test
	public void distributionTest_handleIgnoreMix() {
		//TODO (ALEPH-19)
	}
	
	@Test
	public void distributionTest_ignoreHandleTimeoutMix() {
		//TODO (ALEPH-19)
	}
}
