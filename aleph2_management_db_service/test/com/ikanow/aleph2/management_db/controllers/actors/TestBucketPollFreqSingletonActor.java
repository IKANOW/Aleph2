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

import java.io.File;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.data_import.HarvestControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean.MasterEnrichmentType;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.data_model.utils.UuidUtils;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.distributed_services.services.MockCoreDistributedServices;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage;
import com.ikanow.aleph2.management_db.services.ManagementDbActorContext;
import com.ikanow.aleph2.management_db.utils.ActorUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

public class TestBucketPollFreqSingletonActor {
	private static final Logger _logger = LogManager.getLogger();	

	@Inject 
	protected IServiceContext _service_context = null;	
	
	protected ICoreDistributedServices _cds = null;
	protected IManagementDbService _core_mgmt_db = null;
	protected ManagementDbActorContext _actor_context = null;
	
	// This one always accepts, and returns a message
	public static class TestActor_Accepter extends UntypedActor {
		public static long num_accepted_messages = 0;
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
			else {
				_logger.info("Accept MESSAGE from: " + uuid);
				num_accepted_messages++;
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
		}		
	}
	
	@SuppressWarnings("deprecation")
	@Before
	public void testSetup() throws Exception {
		
		if (null != _service_context) {
			return;
		}
		final String temp_dir = System.getProperty("java.io.tmpdir") + File.separator;
		
		// OK we're going to use guice, it was too painful doing this by hand...				
		Config config = ConfigFactory.parseReader(new InputStreamReader(this.getClass().getResourceAsStream("actor_test.properties")))
							.withValue("globals.local_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
							.withValue("globals.local_cached_jar_dir", ConfigValueFactory.fromAnyRef(temp_dir))
							.withValue("globals.distributed_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
							.withValue("globals.local_yarn_config_dir", ConfigValueFactory.fromAnyRef(temp_dir));
		
		Injector app_injector = ModuleUtils.createTestInjector(Arrays.asList(), Optional.of(config));	
		app_injector.injectMembers(this);
		
		_cds = _service_context.getService(ICoreDistributedServices.class, Optional.empty()).get();
		MockCoreDistributedServices mcds = (MockCoreDistributedServices) _cds;
		mcds.setApplicationName("DataImportManager");
		
		new ManagementDbActorContext(_service_context, true);		
		_actor_context = ManagementDbActorContext.get();
		
		_core_mgmt_db = _service_context.getCoreManagementDbService();		
	}
	
	/**
	 * Puts items on the test queue and checks that they have been
	 * picked up by the actor appropriately.
	 * @throws Exception 
	 */
	@Test
	public void test_bucketPollFreqSingletonActor() throws Exception {	
		// Setup: register an accepting actor to listen:
		_logger.debug("ABOUT TO ADD A NEW ACTOR");		
		insertActor(TestActor_Accepter.class);
		assertEquals("No messages should be accepted yet", 0, TestActor_Accepter.num_accepted_messages);
		//create a bucket that has a poll freq	
		final DataBucketBean bucket_poll_now = createBucket("5 min", new Date());		
		//create another that doesn't have one until later
		final DataBucketBean bucket_poll_later = createBucket("7m",  new Date(System.currentTimeMillis() + 3600000));
		//create another that doesn't have a poll frequency
		final DataBucketBean bucket_no_poll = createBucket(null, null);
		
		//put it in the datastore
		@SuppressWarnings("unchecked")
		final ICrudService<DataBucketStatusBean> underlying_crud_status = (
				ICrudService<DataBucketStatusBean>) this._core_mgmt_db.getDataBucketStatusStore().getUnderlyingPlatformDriver(ICrudService.class, Optional.empty()).get();				
		@SuppressWarnings("unchecked")
		final ICrudService<DataBucketBean> underlying_crud = (
				ICrudService<DataBucketBean>) this._core_mgmt_db.getDataBucketStore().getUnderlyingPlatformDriver(ICrudService.class, Optional.empty()).get();
		underlying_crud.deleteDatastore().get();
		underlying_crud_status.deleteDatastore().get();
		assertEquals(0, underlying_crud.countObjects().get().intValue());
		assertEquals(0, underlying_crud_status.countObjects().get().intValue());		
		storeBucketAndStatus(bucket_poll_now, true, underlying_crud_status, underlying_crud);
		storeBucketAndStatus(bucket_poll_later, true, underlying_crud_status, underlying_crud);
		storeBucketAndStatus(bucket_no_poll, true, underlying_crud_status, underlying_crud);
				
		//actor checks every second, give it long enough to hit a cycle
		Thread.sleep(10000);
		
		//get the (hopefully) updated bucket		
		final DataBucketBean bucket_poll_now_updated = underlying_crud.getObjectById(bucket_poll_now._id()).get().get();
		final DataBucketBean bucket_poll_later_updated = underlying_crud.getObjectById(bucket_poll_later._id()).get().get();
		final DataBucketBean bucket_no_poll_updated = underlying_crud.getObjectById(bucket_no_poll._id()).get().get();
		//make sure the actor 
		//1. updates freq (means it received the messaage)
		assertTrue("Next poll time should have been greater than what we set poll_time to", bucket_poll_now_updated.next_poll_date().getTime() > bucket_poll_now.next_poll_date().getTime());
		assertTrue("Poll later time should not have been updated", bucket_poll_later_updated.next_poll_date().getTime() == bucket_poll_later.next_poll_date().getTime());
		assertTrue("Poll never time should not have been set", bucket_no_poll_updated.next_poll_date() == null );
		
		//2. tries to send out poll messsage	
		assertEquals("Actor should have received 1 poll request and accepted it", 1, TestActor_Accepter.num_accepted_messages);
	}

	@After
	public void cleanupTest() {
		_actor_context.onTestComplete();
	}
	
	protected DataBucketBean createBucket(String poll_frequency, Date next_poll_date) {
		if ( poll_frequency != null ) {
			return BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::_id, UuidUtils.get().getRandomUuid())
					.with(DataBucketBean::full_name, "/test/path/" + UuidUtils.get().getRandomUuid())
					.with(DataBucketBean::owner_id, "test_owner_id")
					.with(DataBucketBean::created, new Date())
					.with(DataBucketBean::description, "asdf")
					.with(DataBucketBean::modified, new Date())
					.with(DataBucketBean::display_name, "asdf")							
					.with(DataBucketBean::poll_frequency, poll_frequency)
					.with(DataBucketBean::next_poll_date, next_poll_date)					
					.with(DataBucketBean::multi_node_enabled, false) 
					.with(DataBucketBean::master_enrichment_type, MasterEnrichmentType.batch)
					.with(DataBucketBean::harvest_technology_name_or_id, "harvest_tech")
					.with(DataBucketBean::harvest_configs, 
						Arrays.asList(BeanTemplateUtils.build(HarvestControlMetadataBean.class)
								.with(HarvestControlMetadataBean::enabled, true)
								.done().get()))
					
					.done().get();
		} else {
			return BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::_id, UuidUtils.get().getRandomUuid())
					.with(DataBucketBean::full_name, "/test/path/" + UuidUtils.get().getRandomUuid())
					.with(DataBucketBean::owner_id, "test_owner_id")
					.with(DataBucketBean::created, new Date())
					.with(DataBucketBean::description, "asdf")
					.with(DataBucketBean::modified, new Date())
					.with(DataBucketBean::display_name, "asdf")				
					.with(DataBucketBean::multi_node_enabled, false) 
					.with(DataBucketBean::master_enrichment_type, MasterEnrichmentType.batch)
					.with(DataBucketBean::harvest_technology_name_or_id, "harvest_tech")
					.with(DataBucketBean::harvest_configs, 
						Arrays.asList(BeanTemplateUtils.build(HarvestControlMetadataBean.class)
								.with(HarvestControlMetadataBean::enabled, true)
								.done().get()))
					.done().get();
		}
	}
	
	private void storeBucketAndStatus(DataBucketBean bucket,
			boolean store_status,
			ICrudService<DataBucketStatusBean> underlying_crud_status,
			ICrudService<DataBucketBean> underlying_crud) throws InterruptedException, ExecutionException {				
		underlying_crud.storeObject(bucket).get();
		if (store_status) {
			DataBucketStatusBean status_bean = BeanTemplateUtils.build(DataBucketStatusBean.class)
														.with(DataBucketStatusBean::_id, bucket._id())
														.with(DataBucketStatusBean::bucket_path, bucket.full_name())
														.with(DataBucketStatusBean::node_affinity, Collections.emptyList())
														.done().get();
			underlying_crud_status.storeObject(status_bean).get();			
		}
	}
	
	public String insertActor(Class<? extends UntypedActor> actor_clazz) throws Exception {
		String uuid = UuidUtils.get().getRandomUuid();
		ManagementDbActorContext.get().getDistributedServices()
			.getCuratorFramework().create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
			.forPath(ActorUtils.BUCKET_ACTION_ZOOKEEPER + "/" + uuid);
		
		ActorRef handler = ManagementDbActorContext.get().getActorSystem().actorOf(Props.create(actor_clazz, uuid), uuid);
		ManagementDbActorContext.get().getBucketActionMessageBus().subscribe(handler, ActorUtils.BUCKET_ACTION_EVENT_BUS);

		return uuid;
	}
}
