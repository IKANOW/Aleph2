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
package com.ikanow.aleph2.data_import_manager.stream_enrichment.actors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.junit.Before;
import org.junit.Test;

import scala.concurrent.duration.Duration;
import akka.actor.ActorRef;
import akka.actor.Inbox;
import akka.actor.Props;
import akka.pattern.Patterns;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_import_manager.stream_enrichment.actors.DataBucketChangeActor;
import com.ikanow.aleph2.data_import_manager.services.DataImportActorContext;
import com.ikanow.aleph2.data_import_manager.services.GeneralInformationService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.distributed_services.utils.AkkaFutureUtils;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage.BucketActionEventBusWrapper;
import com.ikanow.aleph2.management_db.services.ManagementDbActorContext;
import com.ikanow.aleph2.management_db.utils.ActorUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

public class TestDataBucketChangeActor {

	////////////////////////////////////////////////////////////////////////////////////
	
	// TEST ENVIRONMENT	
	
	@Inject 
	protected IServiceContext _service_context = null;
	
	protected DataImportActorContext _actor_context;
	protected ManagementDbActorContext _db_actor_context;
	
	@Before
	public void setupDependencies() throws Exception {
		if (null != _service_context) {
			return;
		}
		
		final String temp_dir = System.getProperty("java.io.tmpdir") + File.separator;
		
		// OK we're going to use guice, it was too painful doing this by hand...				
		Config config = ConfigFactory.parseReader(new InputStreamReader(this.getClass().getResourceAsStream("test_data_bucket_change.properties")))
							.withValue("globals.local_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
							.withValue("globals.local_cached_jar_dir", ConfigValueFactory.fromAnyRef(temp_dir))
							.withValue("globals.distributed_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
							.withValue("globals.local_yarn_config_dir", ConfigValueFactory.fromAnyRef(temp_dir));
		
		Injector app_injector = ModuleUtils.createInjector(Arrays.asList(), Optional.of(config));	
		app_injector.injectMembers(this);
		
		_db_actor_context = new ManagementDbActorContext(_service_context);				
		
		_actor_context = new DataImportActorContext(_service_context, new GeneralInformationService());
		app_injector.injectMembers(_actor_context);
		
		// Have to do this in order for the underlying management db to live...		
		_service_context.getCoreManagementDbService();
	}
	
	@Test
	public void testSetup() {
		final String temp_dir = System.getProperty("java.io.tmpdir") + File.separator;
		
		assertTrue("setup completed - service context", _service_context != null);
		assertTrue("setup completed - services", _service_context.getCoreManagementDbService() != null);
		assertEquals(temp_dir, _service_context.getGlobalProperties().local_root_dir());
		
		if (File.separator.equals("\\")) { // windows mode!
			assertTrue("WINDOWS MODE: hadoop home needs to be set (use -Dhadoop.home.dir={HADOOP_HOME} in JAVA_OPTS)", null != System.getProperty("hadoop.home.dir"));
			assertTrue("WINDOWS MODE: hadoop home needs to exist: " + System.getProperty("hadoop.home.dir"), null != System.getProperty("hadoop.home.dir"));
		}
	}
	
	////////////////////////////////////////////////////////////////////////////////////
	
	// ACTOR TESTING	
	
	@Test
	public void test_actor() throws UnsupportedFileSystemException, IllegalArgumentException, InterruptedException, ExecutionException, TimeoutException {		
		
		// Create a bucket
		
		final DataBucketBean bucket = createBucket("test_actor");
		
		// Create an actor:
		
		final ActorRef handler = _db_actor_context.getActorSystem().actorOf(Props.create(DataBucketChangeActor.class), "test_host");
		_db_actor_context.getStreamingEnrichmentMessageBus().subscribe(handler, ActorUtils.STREAMING_ENRICHMENT_EVENT_BUS);

		// create the inbox:
		final Inbox inbox = Inbox.create(_actor_context.getActorSystem());		
		
		// 1) A message that it will ignore because it's not for this actor
		{
			final BucketActionMessage.DeleteBucketActionMessage delete =
					new BucketActionMessage.DeleteBucketActionMessage(bucket, new HashSet<String>(Arrays.asList("a", "b")));
			
			inbox.send(handler, delete);
			try {
				inbox.receive(Duration.create(1L, TimeUnit.SECONDS));
				fail("should have timed out");
			}
			catch (Exception e) {
				assertEquals(TimeoutException.class, e.getClass());
			}
		}
		
		// 2) Send an offer
		{
			final BucketActionMessage.BucketActionOfferMessage broadcast =
					new BucketActionMessage.BucketActionOfferMessage(bucket);
			
			_db_actor_context.getStreamingEnrichmentMessageBus().publish(new BucketActionEventBusWrapper(inbox.getRef(), broadcast));
			
			final Object msg = inbox.receive(Duration.create(5L, TimeUnit.SECONDS));
		
			assertEquals(BucketActionReplyMessage.BucketActionWillAcceptMessage.class, msg.getClass());
		}
		
		// 3) Send a message
		//TODO
		{
			final BucketActionMessage.UpdateBucketStateActionMessage suspend =
					new BucketActionMessage.UpdateBucketStateActionMessage(bucket, true, new HashSet<String>(Arrays.asList(_actor_context.getInformationService().getHostname())));
			
			final CompletableFuture<BucketActionReplyMessage> reply4 = AkkaFutureUtils.efficientWrap(Patterns.ask(handler, suspend, 5000L), _db_actor_context.getActorSystem().dispatcher());
			final BucketActionReplyMessage msg4 = reply4.get();
		
			assertEquals(BucketActionReplyMessage.BucketActionHandlerMessage.class, msg4.getClass());
			final BucketActionReplyMessage.BucketActionHandlerMessage msg4b =  (BucketActionReplyMessage.BucketActionHandlerMessage) msg4;
			
			//TODO: currently fails
			assertEquals(false, msg4b.reply().success());
		}		
	}	

	////////////////////////////////////////////////////////////////////////////////////
	
	// UTILS
	
	protected DataBucketBean createBucket(final String harvest_tech_id) {
		//TODO: make this be something streaming instead
		return BeanTemplateUtils.build(DataBucketBean.class)
							.with(DataBucketBean::_id, "test1")
							.with(DataBucketBean::full_name, "/test/path/")
							.with(DataBucketBean::harvest_technology_name_or_id, harvest_tech_id)
							.done().get();
	}
	
}
