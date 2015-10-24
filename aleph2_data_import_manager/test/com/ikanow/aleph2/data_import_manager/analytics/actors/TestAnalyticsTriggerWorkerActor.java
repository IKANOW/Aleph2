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
package com.ikanow.aleph2.data_import_manager.analytics.actors;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_import_manager.analytics.services.AnalyticStateTriggerCheckFactory;
import com.ikanow.aleph2.data_import_manager.analytics.utils.TestAnalyticTriggerCrudUtils;
import com.ikanow.aleph2.data_import_manager.data_model.DataImportConfigurationBean;
import com.ikanow.aleph2.data_import_manager.services.DataImportActorContext;
import com.ikanow.aleph2.data_import_manager.services.GeneralInformationService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.distributed_services.services.MockCoreDistributedServices;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerMessage;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerStateBean;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.services.ManagementDbActorContext;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

public class TestAnalyticsTriggerWorkerActor {

	//TODO (ALEPH-12) testing
	
	private static final Logger _logger = LogManager.getLogger();	

	@Inject 
	protected IServiceContext _service_context = null;	
	
	protected ICoreDistributedServices _cds = null;
	protected IManagementDbService _core_mgmt_db = null;
	protected IManagementDbService _under_mgmt_db = null;
	protected ManagementDbActorContext _actor_context = null;
	
	protected static AtomicLong _num_received = new AtomicLong();
	
	protected ActorRef _trigger_worker = null;
	
	ICrudService<AnalyticTriggerStateBean> _test_crud;
	
	// This one always accepts, but then refuses when it comes down to it...
	public static class TestActor extends UntypedActor {
		public TestActor() {
		}
		@Override
		public void onReceive(Object arg0) throws Exception {
			_logger.info("Received message from singleton! " + arg0.getClass().toString());
			
			if (arg0 instanceof AnalyticTriggerMessage) {
				_num_received.incrementAndGet();
			}
		}
	};
	
	@SuppressWarnings("deprecation")
	@Before
	public void test_Setup() throws Exception {
		
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
		
		Injector app_injector = ModuleUtils.createTestInjector(Arrays.asList(), Optional.of(config));	
		app_injector.injectMembers(this);
		
		_cds = _service_context.getService(ICoreDistributedServices.class, Optional.empty()).get();
		MockCoreDistributedServices mcds = (MockCoreDistributedServices) _cds;
		mcds.setApplicationName("DataImportManager");
		
		new ManagementDbActorContext(_service_context, true);		
		_actor_context = ManagementDbActorContext.get();
		
		_core_mgmt_db = _service_context.getCoreManagementDbService();		
		_under_mgmt_db = _service_context.getService(IManagementDbService.class, Optional.empty()).get();

		// need to create data import actor separately:
		@SuppressWarnings("unused")
		final DataImportActorContext singleton = new DataImportActorContext(_service_context, new GeneralInformationService(), 
				BeanTemplateUtils.build(DataImportConfigurationBean.class).done().get(),
				new AnalyticStateTriggerCheckFactory(_service_context));
		
		_trigger_worker = _actor_context.getActorSystem().actorOf(
				Props.create(com.ikanow.aleph2.data_import_manager.analytics.actors.AnalyticsTriggerWorkerActor.class),
				"test_woker"
				//hostname + ActorNameUtils.ANALYTICS_TRIGGER_WORKER_SUFFIX
				);

		_test_crud = _service_context.getService(IManagementDbService.class, Optional.empty()).get().getAnalyticBucketTriggerState(AnalyticTriggerStateBean.class);
		_test_crud.deleteDatastore().get();
		
	}
	
	@After
	public void tidyUpActor() {
		if (null != _trigger_worker) {
			_trigger_worker.tell(akka.actor.PoisonPill.getInstance(), _trigger_worker);
		}
	}
	
	@Test
	public void test_bucketLifecycle() throws InterruptedException {
		
		// Going to create a bucket, update it, and then suspend it
		// Note not much validation here, since the actual triggers etc are created by the utils functions, which are tested separately
		
		// Create the usual "manual trigger" bucket
		
		final DataBucketBean manual_trigger_bucket = TestAnalyticTriggerCrudUtils.buildBucket("/test/trigger", true);
		
		// 1) Send a message to the worker to fill in that bucket
		{
			final BucketActionMessage.NewBucketActionMessage msg = 
					new BucketActionMessage.NewBucketActionMessage(manual_trigger_bucket, false);
			
			_trigger_worker.tell(msg, _trigger_worker);
			
			// Give it a couple of secs to finish			
			Thread.sleep(2000L);
			
			// Check the DB
		
			final ICrudService<AnalyticTriggerStateBean> trigger_crud = 
					_service_context.getCoreManagementDbService().readOnlyVersion().getAnalyticBucketTriggerState(AnalyticTriggerStateBean.class);
			
			assertEquals(7L, trigger_crud.countObjects().join().intValue());
		}
		//TODO (ALEPH-12)
	}
	
	
}
