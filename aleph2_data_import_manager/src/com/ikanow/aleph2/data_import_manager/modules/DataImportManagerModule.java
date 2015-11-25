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
package com.ikanow.aleph2.data_import_manager.modules;

import java.io.File;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;

import scala.concurrent.duration.Duration;
import akka.actor.ActorRef;
import akka.actor.Props;

import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Scopes;
import com.ikanow.aleph2.data_import_manager.analytics.services.AnalyticStateTriggerCheckFactory;
import com.ikanow.aleph2.data_import_manager.data_model.DataImportConfigurationBean;
import com.ikanow.aleph2.data_import_manager.governance.actors.DataAgeOutSupervisor;
import com.ikanow.aleph2.data_import_manager.services.DataImportActorContext;
import com.ikanow.aleph2.data_import_manager.utils.ActorNameUtils;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.data_model.utils.PropertiesUtils;
import com.ikanow.aleph2.distributed_services.data_model.DistributedServicesPropertyBean;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.management_db.services.ManagementDbActorContext;
import com.ikanow.aleph2.management_db.utils.ActorUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import fj.data.Either;

/** Main module for V1 synchronization service
 *  THIS CLASS HAS NO COVERAGE SO NEED TO HANDLE TEST ON MODIFICATION
 * @author acp
 */
public class DataImportManagerModule {
	private static final Logger _logger = LogManager.getLogger();	

	protected final DataImportConfigurationBean _service_config;
	
	protected final IServiceContext _context;
	protected final IManagementDbService _core_management_db;
	protected final IManagementDbService _underlying_management_db;
	protected final ICoreDistributedServices _core_distributed_services;
	protected final ManagementDbActorContext _db_actor_context;
	protected final DataImportActorContext _local_actor_context;
	
	/** Launches the service
	 * @param context
	 */
	@Inject
	public DataImportManagerModule(IServiceContext context, DataImportActorContext local_actor_context, DataImportConfigurationBean service_config) {
		_context = context;
		_core_management_db = context.getCoreManagementDbService();
		_underlying_management_db = _context.getService(IManagementDbService.class, Optional.empty()).get();
		_core_distributed_services = _context.getService(ICoreDistributedServices.class, Optional.empty()).get();
		_db_actor_context = new ManagementDbActorContext(_context);
		
		_local_actor_context = local_actor_context;
		
		_service_config = service_config;
	}
	
	public void start() {		
		final String hostname = _local_actor_context.getInformationService().getHostname();
		final int MAX_ZK_ATTEMPTS = 6;
		
		if (!_core_distributed_services.waitForAkkaJoin(Optional.of(Duration.create(60L, TimeUnit.SECONDS)))) {
			_core_distributed_services.getAkkaSystem().terminate(); // (last ditch attempt to recover)
			throw new RuntimeException("Problem with CDS/Akka, try to terminate");
		}
		
		////////////////////////////////////////////////////////////////
		
		// HARVEST
		
		if (_service_config.harvest_enabled()) {
			// Create a bucket change actor and register it vs the local message bus
			final ActorRef handler = _local_actor_context.getActorSystem().actorOf(
					Props.create(com.ikanow.aleph2.data_import_manager.harvest.actors.DataBucketHarvestChangeActor.class), 
					hostname + ActorNameUtils.HARVEST_BUCKET_CHANGE_SUFFIX);
			
			_logger.info(ErrorUtils.get("Attaching harvest DataBucketHarvestChangeActor {0} to bus {1}", handler, ActorUtils.BUCKET_ACTION_EVENT_BUS));
			
			_db_actor_context.getBucketActionMessageBus().subscribe(handler, ActorUtils.BUCKET_ACTION_EVENT_BUS);
	
			_logger.info(ErrorUtils.get("Registering {1} with {0}", ActorUtils.BUCKET_ACTION_ZOOKEEPER, hostname));
						
			for (int i = 0; i <= MAX_ZK_ATTEMPTS; ++i) {
				try {
					_core_distributed_services.getCuratorFramework().create()
						.creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(ActorUtils.BUCKET_ACTION_ZOOKEEPER + "/" + hostname);
					break;
				}
				catch (Exception e) {
					_logger.warn(ErrorUtils.getLongForm("Failed to register with Zookeeper: {0}, retrying={1}", e, i < MAX_ZK_ATTEMPTS));
					try { Thread.sleep(10000L); } catch (Exception __) {}
				}
			}
			Runtime.getRuntime().addShutdownHook(new Thread(Lambdas.wrap_runnable_u(() -> {
				try {
					_logger.info("Shutting down IkanowV1SynchronizationModule subservice=v1_sync_service");
				}
				catch (Throwable e) {} // logging might not still work at this point
				
				_core_distributed_services.getCuratorFramework().delete().deletingChildrenIfNeeded().forPath(ActorUtils.BUCKET_ACTION_ZOOKEEPER + "/" + hostname);
			})));
			_logger.info("Starting IkanowV1SynchronizationModule subservice=v1_sync_service");
		}
		
		////////////////////////////////////////////////////////////////
		
		// ANALYTICS		
		
		if (_service_config.analytics_enabled()) {
			// Create a analytics bucket change actor and register it vs the local message bus
			final ActorRef analytics_handler = _local_actor_context.getActorSystem().actorOf(
					Props.create(com.ikanow.aleph2.data_import_manager.analytics.actors.DataBucketAnalyticsChangeActor.class), 
					hostname + ActorNameUtils.ANALYTICS_BUCKET_CHANGE_SUFFIX);
			
			_logger.info(ErrorUtils.get("Attaching analytics DataBucketAnalyticsChangeActor {0} to bus {1}", analytics_handler, ActorUtils.BUCKET_ANALYTICS_EVENT_BUS));
			
			_db_actor_context.getAnalyticsMessageBus().subscribe(analytics_handler, ActorUtils.BUCKET_ANALYTICS_EVENT_BUS);
	
			// Create trigger supervisor and worker
			
			final Optional<ActorRef> trigger_supervisor = _core_distributed_services.createSingletonActor(
					hostname + ActorNameUtils.ANALYTICS_TRIGGER_SUPERVISOR_SUFFIX, 
						ImmutableSet.<String>builder().add(DistributedServicesPropertyBean.ApplicationNames.DataImportManager.toString()).build()
						, 
					Props.create(com.ikanow.aleph2.data_import_manager.analytics.actors.AnalyticsTriggerSupervisorActor.class));
			
			if (!trigger_supervisor.isPresent()) {
				_logger.error("Analytic trigger supervisor didn't start, unknown reason (wrong CDS application_name?)");
			}
			
			final ActorRef trigger_worker = _local_actor_context.getActorSystem().actorOf(
					Props.create(com.ikanow.aleph2.data_import_manager.analytics.actors.AnalyticsTriggerWorkerActor.class),
					hostname + ActorNameUtils.ANALYTICS_TRIGGER_WORKER_SUFFIX
					);

			_logger.info(ErrorUtils.get("Attaching analytics AnalyticsTriggerWorkerActor {0} to bus {1}", trigger_worker, ActorUtils.ANALYTICS_TRIGGER_BUS));

			_db_actor_context.getAnalyticsTriggerBus().subscribe(trigger_worker, ActorUtils.ANALYTICS_TRIGGER_BUS);
			
			_logger.info(ErrorUtils.get("Registering {1} with {0}", ActorUtils.BUCKET_ANALYTICS_ZOOKEEPER, hostname));
			
			for (int i = 0; i <= MAX_ZK_ATTEMPTS; ++i) {
				try {
					_core_distributed_services.getCuratorFramework().create()
						.creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(ActorUtils.BUCKET_ANALYTICS_ZOOKEEPER + "/" + hostname);
					break;
				}
				catch (Exception e) {
					_logger.warn(ErrorUtils.getLongForm("Failed to register with Zookeeper: {0}, retrying={1}", e, i < MAX_ZK_ATTEMPTS));
					try { Thread.sleep(10000L); } catch (Exception __) {}
				}
			}
			Runtime.getRuntime().addShutdownHook(new Thread(Lambdas.wrap_runnable_u(() -> {
				try {
					_logger.info("Shutting down IkanowV1SynchronizationModule subservice=analytics");
				}
				catch (Throwable e) {} // logging might not still work at this point
					
				_core_distributed_services.getCuratorFramework().delete().deletingChildrenIfNeeded().forPath(ActorUtils.BUCKET_ANALYTICS_ZOOKEEPER + "/" + hostname);
			})));
			_logger.info("Starting IkanowV1SynchronizationModule subservice=analytics");
		}
		
		////////////////////////////////////////////////////////////////
		
		// GOVERNANCE
				
		if (_service_config.governance_enabled()) {			
			_core_distributed_services.createSingletonActor(hostname + ".governance.actors.DataAgeOutSupervisor", 
					ImmutableSet.<String>builder().add(DistributedServicesPropertyBean.ApplicationNames.DataImportManager.toString()).build(), 
					Props.create(DataAgeOutSupervisor.class));
			
			_logger.info("Starting IkanowV1SynchronizationModule subservice=governance");
		}		
		for (;;) {
			try { Thread.sleep(10000); } catch (Exception e) {}
		}
	}
	
	/** Entry point
	 * @param args - config_file source_key harvest_tech_id
	 * @throws Exception 
	 */
	public static void main(final String[] args) {
		try {
			if (args.length < 1) {
				System.out.println("CLI: config_file");
				System.exit(-1);
			}
			System.out.println("Running with command line: " + Arrays.toString(args));
			final Config config = ConfigFactory.parseFile(new File(args[0]));
			
			final DataImportManagerModule app = ModuleUtils.initializeApplication(Arrays.asList(new Module()), Optional.of(config), Either.left(DataImportManagerModule.class));
			app.start();
		}
		catch (Throwable e) {
			_logger.error(ErrorUtils.getLongForm("Exception reached main(): {0}", e));
			try {
				e.printStackTrace();
			}
			catch (Exception e2) { // the exception failed!
			}
			System.exit(-1);
		}
	}

	/** Subclass for setting up module
	 * @author Alex
	 *
	 */
	public static class Module extends AbstractModule {
		public Module() {		
		}
		
		@Override
		protected void configure() {
			final Config config = ModuleUtils.getStaticConfig();
			try {
				final DataImportConfigurationBean bean = BeanTemplateUtils.from(PropertiesUtils.getSubConfig(config, DataImportConfigurationBean.PROPERTIES_ROOT).orElse(null), DataImportConfigurationBean.class);
				this.bind(DataImportConfigurationBean.class).toInstance(bean);
								
				this.bind(AnalyticStateTriggerCheckFactory.class).in(Scopes.SINGLETON);
			} 
			catch (Exception e) {
				throw new RuntimeException(ErrorUtils.get(ErrorUtils.INVALID_CONFIG_ERROR,
						DataImportConfigurationBean.class.toString(),
						config.getConfig(DataImportConfigurationBean.PROPERTIES_ROOT)
						), e);
			}			
		}
	}
}
