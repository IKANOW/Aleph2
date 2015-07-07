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
package com.ikanow.aleph2.data_import_manager.harvest.modules;

import java.io.File;
import java.util.Arrays;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import akka.actor.ActorRef;
import akka.actor.Props;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_import_manager.data_model.DataImportConfigurationBean;
import com.ikanow.aleph2.data_import_manager.services.DataImportActorContext;
import com.ikanow.aleph2.data_import_manager.stream_enrichment.services.IStormController;
import com.ikanow.aleph2.data_import_manager.stream_enrichment.services.LocalStormController;
import com.ikanow.aleph2.data_import_manager.utils.StormControllerUtil;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.data_model.utils.PropertiesUtils;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.management_db.services.ManagementDbActorContext;
import com.ikanow.aleph2.management_db.utils.ActorUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/** Main module for V1 synchronization service
 * @author acp
 */
public class IkanowV1SynchronizationModule {
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
	public IkanowV1SynchronizationModule(IServiceContext context, DataImportActorContext local_actor_context, DataImportConfigurationBean service_config) {
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
		
		if (_service_config.harvest_enabled()) {
			// Create a bucket change actor and register it vs the local message bus
			final ActorRef handler = _local_actor_context.getActorSystem().actorOf(
					Props.create(com.ikanow.aleph2.data_import_manager.harvest.actors.DataBucketChangeActor.class), 
					hostname + ".harvest.actors.DataBucketChangeActor");
			
			_logger.info(ErrorUtils.get("Attaching harvest DataBucketChangeActor {0} to bus {1}", handler, ActorUtils.BUCKET_ACTION_EVENT_BUS));
			
			_db_actor_context.getBucketActionMessageBus().subscribe(handler, ActorUtils.BUCKET_ACTION_EVENT_BUS);
	
			_logger.info(ErrorUtils.get("Registering {1} with {0}", ActorUtils.BUCKET_ACTION_ZOOKEEPER, hostname));
			
			try {
				_core_distributed_services.getCuratorFramework().create()
					.creatingParentsIfNeeded().forPath(ActorUtils.BUCKET_ACTION_ZOOKEEPER + "/" + hostname);
			}
			catch (Exception e) {
				_logger.error(ErrorUtils.getLongForm("Failed to register with Zookeeper: {0}", e));
			}
			_logger.info("Starting IkanowV1SynchronizationModule subservice=v1_sync_service");
		}
		if (_service_config.streaming_enrichment_enabled()) {
			// Create a bucket change actor and register it vs the local message bus
			final ActorRef handler = _local_actor_context.getActorSystem().actorOf(
					Props.create(com.ikanow.aleph2.data_import_manager.stream_enrichment.actors.DataBucketChangeActor.class), 
					hostname + ".stream_enrichment.actors.DataBucketChangeActor");
			
			_logger.info(ErrorUtils.get("Attaching stream_enrichment DataBucketChangeActor {0} to bus {1}", handler, ActorUtils.STREAMING_ENRICHMENT_EVENT_BUS));
			
			_db_actor_context.getStreamingEnrichmentMessageBus().subscribe(handler, ActorUtils.STREAMING_ENRICHMENT_EVENT_BUS);
	
			_logger.info(ErrorUtils.get("Registering {1} with {0}", ActorUtils.STREAMING_ENRICHMENT_ZOOKEEPER, hostname));
			
			try {
				_core_distributed_services.getCuratorFramework().create()
					.creatingParentsIfNeeded().forPath(ActorUtils.STREAMING_ENRICHMENT_ZOOKEEPER + "/" + hostname);
			}
			catch (Exception e) {
				_logger.error(ErrorUtils.getLongForm("Failed to register with Zookeeper: {0}", e));
			}
			_logger.info("Starting IkanowV1SynchronizationModule subservice=stream_enrichment");
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
			
			final Injector app_injector = ModuleUtils.createInjector(Arrays.asList(new Module()), Optional.of(config));
			
			final IkanowV1SynchronizationModule app = app_injector.getInstance(IkanowV1SynchronizationModule.class);
			app.start();
		}
		catch (Exception e) {
			try {
				System.out.println("Got all the way to main");
				e.printStackTrace();
			}
			catch (Exception e2) { // the exception failed!
				System.out.println(ErrorUtils.getLongForm("Got all the way to main: {0}", e));
			}
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
				DataImportConfigurationBean bean = BeanTemplateUtils.from(PropertiesUtils.getSubConfig(config, DataImportConfigurationBean.PROPERTIES_ROOT).orElse(null), DataImportConfigurationBean.class);
				this.bind(DataImportConfigurationBean.class).toInstance(bean);
				
				if (bean.streaming_enrichment_enabled() && !bean.storm_debug_mode()) {
					this.bind(IStormController.class).toInstance(
							StormControllerUtil.getStormControllerFromYarnConfig(
									ModuleUtils.getGlobalProperties().local_yarn_config_dir()));					
				}
				else { // guice still needs an implementation, making it null is problematic, so we'll just bind a local controller
					this.bind(IStormController.class).toInstance(new LocalStormController());					
				}
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
