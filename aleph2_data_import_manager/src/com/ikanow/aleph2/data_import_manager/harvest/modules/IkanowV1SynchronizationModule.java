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

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class IkanowV1SynchronizationModule {

	protected final IServiceContext _context;
	protected final IManagementDbService _core_management_db;
	protected final IManagementDbService _underlying_management_db;
	
	/** Launches the service
	 * @param context
	 */
	@Inject
	public IkanowV1SynchronizationModule(IServiceContext context) {
		_context = context;
		_core_management_db = context.getCoreManagementDbService();
		_underlying_management_db = _context.getService(IManagementDbService.class, Optional.empty());
	}
	
	public void start() {
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
			Config config = ConfigFactory.parseFile(new File(args[0]));
			
			Injector app_injector = ModuleUtils.createInjector(Arrays.asList(), Optional.of(config));
			
			IkanowV1SynchronizationModule app = app_injector.getInstance(IkanowV1SynchronizationModule.class);
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
}
