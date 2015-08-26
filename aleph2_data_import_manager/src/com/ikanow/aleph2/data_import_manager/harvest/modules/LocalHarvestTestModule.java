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
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Optional;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_import.services.HarvestContext;
import com.ikanow.aleph2.core.shared.utils.ClassloaderUtils;
import com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext;
import com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestTechnologyModule;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.SingleQueryComponent;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import fj.data.Either;
import fj.data.Validation;

/** A test module for developing harvesters (not maintained - was mainly written for early development and 
 *  at some point either needs to be made useful for standalone testing by 3rd party devs or removed)
 *  THIS CLASS HAS NO COVERAGE SO NEED TO HANDLE TEST ON MODIFICATION
 * @author Alex
 */
public class LocalHarvestTestModule {

	protected final IServiceContext _context;
	protected final IManagementDbService _core_management_db;
	protected final IManagementDbService _underlying_management_db;
	protected final GlobalPropertiesBean _globals;
	
	protected @Inject Injector _injector;
	
	/** Guice constructor, injects underlying management service (must actually be mongodb)
	 * @param management_db_service
	 */
	@Inject
	public LocalHarvestTestModule(IServiceContext service_context) {
		_context = service_context;
		_core_management_db = _context.getCoreManagementDbService();
		_underlying_management_db = _context.getService(IManagementDbService.class, Optional.empty()).get();
		_globals = service_context.getGlobalProperties();		
	}
	
	public void start(String source_key, String harvest_tech_jar_path, String... commands) throws Exception {
		if ((null == commands) || (0 == commands.length)) {
			try (Scanner scanner = new Scanner(System.in)) {
				while (true) {
					System.out.println("Enter command (or help/exit): ");
					String command = scanner.nextLine();
					if (command.equals("exit")) {
						System.exit(0);
					}
					run_command(source_key, harvest_tech_jar_path, command);
				}
			}
		}
		else for (String command: commands) {
			run_command(source_key, harvest_tech_jar_path, command);
		}
	}

	/** Entry point
	 * @param args - config_file source_key harvest_tech_id
	 * @throws Exception 
	 */
	public static void main(final String[] args) {
		try {
			if (args.length < 3) {
				System.out.println("CLI: config_file source_key harvest_tech_jar_path");
				System.exit(-1);
			}
			System.out.println("Running with command line: " + Arrays.toString(args));
			Config config = ConfigFactory.parseFile(new File(args[0]));
			
			LocalHarvestTestModule app = ModuleUtils.initializeApplication(Arrays.asList(), Optional.of(config), Either.left(LocalHarvestTestModule.class));
			app.start(args[1], args[2], Arrays.copyOfRange(args, 3, args.length));
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
	
	/** Actually perform harvester command
	 * @param source_key
	 * @param harvest_tech_jar_path
	 * @param command
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 * @throws IOException 
	 * @throws JsonMappingException 
	 * @throws JsonParseException 
	 */
	private void run_command(String source_key, String harvest_tech_jar_path, String command) throws Exception {
		
		@SuppressWarnings("unchecked")
		final ICrudService<JsonNode> v1_config_db = _underlying_management_db.getUnderlyingPlatformDriver(ICrudService.class, Optional.of("ingest.source")).get();
		
		final SingleQueryComponent<JsonNode> query = CrudUtils.allOf().when("key", source_key);
		final Optional<JsonNode> result = v1_config_db.getObjectBySpec(query).get();
		
		if (!result.isPresent()) {
			System.out.println("Must specify valid source.key: " + source_key);
			return;
		}
		
		// Create a bucket out of the source
		
		DataBucketBean bucket = createBucketFromSource(result.get());
		
		// OK now we simply create an instance of the harvester and invoke it
		
		final Validation<BasicMessageBean, IHarvestTechnologyModule> ret_val = 
				ClassloaderUtils.getFromCustomClasspath(IHarvestTechnologyModule.class, 
						"com.ikanow.aleph2.test.example.ExampleHarvestTechnology", 
						Optional.of(new File(harvest_tech_jar_path).getAbsoluteFile().toURI().toString()),
						Collections.emptyList(), "test1", command);						
						
		final IHarvestContext context = _injector.getInstance(HarvestContext.class);
		
		if (ret_val.isFail()) {
			System.out.println("Failed to instantiate harvester: " + ret_val.fail().message());
		}		
		else {
			final IHarvestTechnologyModule harvester = ret_val.success();
			if (command.equals("canRunOnThisNode")) {
				System.out.println(command + ": " + harvester.canRunOnThisNode(bucket, context));
			}
			else {
				CompletableFuture<BasicMessageBean> harvest_result = null; // (this is most of them)
				if (command.equals("onDelete")) {
					harvest_result = harvester.onDelete(bucket, context);
				}
				else if (command.equals("onHarvestComplete")) {
					harvest_result = harvester.onHarvestComplete(bucket, context);
				}
				else if (command.equals("onNewSource")) {
					harvest_result = harvester.onNewSource(bucket, context, true);
				}
				else if (command.equals("onPeriodicPoll")) {
					harvest_result = harvester.onPeriodicPoll(bucket, context);
				}
				else if (command.equals("onPurge")) {
					harvest_result = harvester.onPurge(bucket, context);
				}
				else if (command.equals("onTestSource")) {
					harvest_result = harvester.onTestSource(bucket, null, context);
				}
				else if (command.equals("onUpdatedSource")) {
					harvest_result = harvester.onUpdatedSource(bucket, bucket, true, Optional.empty(), context);
				}
				else {
					if (command.equals("help")) {
						System.out.println("Allowed commands: ");
					}
					else {
						System.out.println("Command not recognized, allowed commands: ");
					}
					System.out.println(Arrays.asList(harvester.getClass().getMethods()).stream().map(m -> m.getName()).collect(Collectors.joining(",")));
				}
				if (null != harvest_result) {
					System.out.println(command + ": success: " + harvest_result.get().success());
					System.out.println(command + ": source: " + harvest_result.get().source());
					System.out.println(command + ": message: " + harvest_result.get().message());
				}
				else {
					System.out.println("(no return value)");
				}
			}
		}
	}
	
	private static DataBucketBean createBucketFromSource(JsonNode src_json) throws Exception {
		
		@SuppressWarnings("unused")
		final String _id = safeJsonGet("_id", src_json).asText(); // (think we'll use key instead of _id?)
		final String key = safeJsonGet("key", src_json).asText();
		final String created = safeJsonGet("created", src_json).asText();
		final String modified = safeJsonGet("modified", src_json).asText();
		final String title = safeJsonGet("title", src_json).asText();
		final String description = safeJsonGet("description", src_json).asText();
		final String owner_id = safeJsonGet("ownerId", src_json).asText();
		
		final JsonNode tags = safeJsonGet("tags", src_json); // collection of strings
		//TODO (ALEPH-19): need to convert the DB authentication across to the Aleph2 format 
		@SuppressWarnings("unused")
		final JsonNode comm_ids = safeJsonGet("communityIds", src_json); // collection of strings
		final JsonNode px_pipeline = safeJsonGet("processingPipeline", src_json); // collection of JSON objects, first one should have data_bucket
		final JsonNode px_pipeline_first_el = px_pipeline.get(0);
		final JsonNode data_bucket = safeJsonGet("data_bucket", px_pipeline_first_el);
		
		final DataBucketBean bucket = BeanTemplateUtils.build(data_bucket, DataBucketBean.class)
													.with(DataBucketBean::_id, key)
													.with(DataBucketBean::created, parseJavaDate(created))
													.with(DataBucketBean::modified, parseJavaDate(modified))
													.with(DataBucketBean::display_name, title)
													.with(DataBucketBean::description, description)
													.with(DataBucketBean::owner_id, owner_id)
													.with(DataBucketBean::tags, 
															StreamSupport.stream(tags.spliterator(), false)
																			.map(jt -> jt.asText())
																			.collect(Collectors.toSet()))																	
													.done().get();
		
		return bucket;
	}
	private static Date parseJavaDate(String java_date_tostring_format) throws ParseException {
		return new SimpleDateFormat("EEE MMM d HH:mm:ss zzz yyyy").parse(java_date_tostring_format);
	}
	
	private static JsonNode safeJsonGet(String fieldname, JsonNode src) {
		final JsonNode j = Optional.ofNullable(src.get(fieldname)).orElse(JsonNodeFactory.instance.objectNode());
		//DEBUG
		//System.out.println(j);
		return j;
	}
}
