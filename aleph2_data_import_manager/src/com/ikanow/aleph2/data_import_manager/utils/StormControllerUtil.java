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
******************************************************************************/
package com.ikanow.aleph2.data_import_manager.utils;

import java.io.File;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import backtype.storm.generated.StormTopology;
import backtype.storm.generated.TopologyInfo;

import com.ikanow.aleph2.data_import.services.StreamingEnrichmentContext;
import com.ikanow.aleph2.data_import_manager.stream_enrichment.services.IStormController;
import com.ikanow.aleph2.data_import_manager.stream_enrichment.services.LocalStormController;
import com.ikanow.aleph2.data_import_manager.stream_enrichment.services.RemoteStormController;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentStreamingTopology;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.UuidUtils;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Factory for returning a local or remote storm controller.
 * 
 * Also contains static functions for using that cluster to perform various actions
 * 
 * @author Burch
 *
 */
public class StormControllerUtil {
	private static final Logger logger = LogManager.getLogger();
	
	/**
	 * Returns an instance of a local storm controller.
	 * 
	 * @return
	 */
	public static IStormController getLocalStormController() {
		return new LocalStormController();
	}
	
	/**
	 * Returns an instance of a remote storm controller pointed at the given nimbus server
	 * for storm_thrift_transport_plugin we typically use "backtype.storm.security.auth.SimpleTransportPlugin"
	 * 
	 * @param nimbus_host
	 * @param nimbus_thrift_port
	 * @param storm_thrift_transport_plugin
	 * @return
	 */
	public static IStormController getRemoteStormController(String nimbus_host, int nimbus_thrift_port, String storm_thrift_transport_plugin) {		
		return new RemoteStormController(nimbus_host, nimbus_thrift_port, storm_thrift_transport_plugin);
	}
	
	/**
	 * Returns an instance of a remote storm controller pointed at the given config
	 * 
	 * @param config
	 * @return
	 */
	public static IStormController getRemoteStormController(Map<String, Object> config) {
		return new RemoteStormController(config);
	}
	
	/**
	 * Submits a job on given storm cluster.  When submitting to a local cluster, input_jar_location
	 * can be null (it won't be used).  When submitting remotely it should be the local file path
	 * where the jar to be submitted is located.
	 * 
	 * To check the status of a job call getJobStats
	 * 
	 * @param storm_controller
	 * @param job_name
	 * @param input_jar_location
	 * @param topology
	 * @throws Exception
	 */
	public static void submitJob(IStormController storm_controller, String job_name, String input_jar_location, StormTopology topology) throws Exception {		
		storm_controller.submitJob(job_name, input_jar_location, topology);
	}
	
	/**
	 * Should stop a job on the storm cluster given the job_name, status of the stop can
	 * be checked via getJobStats
	 * 
	 * @param storm_controller
	 * @param job_name
	 * @throws Exception
	 */
	public static void stopJob(IStormController storm_controller, String job_name) throws Exception {
		storm_controller.stopJob(job_name);
	}
	
	/**
	 * Should return the job statistics for a job running on the storm cluster with the given job_name
	 * 
	 * @param storm_controller
	 * @param job_name
	 * @return
	 * @throws Exception
	 */
	public static TopologyInfo getJobStats(IStormController storm_controller, String job_name) throws Exception {
		return storm_controller.getJobStats(job_name);
	}
	
	/**
	 * Util function to create a storm jar in a random temp location.  This
	 * can be used for creating the jar to submit to submitJob.
	 * 
	 * Note: you do not want to pass the storm library to this function if you
	 * intend to use it to submit a storm job, storm jobs cannot contain that library.
	 * 
	 * Also note: JarBuilderUtil merges files in order, so if jars_to_merge[0] contains
	 * a file in the same location as jars_to_merge[1], only jars_to_merge[0] will exist in
	 * the final jar.
	 * 
	 * @param jars_to_merge
	 * @return
	 */
	public static String buildStormTopologyJar(List<String> jars_to_merge) {
		try {				
			logger.debug("creating jar to submit");
			final String input_jar_location = System.getProperty("java.io.tmpdir") + File.separator + UuidUtils.get().getTimeBasedUuid() + ".jar";
			JarBuilderUtil.mergeJars(jars_to_merge, input_jar_location);
			return input_jar_location;
		} catch (Exception e) {
			logger.error(ErrorUtils.getLongForm("Error building storm jar {0}", e));
			return null;
		}			
	}
	
	/**
	 * Returns a remote storm controller given a yarn config directory.  Will look for
	 * the storm config at yarn_config_dir/storm.properties
	 * 
	 * @param yarn_config_dir
	 * @return
	 */
	public static IStormController getStormControllerFromYarnConfig(String yarn_config_dir) {
		Config config = ConfigFactory.parseFile(new File(yarn_config_dir + File.separator + "storm.properties"));
		IStormController storm = getRemoteStormController(config.root().unwrapped());
		return storm;		
	}	
	
	/**
	 * Starts up a storm job.
	 * 
	 * 1. gets the storm instance from the yarn config
	 * 2. Makes a mega jar consisting of:
	 * 	A. Underlying artefacts (system libs)
	 *  B. User supplied libraries
	 * 3. Submit megajar to storm with jobname of the bucket id
	 * 
	 * @param bucket
	 * @param context
	 * @param yarn_config_dir
	 * @param user_lib_paths
	 * @param enrichment_toplogy
	 * @return
	 */
	public static CompletableFuture<BucketActionReplyMessage> startJob(IStormController storm_controller, DataBucketBean bucket, StreamingEnrichmentContext context, List<String> user_lib_paths, IEnrichmentStreamingTopology enrichment_toplogy) {
		CompletableFuture<BucketActionReplyMessage> start_future = new CompletableFuture<BucketActionReplyMessage>();
		//IStormController storm = getStormControllerFromYarnConfig(yarn_config_dir);

		List<String> jars_to_merge = new LinkedList<String>(); 
		//add in all the underlying artefacts file paths
		Collection<Object> underlying_artefacts = context.getUnderlyingArtefacts();
		jars_to_merge.addAll( underlying_artefacts.stream().map( artefact -> LiveInjector.findPathJar(artefact.getClass(), "")).collect(Collectors.toList()));
		
		//add in the user libs
		jars_to_merge.addAll(user_lib_paths);
		
		//create jar
		String jar_file_location = buildStormTopologyJar(jars_to_merge);
		
		//Get topology from user
		StormTopology topology = (StormTopology) enrichment_toplogy.getTopologyAndConfiguration(bucket, context)._1;
		
		//submit to storm		
		CompletableFuture<BasicMessageBean> submit_future =	storm_controller.submitJob(bucket._id(), jar_file_location, topology);
		try { 
			if ( submit_future.get().success() ) {
				start_future.complete(new BucketActionReplyMessage.BucketActionHandlerMessage("Started storm job successfully", new BasicMessageBean(new Date(), true, null, "startStormJob", 0, "Started storm job succesfully", null)));
			} else {
				start_future.complete(new BucketActionReplyMessage.BucketActionHandlerMessage("Failure to submit: " + submit_future.get().message(), submit_future.get()));				
			}						
		} catch (Exception ex ) {
			start_future.complete(new BucketActionReplyMessage.BucketActionTimeoutMessage(ErrorUtils.getLongForm("Error starting storm job: {0}", ex)));		
		}
		
		return start_future;
	}

	/**
	 * Stops a storm job, uses the bucket.id to try and find the job to stop
	 * 
	 * @param bucket
	 * @return
	 */
	public static CompletableFuture<BucketActionReplyMessage> stopJob(IStormController storm_controller, DataBucketBean bucket) {
		CompletableFuture<BucketActionReplyMessage> stop_future = new CompletableFuture<BucketActionReplyMessage>();
		try {
			storm_controller.stopJob(bucket._id());
		} catch (Exception ex) {
			stop_future.complete(new BucketActionReplyMessage.BucketActionTimeoutMessage(ErrorUtils.getLongForm("Error stopping storm job: {0}", ex)));
			return stop_future;
		}
		stop_future.complete(new BucketActionReplyMessage.BucketActionHandlerMessage("Stopped storm job successfully", new BasicMessageBean(new Date(), true, null, "stopStormJob", 0, "Stopped storm job succesfully", null)));
		return stop_future;
	}

	/**
	 * Restarts a storm job by first calling stop, then calling start
	 * 
	 * @param bucket
	 * @param context
	 * @param yarn_config_dir
	 * @param user_lib_paths
	 * @param enrichment_toplogy
	 * @return
	 */
	public static CompletableFuture<BucketActionReplyMessage> restartJob(IStormController storm_controller, DataBucketBean bucket, StreamingEnrichmentContext context, List<String> user_lib_paths, IEnrichmentStreamingTopology enrichment_toplogy) {
		CompletableFuture<BucketActionReplyMessage> stop_future = stopJob(storm_controller, bucket);
		try {
			stop_future.get(5, TimeUnit.SECONDS);
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			CompletableFuture<BucketActionReplyMessage> error_future = new CompletableFuture<BucketActionReplyMessage>();
			error_future.complete(new BucketActionReplyMessage.BucketActionTimeoutMessage(ErrorUtils.getLongForm("Error stopping storm job: {0}", e)));
		}
		return startJob(storm_controller, bucket, context, user_lib_paths, enrichment_toplogy);
	}
}
