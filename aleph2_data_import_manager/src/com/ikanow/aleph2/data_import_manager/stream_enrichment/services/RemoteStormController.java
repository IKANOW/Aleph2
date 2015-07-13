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
package com.ikanow.aleph2.data_import_manager.stream_enrichment.services;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift7.TException;
import org.json.simple.JSONValue;

import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.FutureUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.TopologySummary;
import backtype.storm.utils.NimbusClient;

/**
 * Hands submitting storm jobs to a remote cluster.
 * 
 * @author Burch
 *
 */
public class RemoteStormController implements IStormController  {
	private static final Logger logger = LogManager.getLogger();
	final private Map<String, Object> remote_config;
	private Client client;
	
	/**
	 * Initialize the remote client.  Need the nimbus host:port and the transport plugin.
	 * Additionally, the nimbus client will attempt to find the storm.yaml or defaults.yaml
	 * config file on the classpath.  If this is bundled with storm-core it'll use the defaults
	 * in there.  If this is put out on a server you have to deploy a version there (or get the
	 * bundled storm-core/defaults.yaml on the classpath).
	 * 
	 * Need these params minimally:
	 * nimbus.host
	 * nimbus.thrift.port
	 * storm.thrift.transport
	 * storm.meta.serialization.delegate
	 * 
	 * @param config
	 */
	public RemoteStormController(Map<String, Object> config) {
		remote_config = config;
		logger.info("Connecting to remote storm: " + remote_config.toString() );
		client = NimbusClient.getConfiguredClient(remote_config).getClient();
	}
	
	/**
	 * Initialize the remote client.  Need the nimbus host:port and the transport plugin.
	 * Additionally, the nimbus client will attempt to find the storm.yaml or defaults.yaml
	 * config file on the classpath.  If this is bundled with storm-core it'll use the defaults
	 * in there.  If this is put out on a server you have to deploy a version there (or get the
	 * bundled storm-core/defaults.yaml on the classpath).
	 * 
	 * @param nimbus_host
	 * @param nimbus_thrift_port
	 * @param storm_thrift_transport_plugin typically "backtype.storm.security.auth.SimpleTransportPlugin"
	 */
	public RemoteStormController(String nimbus_host, int nimbus_thrift_port, String storm_thrift_transport_plugin) {
		remote_config = new HashMap<String, Object>();
		remote_config.put(Config.NIMBUS_HOST, nimbus_host);
		remote_config.put(Config.NIMBUS_THRIFT_PORT, nimbus_thrift_port);
		remote_config.put(Config.STORM_THRIFT_TRANSPORT_PLUGIN, storm_thrift_transport_plugin);	
		remote_config.put(Config.STORM_META_SERIALIZATION_DELEGATE, "todo"); //TODO (ALEPH-10) need to find the correct file for this, throws an error in the logs currently and loads a default
		logger.info("Connecting to remote storm: " + remote_config.toString() );
		client = NimbusClient.getConfiguredClient(remote_config).getClient();
	}

	/**
	 * Submits a job to the remote storm cluster.  Sends the input jar to the server and then
	 * submits the supplied topology w/ the given job_name.
	 * 
	 */
	@Override
	public CompletableFuture<BasicMessageBean> submitJob(String job_name, String input_jar_location,
			StormTopology topology)  {
		CompletableFuture<BasicMessageBean> future = new CompletableFuture<BasicMessageBean>();
		logger.info("Submitting job: " + job_name + " jar: " + input_jar_location);
		logger.info("submitting jar");		
		String remote_jar_location = StormSubmitter.submitJar(remote_config, input_jar_location);
		String json_conf = JSONValue.toJSONString(remote_config);
		logger.info("submitting topology");
		try {
			synchronized (client) {
				client.submitTopology(job_name, remote_jar_location, json_conf, topology);
			}
			//verify job was assigned some executors
			TopologyInfo info = getJobStats(job_name);
			logger.info("submitted job received: " + info.get_executors_size() + " executors");
			if ( info.get_executors_size() == 0 ) {
				logger.info("received 0 executors, killing job, reporting failure");
				//no executors were available for this job, stop the job, throw an error
				stopJob(job_name);
				future.complete(new BasicMessageBean(new Date(), false, null, "onNewSource", null, "No executors were assigned to this job, typically this is because too many jobs are currently running, kill some other jobs and resubmit.", null));
				return future;					
			}
		} catch (Exception ex ) {
			logger.info( ErrorUtils.getLongForm("Error submitting job: " + job_name + ": {0}", ex));
			return FutureUtils.returnError(ex);
		}
		
		future.complete(new BasicMessageBean(new Date(), true, null, "submit job", 0, "Submitted job successfully", null));
		return future;
	}

	/**
	 * Attempts to stop the given job_name, if the job is not found it will throw an exception.
	 * 
	 */
	@Override
	public CompletableFuture<BasicMessageBean> stopJob(String job_name) {
		logger.info("Stopping job: " + job_name);
		CompletableFuture<BasicMessageBean> future = new CompletableFuture<BasicMessageBean>();
		
		try {
			String actual_job_name = getJobTopologySummaryFromJobPrefix(job_name).get_name();
			if ( actual_job_name != null ) { 
				synchronized (client) {
					client.killTopology(actual_job_name);
				}
			}
		} catch (Exception ex) {
			//let die for now, usually happens when top doesn't exist
			logger.info( ErrorUtils.getLongForm("Error stopping job: " + job_name + "  this is typical with storm because the job may not exist that we try to kill {0}", ex));
			return FutureUtils.returnError(ex);
		}
		
		future.complete(new BasicMessageBean(new Date(), true, null, "stop job", 0, "Stopped job successfully", null));
		return future;
	}

	/**
	 * Grabs the topology info for the given job name.  First needs to get the
	 * job id from the job_name, then lookup the stats.
	 * 
	 */
	@Override
	public TopologyInfo getJobStats(String job_name) throws Exception {
		logger.info("Looking for stats for job: " + job_name);		
		String job_id = getJobTopologySummaryFromJobPrefix(job_name).get_id();
		logger.info("Looking for stats with id: " + job_id);
		if ( job_id != null ) {
			synchronized (client) {
				return client.getTopologyInfo(job_id);
			}
		}
		return null;
	}
	
	/**
	 * Tries to find a running topology using the job_prefix.  Tries to match on job_name.
	 * 
	 * @param job_prefix
	 * @return
	 * @throws TException
	 */
	private TopologySummary getJobTopologySummaryFromJobPrefix(String job_prefix) throws TException {
		ClusterSummary cluster_summary = Lambdas.wrap_u(() -> {
			synchronized (client) {
				return client.getClusterInfo();
			}
		}).get();
		Iterator<TopologySummary> iter = cluster_summary.get_topologies_iterator();
		 while ( iter.hasNext() ) {
			 TopologySummary summary = iter.next();
			 //WARNING: this just matches on prefix (because we don't know the unique jobid attached to the end of the job_name anymore)
			 //this means you can false positive if you have 2 jobs one a prefix of the other e.g. job_a and job_a_1
			 if ( summary.get_name().startsWith(job_prefix)) {
				 logger.info("found a matching job with name: " + summary);
				 return summary;
			 }
		 }	
		 logger.info("did not find existing job with prefix");
		 return null;
	}
}
