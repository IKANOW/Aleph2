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
package com.ikanow.aleph2.data_import_manager.streaming_enrichment;

import java.util.Iterator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift7.TException;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.KillOptions;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.TopologySummary;

/**
 * Hands submitting storm jobs to a local cluster.
 * 
 * @author Burch
 *
 */
public class LocalStormController implements IStormController {
	private LocalCluster local_cluster;
	private static final Logger logger = LogManager.getLogger();
	
	/**
	 * Starts up the local cluster, this sometimes takes a little bit
	 * 
	 */
	public LocalStormController() {
		local_cluster = new LocalCluster();
	}

	@Override
	public void submitJob(String job_name, String input_jar_location,
			StormTopology topology) throws Exception {
		logger.info("Submitting job: " + job_name);
		Config config = new Config();
		config.setDebug(true);
		local_cluster.submitTopology(job_name, config, topology);
	}

	@Override
	public void stopJob(String job_name) throws Exception {
		logger.info("Stopping job: " + job_name);
		KillOptions ko = new KillOptions();
		ko.set_wait_secs(0);	
		local_cluster.killTopologyWithOpts(getJobTopologySummaryFromJobPrefix(job_name).get_name(), ko);
	}

	@Override
	public TopologyInfo getJobStats(String job_name) throws Exception {
		logger.info("Looking for stats for job: " + job_name);		
		String job_id = getJobTopologySummaryFromJobPrefix(job_name).get_id();
		logger.info("Looking for stats with id: " + job_id);
		if ( job_id != null )
			return local_cluster.getTopologyInfo(job_id);			
		return null;
	}
	
	private TopologySummary getJobTopologySummaryFromJobPrefix(String job_prefix) throws TException {
		ClusterSummary cluster_summary = local_cluster.getClusterInfo();
		Iterator<TopologySummary> iter = cluster_summary.get_topologies_iterator();
		 while ( iter.hasNext() ) {
			 TopologySummary summary = iter.next();
			 System.out.println(summary.get_name() + summary.get_id() + summary.get_status());				 
			 if ( summary.get_name().startsWith(job_prefix));
			 	return summary;
		 }	
		 return null;
	}
}
