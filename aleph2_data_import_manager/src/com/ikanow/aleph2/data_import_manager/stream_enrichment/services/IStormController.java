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

import java.util.concurrent.CompletableFuture;

import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;

import backtype.storm.generated.StormTopology;
import backtype.storm.generated.TopologyInfo;

/**
 * Interface for controlling storms basic functions.
 * 
 * @author Burch
 *
 */
public interface IStormController {
	/**
	 * Should submit a job to the storm cluster with the name job_name, using the input_jar_location for
	 * the jar file and the given topology.
	 * 
	 * @param job_name
	 * @param input_jar_location
	 * @param topology
	 * @throws Exception
	 */
	CompletableFuture<BasicMessageBean> submitJob(String job_name, String input_jar_location, StormTopology topology);
	/**
	 * Should stop a job on the storm cluster given the job_name
	 * 
	 * @param job_name
	 * @throws Exception
	 */
	CompletableFuture<BasicMessageBean> stopJob(String job_name);
	/**
	 * Should return the job statistics for a job running on the storm cluster with the given job_name
	 * 
	 * @param job_name
	 * @return
	 * @throws Exception
	 */
	TopologyInfo getJobStats(String job_name) throws Exception;
}
