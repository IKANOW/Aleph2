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
package com.ikanow.aleph2.data_model.objects.data_analytics;

import java.io.Serializable;
import java.util.List;

import java.util.Collections;

/** This class defines an analytic thread within a bucket
 * @author Alex
 */
public class AnalyticThreadBean implements Serializable {
	private static final long serialVersionUID = -1884220783549625389L;

	protected AnalyticThreadBean() {}
	
	/** User c'tor
	 * @param trigger_config - Either a specific trigger instance, or a boolean grouping of trigger instances (can nest indefinitely)
	 * @param jobs - The list of configurations of individual jobs within the larger analytic thread
	 */
	public AnalyticThreadBean(final Boolean enabled, final AnalyticThreadTriggerBean trigger_config, final List<AnalyticThreadJobBean> jobs) {
		this.enabled = enabled;
		this.trigger_config = trigger_config;
		this.jobs = jobs;
	}

	/** Whether the analytic job is enabled
	 * @return
	 */
	public Boolean enabled() {
		return enabled;
	}	
	
	/** Either a specific trigger instance, or a boolean grouping of trigger instances (can nest indefinitely)
	 * @return the trigger configuration
	 */
	public AnalyticThreadTriggerBean trigger_config() { return trigger_config; }
	
	/** The list of configurations of individual jobs within the larger analytic thread
	 * @return the list of jobs
	 */
	public  List<AnalyticThreadJobBean> jobs() { return null == jobs ? jobs : Collections.unmodifiableList(jobs); }
	
	private Boolean enabled;
	private AnalyticThreadTriggerBean trigger_config;
	private List<AnalyticThreadJobBean> jobs;	
}
