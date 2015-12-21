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

import java.util.Date;

/** Contains internal state relating to a bucket's analytic thread
 * @author Alex
 */
public class AnalyticThreadStateBean {
	
	/** Jackson c'tor
	 */
	protected AnalyticThreadStateBean() {}
	
	/** User c'tor
	 * @param last_run - The last time this job was run
	 * @param curr_run - The time the current job was started, if actively running
	 * @param last_run_success - Whether the last completed job was successful (not populated if the job was never run)
	 */
	public AnalyticThreadStateBean(Date last_run, Date curr_run,
			Boolean last_run_success) {
		super();
		this.last_run = last_run;
		this.curr_run = curr_run;
		this.last_run_success = last_run_success;
	}

	/** The last time this job was run
	 * @return The last time this job was run
	 */
	public Date last_run() { return last_run; }
	
	/** The time the current job was started, if actively running
	 * @return The time the current job was started, if actively running
	 */
	public Date curr_run() { return curr_run; }
		
	/** Whether the last completed job was successful (not populated if the job was never run)
	 * @return Whether the last completed job was successful (not populated if the job was never run)
	 */
	public Boolean last_run_success() { return last_run_success; }	
	
	private Date last_run;
	private Date curr_run;
	private Boolean last_run_success;
}
