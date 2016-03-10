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
package com.ikanow.aleph2.management_db.data_model;

import java.io.Serializable;
import java.util.Date;

import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean.AnalyticThreadComplexTriggerBean.TriggerType;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.UuidUtils;

/** Contains the state necessary to determine when analytic buckets and jobs should trigger
 * @author Alex
 */
public class AnalyticTriggerStateBean implements Serializable {
	private static final long serialVersionUID = 4835760952601844653L;

	/** Jackson c'tor
	 */
	protected AnalyticTriggerStateBean() {}
	
	/** A c'tor for an analytic state trigger
	 * @param is_active - whether the job/input is part of a bucket that is currently active //TODO: needs to be split between bucket and job
	 * @param is_pending - whether this trigger hasn't been applied because it's waiting for the current job to complete first
	 * @param last_checked - the last time the trigger state was checked
	 * @param next_check - the next scheduled check
	 * @param bucket_id - the _id of the bucket
	 * @param bucket_name - the full_name of the bucket
	 * @param job_name - the name of the job (not present for external triggers)
	 * @param input_data_service - the data_service of the input
	 * @param trigger_type - the trigger type (can be manually specified for 
	 * @param input_resource_name_or_id - the resource_name_or_id of the input
	 * @param last_resource_size - the last resource size that triggered
	 * @param new_resource_size - the current resource size
	 * @param resource_limit - enables quick trigger checks
	 * @param locked_to_host - if this job is associated with a specific host (supports multi-node analytic technologies)
	 */
	public AnalyticTriggerStateBean(
			Boolean is_bucket_active, Boolean is_job_active, 
			Boolean is_bucket_suspended, Boolean is_pending, 
			Date last_checked, Date next_check, 
			String bucket_id, String bucket_name,
			String job_name, 
			TriggerType trigger_type, String input_data_service,
			String input_resource_name_or_id, 
			String input_resource_combined,
			Long last_resource_size,
			Long curr_resource_size,
			Long resource_limit,
			String locked_to_host
			) {
		this.is_bucket_active = is_bucket_active;
		this.is_job_active = is_job_active;
		this.is_bucket_suspended = is_bucket_suspended;
		this.is_pending = is_pending;
		this.last_checked = last_checked;
		this.next_check = next_check;
		this.bucket_id = bucket_id;
		this.bucket_name = bucket_name;
		this.job_name = job_name;
		this.input_data_service = input_data_service;
		this.trigger_type = trigger_type;
		this.input_resource_name_or_id = input_resource_name_or_id;
		this.input_resource_combined = input_resource_combined;
		this.last_resource_size = last_resource_size;
		this.curr_resource_size = curr_resource_size;
		this.resource_limit = resource_limit;
		this.locked_to_host = locked_to_host;
		
		this._id = buildId(this, this.is_pending());
	}

	/** Builds this id
	 * @param bean
	 * @return
	 */
	public static String buildId(final AnalyticTriggerStateBean bean, final boolean is_pending) {
		return UuidUtils.get().getContentBasedUuid(
				BeanTemplateUtils.build(AnalyticTriggerStateBean.class)
					.with(AnalyticTriggerStateBean::bucket_name, bean.bucket_name())
					.with(AnalyticTriggerStateBean::job_name, bean.job_name())
					.with(AnalyticTriggerStateBean::input_data_service, bean.input_data_service())
					.with(AnalyticTriggerStateBean::trigger_type, bean.trigger_type())
					.with(AnalyticTriggerStateBean::input_resource_name_or_id, bean.input_resource_name_or_id())
					.with(AnalyticTriggerStateBean::input_resource_combined, bean.input_resource_combined())
					.with(AnalyticTriggerStateBean::locked_to_host, bean.locked_to_host())
					.with(AnalyticTriggerStateBean::is_pending, is_pending)
				.done().get()
				);
	}
	
	/** The bucket _id in the database
	 * @return
	 */
	public String _id() { return _id; }
	
	/** whether the job/input is part of a _bucket_ that is currently active
	 * @return  whether the job/input is part of a bucket that is currently active
	 */
	public Boolean is_bucket_active() { return is_bucket_active; }
	
	/** whether the job/input is part of a _bucket_ that is currently suspended (ie inactive)
	 * @return  whether the job/input is part of a bucket that is currently active
	 */
	public Boolean is_bucket_suspended() { return is_bucket_suspended; }
	
	/** whether the job/input is part of a _job_ that is currently active
	 *  (not used by buckets)
	 * @return  whether the job/input is part of a bucket that is currently active
	 */
	public Boolean is_job_active() { return is_job_active; }
	
	/** If a bucket is active then can't edit its trigger until its done - we store the state here and then overwrite 
	 *  (Triggers only, not active records)
	 * @return  whether the job/input is an update to an active bucket
	 */
	public Boolean is_pending() { return is_pending; }	
	
	/** the last time the trigger state was checked
	 *  For active records, indicates when the job was started (or the last time a job was started in the case of bucket active records)
	 * @return the last time the trigger state was checked
	 */
	public Date last_checked() { return last_checked; }
	/** the next scheduled check
	 *  (For active records, this is not used in the logic - active records are always checked every trigger)
	 * @return the next scheduled check
	 */
	public Date next_check() { return next_check; }
	/** the _id of the bucket 
	 * @return the _id of the bucket
	 */
	public String bucket_id() { return bucket_id; }
	/** the full_name of the bucket
	 * @return the full_name of the bucket
	 */
	public String bucket_name() { return bucket_name; }
	/** the name of the job  (not used for bucket active records or external triggers)
	 * @return the name of the job 
	 */
	public String job_name() { return job_name; }
	/** the data_service of the input
	 *  (not used for active records)
	 * @return the data_service of the input
	 */
	public String input_data_service() { return input_data_service; }
	
	/** the resource_name_or_id of the input - note _doesn't_ include any sub-channel information
	 *  (not used for active records)
	 * @return the resource_name_or_id of the input
	 */
	public String input_resource_name_or_id() { return input_resource_name_or_id; }
	
	/** For cases where the trigger depends on an intermediate result, this is in the form resource_name_or_id:subchannel
	 *  (Not used for internal trigger records or active records)
	 * @return combined resource-id or sub-channel of the resource, if present
	 */
	public String input_resource_combined() { return input_resource_combined; }
	
	/** the last resource size that triggered
	 *  (not used for active records)
	 * @return the last resource size that triggered
	 */
	public Long last_resource_size() { return last_resource_size; }
	/** the current resource size
	 *  (not used for job active records ; for bucket active records is 1 if the bucket has seen jobs since activation)
	 * @return the current resource size
	 */
	public Long curr_resource_size() { return curr_resource_size; }	
	
	/** If populated, enables a quick check of the 2 resource values to determine whether to trigger (without hitting the DB)
	 *  (not used for active records)
	 * @return the limit to compare last-curr resource size
	 */
	public Long resource_limit() { return resource_limit; }
	
	/** For multi-node analytic jobs the copy of this state locked to this host
	 * @return For multi-node analytic jobs the copy of this state locked to this host
	 */
	public String locked_to_host() { return locked_to_host; }
	
	/** The type of the trigger ("none" if it's an active job notification)
	 * @return The type of the trigger ("none" if it's an active bucket.job notification)
	 */
	public TriggerType trigger_type() { return trigger_type; } 
	
	protected String _id;	
	protected Boolean is_job_active;
	protected Boolean is_bucket_active;
	protected Boolean is_bucket_suspended;
	protected Boolean is_pending;
	protected Date last_checked;
	protected Date next_check;
	protected String bucket_id;
	protected String bucket_name;
	protected String job_name;
	protected String input_data_service;
	protected String input_resource_name_or_id;
	protected String input_resource_combined;
	protected Long last_resource_size;
	protected Long curr_resource_size;	
	protected Long resource_limit;
	protected String locked_to_host;
	protected TriggerType trigger_type;
}
