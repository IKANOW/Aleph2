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
package com.ikanow.aleph2.management_db.data_model;

import java.io.Serializable;
import java.util.Date;
import java.util.Optional;

/** Contains the state necessary to determine when analytic buckets and jobs should trigger
 * @author Alex
 */
public class AnalyticTriggerStateBean implements Serializable {
	private static final long serialVersionUID = 4835760952601844653L;

	/** Jackson c'tor
	 */
	protected AnalyticTriggerStateBean() {}
	
	/**
	 * @param is_active - whether the job/input is part of a bucket that is currently active
	 * @param last_checked - the last time the trigger state was checked
	 * @param next_check - the next scheduled check
	 * @param bucket_id - the _id of the bucket
	 * @param bucket_name - the full_name of the bucket
	 * @param job_name - the name of the job 
	 * @param input_data_service - the data_service of the input
	 * @param input_resource_name_or_id - the resource_name_or_id of the input
	 * @param last_resource_size - the last resource size that triggered
	 * @param new_resource_size - the current resource size
	 */
	public AnalyticTriggerStateBean(Boolean is_active, Boolean is_pending, Date last_checked,
			Date next_check, String bucket_id, String bucket_name,
			String job_name, String input_data_service,
			String input_resource_name_or_id, Long last_resource_size,
			Long curr_resource_size,
			String locked_to_host
			) {
		super();
		this.is_active = is_active;
		this.is_pending = is_pending;
		this.last_checked = last_checked;
		this.next_check = next_check;
		this.bucket_id = bucket_id;
		this.bucket_name = bucket_name;
		this.job_name = job_name;
		this.input_data_service = input_data_service;
		this.input_resource_name_or_id = input_resource_name_or_id;
		this.last_resource_size = last_resource_size;
		this.curr_resource_size = curr_resource_size;
		this.locked_to_host = locked_to_host;
		
		this._id = buildId(bucket_name, job_name, Optional.ofNullable(locked_to_host), Optional.ofNullable(is_pending));
	}

	/** Builds this id
	 * @param bean
	 * @return
	 */
	public static String buildId(final String bucket_name, final String job_name, final Optional<String> locked_to_host, final Optional<Boolean> is_pending) {
		return bucket_name + ":" + job_name + ":" + locked_to_host.orElse("") + ":" + is_pending.orElse(false);
	}
	
	/** The bucket _id in the database
	 * @return
	 */
	public String _id() { return _id; }
	
	/** whether the job/input is part of a bucket that is currently active
	 * @return  whether the job/input is part of a bucket that is currently active
	 */
	public Boolean is_active() { return is_active; }
	
	/** If a bucket is active then can't edit its trigger until its done - we store the state here and then overwrite 
	 * @return  whether the job/input is an update to an active bucket
	 */
	public Boolean is_pending() { return is_pending; }	
	
	/** the last time the trigger state was checked
	 * @return the last time the trigger state was checked
	 */
	public Date last_checked() { return last_checked; }
	/** the next scheduled check
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
	/** the name of the job 
	 * @return the name of the job 
	 */
	public String job_name() { return job_name; }
	/** the data_service of the input
	 * @return the data_service of the input
	 */
	public String input_data_service() { return input_data_service; }
	/** the resource_name_or_id of the input
	 * @return the resource_name_or_id of the input
	 */
	public String input_resource_name_or_id() { return input_resource_name_or_id; }
	/** the last resource size that triggered
	 * @return the last resource size that triggered
	 */
	public Long last_resource_size() { return last_resource_size; }
	/** the current resource size
	 * @return the current resource size
	 */
	public Long curr_resource_size() { return curr_resource_size; }	
	
	/** For multi-node analytic jobs the copy of this state locked to this host
	 * @return For multi-node analytic jobs the copy of this state locked to this host
	 */
	public String locked_to_host() { return locked_to_host; }
	
	protected String _id;
	
	protected Boolean is_active;
	protected Boolean is_pending;
	protected Date last_checked;
	protected Date next_check;
	protected String bucket_id;
	protected String bucket_name;
	protected String job_name;
	protected String input_data_service;
	protected String input_resource_name_or_id;
	protected Long last_resource_size;
	protected Long curr_resource_size;	
	protected String locked_to_host;
}
