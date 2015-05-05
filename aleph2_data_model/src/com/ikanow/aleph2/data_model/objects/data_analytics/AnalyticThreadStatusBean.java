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
package com.ikanow.aleph2.data_model.objects.data_analytics;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import scala.Tuple2;

import com.google.common.collect.Multimap;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;

/** Represents generic analytic thread status
 * @author acp
 */
public class AnalyticThreadStatusBean {
	protected AnalyticThreadStatusBean() {}
	
	/** User constructor
	 */
	public AnalyticThreadStatusBean(
			@NonNull String id,
			@NonNull Boolean suspended,
			@Nullable Date quarantined_until,
			@NonNull Long num_objects,
			@Nullable List<String> node_affinity,
			@Nullable Map<String, BasicMessageBean> last_status_messages,
			@Nullable Map<Tuple2<String, String>, BasicMessageBean> last_storage_status_messages,
			@Nullable Multimap<Tuple2<String, String>, BasicMessageBean> analytics_log_messages) {
		super();
		_id = id;
		this.suspended = suspended;
		this.quarantined_until = quarantined_until;
		this.num_objects = num_objects;
		this.node_affinity = node_affinity;
		this.last_status_messages = last_status_messages;
		this.last_storage_status_messages = last_storage_status_messages;
		this.analytics_log_messages = analytics_log_messages;
	}
	
	/** The _id of the data status bean in the management DB
	 * @return the _id
	 */
	public String _id() {
		return _id;
	}
	
	/** True if the thread has been suspended
	 * @return the suspend state of the bucket
	 */
	public Boolean suspended() {
		return suspended;
	}
	/** If non-null, this thread has been quarantined and will not be processed until the 
	 *  specified date (at which point quarantined_until will be set to null)
	 * @return the quarantined_until date
	 */
	public Date quarantined_until() {
		return quarantined_until;
	}
	
	/** An estimate of the number of objects written by the thread 
	 * @return estimated number of objects written by the thread
	 */
	public Long num_objects() {
		return num_objects;
	}

	/** The current set of hostnames on which the analytics technology is running
	 * @return the current list of hostnames on which the analytics technology is running
	 */
	public List<String> node_affinity() {
		return node_affinity;
	}
	
	/** Each time a host performs an analytic activity (either -internal- technology or -external- module) it can update this date/status 
	 * @return a map of hosts vs the last time they performed processing for this thread 
	 */
	public Map<String, BasicMessageBean> last_status_messages() {
		return last_status_messages;
	}
	
	/** Each time a host and data service write to a bucket it updates this date/status (from within the core) 
	 * @return a map of host+service vs the status/date
	 */
	public Map<Tuple2<String, String>, BasicMessageBean> last_storage_status_messages() {
		return last_storage_status_messages;
	}
	/** A set of recent log messages from the modules, keyed by host. The core will remove old messages in an unspecified FIFO 
	 * @return multimap of recent analytics messages vs host and analytic module
	 */
	public Multimap<Tuple2<String, String>, BasicMessageBean> analytics_log_messages() {
		return analytics_log_messages;
	}
	
	private String _id;
	private Boolean suspended;
	private Date quarantined_until;
	
	private Long num_objects;
	
	private List<String> node_affinity;
	
	private Map<String, BasicMessageBean> last_status_messages;
	private Map<Tuple2<String, String>, BasicMessageBean> last_storage_status_messages;	
	private Multimap<Tuple2<String, String>, BasicMessageBean> analytics_log_messages;
}
