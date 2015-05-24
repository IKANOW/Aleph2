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
package com.ikanow.aleph2.data_model.objects.data_import;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;

/** Represents generic harvest status
 * @author acp
 */
public class DataBucketStatusBean {

	public static final String HARVEST_LOG_PATH = "/logs/harvest/";
	public static final String ENRICHMENT_LOG_PATH = "/logs/enrichment/";
	public static final String STORAGE_LOG_PATH = "/logs/storage/";
	
	protected DataBucketStatusBean() {}
	
	/** User constructor
	 */
	public DataBucketStatusBean(
			final @NonNull String _id,
			final @NonNull String bucket_path,
			final @NonNull Boolean suspended,
			final @Nullable Date quarantined_until,
			final @NonNull Long num_objects,
			final @Nullable List<String> node_affinity,
			final @Nullable Map<String, BasicMessageBean> last_harvest_status_messages,
			final @Nullable Map<String, BasicMessageBean> last_enrichment_status_messages,
			final @Nullable Map<String, BasicMessageBean> last_storage_status_messages) {
		super();
		this._id = _id;
		this.suspended = suspended;
		this.quarantined_until = quarantined_until;
		this.num_objects = num_objects;
		this.node_affinity = node_affinity;
		this.last_harvest_status_messages = last_harvest_status_messages;
		this.last_enrichment_status_messages = last_enrichment_status_messages;
		this.last_storage_status_messages = last_storage_status_messages;
	}
	
	/** The _id of the data status bean in the management DB
	 * @return the _id
	 */
	public String _id() {
		return _id;
	}
	
	/** The path of the corresponding bucket (relative to /app/aleph2/data)
	 * @return the path of the corresponding bucket
	 */
	public String bucket_path() {
		return bucket_path;
	}
	
	/** True if the bucket has been suspended
	 * @return the suspend state of the bucket
	 */
	public Boolean suspended() {
		return suspended;
	}
	/** If non-null, this bucket has been quarantined and will not be processed until the 
	 *  specified date (at which point quarantined_until will be set to null)
	 * @return the quarantined_until date
	 */
	public Date quarantined_until() {
		return quarantined_until;
	}
	
	/** An estimate of the number of objects in the bucket
	 * @return estimated number of objects in the bucket
	 */
	public Long num_objects() {
		return num_objects;
	}

	/** The current set of hostnames on which the associated harvest technology is running
	 * @return the current list of hostnames on which the associated harvest technology is running
	 */
	public List<String> node_affinity() {
		return Collections.unmodifiableList(node_affinity);
	}
	
	/** Each time a host performs a harvest activity (either -internal- technology or -external- module) it can update this date/status 
	 * @return a map of hosts vs the last time they harvested for this bucket 
	 */
	public Map<String, BasicMessageBean> last_harvest_status_messages() {
		return Collections.unmodifiableMap(last_harvest_status_messages);
	}
	/** Each time a host performs an enrichment activity it updates this date/status (from within the core) 
	 * @return a map of hosts vs the last time they enriched for this bucket 
	 */
	public Map<String, BasicMessageBean> last_enrichment_status_messages() {
		return Collections.unmodifiableMap(last_enrichment_status_messages);
	}
	
	/** Each time a host and data service performs an enrichment activity it updates this date/status (from within the core) 
	 * @return a map of host+service vs the status/date
	 */
	public Map<String, BasicMessageBean> last_storage_status_messages() {
		return Collections.unmodifiableMap(last_storage_status_messages);
	}
	private String _id;
	private String bucket_path;
	private Boolean suspended;
	private Date quarantined_until;
	
	private Long num_objects;
	
	private List<String> node_affinity;
	
	private Map<String, BasicMessageBean> last_harvest_status_messages;
	private Map<String, BasicMessageBean> last_enrichment_status_messages;
	private Map<String, BasicMessageBean> last_storage_status_messages;
}

