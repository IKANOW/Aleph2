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
import java.util.List;
import java.util.Map;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Bean controlling a harvester configuration - normally found embedded within a DataBucketBean
 * @author acp
 */
public class HarvestControlMetadataBean {
	
	protected HarvestControlMetadataBean() {}
	
	public HarvestControlMetadataBean(final @NonNull String name, final @NonNull Boolean enabled, final @Nullable List<String> library_ids_or_names,
			final @NonNull Map<String, Object> config) {
		this.name = name;
		this.enabled = enabled;
		this.library_ids_or_names = library_ids_or_names;
		this.config = config;
	}
	/** A name for this harvest config - must be unique within the list of harvest configs in the bucket
	 *  (currently used for search/display only)
	 * @return the enabled
	 */
	public String name() {
		return name;
	}
	/** Whether this harvest block is currently enabled
	 * @return the enabled
	 */
	public Boolean enabled() {
		return enabled;
	}
	/** A list of ids or names pointing to the JAR library that is maintained either in
	 * the global list or the per-bucket list (in both cases accessible via the Management DB or the harvest context)
	 * The libraries can be arbitrary JARs
	 * @return the library_ids_or_names
	 */
	public List<String> library_ids_or_names() {
		return null == library_ids_or_names ? null : Collections.unmodifiableList(library_ids_or_names);
	}
	/** The harvest-technology-specific configuration that controls the per-bucket import
	 * @return the config
	 */
	public Map<String, Object> config() {
		return null == config ? null : Collections.unmodifiableMap(config);
	}
	private String name;
	private Boolean enabled;
	private List<String> library_ids_or_names;
	private Map<String, Object> config;
}	
