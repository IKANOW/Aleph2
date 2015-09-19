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

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


/** Bean controlling a harvester configuration - normally found embedded within a DataBucketBean
 * @author acp
 */
public class HarvestControlMetadataBean implements Serializable {
	private static final long serialVersionUID = 1850167505811531333L;
	protected HarvestControlMetadataBean() {}
	
	/** User c'tor
	 * @param name - The name of the job - optional but required if it is needed as a dependency
	 * @param enabled - Whether the job is currently enabled, defaults to true
	 * @param module_name_or_id - An optional primary module containing application logic within the analytic technology
	 * @param library_names_or_ids - An optional list of addition modules required on the classpath
	 * @param entry_point - the entry point class of the module to execute; can be used to override the shared library's entry point (or if the entry point is not specified)
	 * @param config - The analytic technology specific module configuration JSON
	 */
	public HarvestControlMetadataBean(final String name, 
			final Boolean enabled, 
			final String module_name_or_id,
			final List<String> library_names_or_ids,
			final String entry_point,
			final LinkedHashMap<String, Object> config) {
		this.name = name;
		this.enabled = enabled;
		this.module_name_or_id = module_name_or_id;
		this.library_names_or_ids = library_names_or_ids;
		this.entry_point = entry_point;
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
	
	/** An optional primary module containing application logic within the analytic technology
	 *  The difference vs the library_names_or_ids is that the library bean (And hence its entry point) is accessible from the context
	 *  hence no entry_point need then be specified
	 * @return An optional primary module containing application logic within the analytic technology
	 */
	public String module_name_or_id() { return module_name_or_id; }
	
	/** A list of ids or names pointing to the JAR library that is maintained either in
	 * the global list or the per-bucket list (in both cases accessible via the Management DB or the harvest context)
	 * The libraries can be arbitrary JARs
	 * @return the library_ids_or_names
	 */
	public List<String> library_names_or_ids() {
		return (null != library_ids_or_names)
				? Collections.unmodifiableList(library_ids_or_names)
				: (null == library_names_or_ids ? null : Collections.unmodifiableList(library_names_or_ids));
	}
	
	/** The entry point class of the module to execute; can be used to override the shared library's entry point (or if the entry point is not specified) 
	 *  In many (simpler) cases it is not necessary, eg the technology + config is sufficient to define the job 
	 * @return (optional) the entry point class to run the module that defines the job
	 */
	public String entry_point() { return entry_point; }
	
	/** The harvest-technology-specific configuration that controls the per-bucket import
	 * @return the config
	 */
	public Map<String, Object> config() {
		return null == config ? null : Collections.unmodifiableMap(config);
	}
	private String name;
	private Boolean enabled;
	private String module_name_or_id;
	private List<String> library_names_or_ids;
	private String entry_point;
	private LinkedHashMap<String, Object> config;
	
	// Legacy, renamed to "library_ids_or_names": just for bw-compatibility support:
	private List<String> library_ids_or_names;
}	
