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


/** Bean controlling an enrichment configuration
 * @author acp
 */
public class EnrichmentControlMetadataBean implements Serializable {
	private static final long serialVersionUID = 2550210707023662158L;
	protected EnrichmentControlMetadataBean() {}
	
	/** User constructor
	 * @param name - The name of the job - optional but required if it is needed as a dependency
	 * @param dependencies - 
	 * @param enabled - Whether the job is currently enabled, defaults to true
	 * @param module_name_or_id - An optional primary module containing application logic within the analytic technology
	 * @param library_names_or_ids - An optional list of addition modules required on the classpath
	 * @param entry_point - the entry point class of the module to execute; can be used to override the shared library's entry point (or if the entry point is not specified)
	 * @param config - The analytic technology specific module configuration JSON
	 */
	public EnrichmentControlMetadataBean(
			final String name,
			final List<String> dependencies, 
			final Boolean enabled,
			final String module_name_or_id,
			final List<String> library_names_or_ids,
			final String entry_point,
			final LinkedHashMap<String, Object> config) {
		super();
		this.name = name;
		this.dependencies = dependencies;
		this.enabled = enabled;
		this.module_name_or_id = module_name_or_id;
		this.library_names_or_ids = library_names_or_ids;
		this.entry_point = entry_point;
		this.config = config;
	}
	/** The name of the enrichment - must be unique within the list of enrichments in this bucket (used for search/display/dependencies)
	 * @return the name
	 */
	public String name() {
		return name;
	}
	/** Defines the dependency order of enrichment - this can be used by the framework to optimize runtime
	 * @return the dependencies
	 */
	public List<String> dependencies() {
		return dependencies == null ? null : Collections.unmodifiableList(dependencies);
	}
	/** Returns if this enrichment is currently enabled - implicitly disables all dependent enrichments
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
	
	/** A list of ids or names (within either the bucket or global library) of enrichment JARs to be
	 *  used as part of this enrichment. Exactly one of the JARs must be of type IEnrichmentBatchModule or IEnrichmentBatchTopology  
	 * @return the library_ids_or_names
	 */
	public List<String> library_names_or_ids() {
		return (null != library_ids_or_names)
				? Collections.unmodifiableList(library_ids_or_names)
				: (null == library_names_or_ids ? null : Collections.unmodifiableList(library_names_or_ids));
	}
	/** The entry point class of the module to execute; can be used to override the shared library's entry point (or if the entry point is not specified) 
	 * @return (optional) the entry point class to run the module that defines the job
	 */
	public String entry_point() { return entry_point; }
	
	/** The enrichment-module-specific configuration that controls the per-bucket enrichment
	 * @return the config
	 */
	public Map<String, Object> config() {
		return config == null ? null : Collections.unmodifiableMap(config);
	}
	private String name;
	private List<String> dependencies;
	private Boolean enabled;
	private String module_name_or_id;
	private List<String> library_names_or_ids;
	private String entry_point;
	private LinkedHashMap<String, Object> config;
	
	// Legacy, renamed to "library_ids_or_names": just for bw-compatibility support:
	private List<String> library_ids_or_names;
}	
