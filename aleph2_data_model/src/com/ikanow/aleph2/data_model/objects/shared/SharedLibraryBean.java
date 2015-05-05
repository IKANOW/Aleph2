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
package com.ikanow.aleph2.data_model.objects.shared;

import java.util.Collections;
import java.util.Date;
import java.util.Set;

import org.checkerframework.checker.nullness.qual.NonNull;

public class SharedLibraryBean {

	protected SharedLibraryBean() {}
	
	/** User constructor
	 */
	public SharedLibraryBean(String _id, String display_name, @NonNull String path_name,
			@NonNull LibraryType type, String subtype, @NonNull String owner_id,
			Set<String> tags, @NonNull AuthorizationBean access_rights) {
		super();
		this._id = _id;
		this.display_name = display_name;
		this.path_name = path_name;
		this.type = type;
		this.subtype = subtype;
		this.owner_id = owner_id;
		this.tags = tags;
		this.access_rights = access_rights;
	}
	/** The management DB id of the shared library (unchangeable, unlike the name)
	 * @return the library _id
	 */
	public String id() {
		return _id;
	}
	/** When this bucket was first created
	 * @return created date
	 */
	public Date created() {
		return created;
	}
	/** When this bucket was last modified
	 * @return modified date
	 */
	public Date modified() {
		return modified;
	}
	/** The display name - used for display and search only. If not set will correspond to the filename in the main path
	 * @return the display name
	 */
	public String display_name() {
		return display_name;
	}
	/** The full path - must be unique within the cluster
	 * @return the full path of the file (in the storage service eg HDFS)
	 */
	public String path_name() {
		return path_name;
	}
	/** The type of the library (harvest/enrichment/analytics/access/misc and technology/module/etc-for-misc)
	 * @return the type
	 */
	public LibraryType type() {
		return type;
	}
	/** An optional sub-type provided for searching, can be any string - for display/search only
	 * @return the subtype
	 */
	public String subtype() {
		return subtype;
	}
	/** The owner id, a meaningful string to the security service
	 * @return the owner id
	 */
	public String owner_id() {
		return owner_id;
	}
	/** An optional set of tags for display/search purposes
	 * @return
	 */
	public Set<String> tags() {
		return Collections.unmodifiableSet(tags);
	}
	/** The set of access tokens, read access only - any admin has write access and nobody else
	 * @return the set of access tokens
	 */
	public AuthorizationBean access_rights() {
		return access_rights;
	}
	
	/** For JARs, the default entry point (type specific - eg will point to the implementation of IAccessTechnology, or IEnrichmentBatchModule, etc)
	 * @return the fully qualified classpath of the primary/default entry point
	 */
	public String primary_entry_point() {
		return primary_entry_point;
	}

	private String _id;
	private Date created;
	private Date modified;
	private String display_name;
	private String path_name;
	private LibraryType type;
	private String subtype;
	public enum LibraryType { 
			analytics_technology, analytics_module, 
				enrichment_module, enrichment_utility, 
					harvest_technology, harvest_module,
						access_module, access_utility,
							misc_archive, misc_directory, misc_file, misc_json };
						
	private String owner_id;
	private Set<String> tags;
	private AuthorizationBean access_rights;
	private String primary_entry_point;
}
