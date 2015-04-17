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

import java.util.Set;

import org.checkerframework.checker.nullness.qual.NonNull;

public class SharedLibraryBean {

	public SharedLibraryBean() {}
	
	/** User constructor
	 */
	public SharedLibraryBean(String _id, String display_name, @NonNull String path_name,
			@NonNull LibraryType type, String subtype, @NonNull String owner_id,
			Set<String> tags, @NonNull Set<String> access_tokens) {
		super();
		this._id = _id;
		this.display_name = display_name;
		this.path_name = path_name;
		this.type = type;
		this.subtype = subtype;
		this.owner_id = owner_id;
		this.tags = tags;
		this.access_tokens = access_tokens;
	}
	/** The management DB id of the shared library (unchangeable, unlike the name)
	 * @return the library _id
	 */
	public String id() {
		return _id;
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
	/** The type of the library (harvest/enrichment/analytics/access and technology/module/utility)
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
		return tags;
	}
	/** The set of access tokens, read access only - any admin has write access and nobody else
	 * @return the set of access tokens
	 */
	public Set<String> access_tokens() {
		return access_tokens;
	}
	
	private String _id;
	private String display_name;
	private String path_name;
	private LibraryType type;
	private String subtype;
	public enum LibraryType { 
			analytics_technology, analytics_module, 
				enrichment_module, enrichment_utility, 
					harvest_technology, harvest_module,
						access_module, access_utility };
						
	private String owner_id;
	private Set<String> tags;
	private Set<String> access_tokens;
}
