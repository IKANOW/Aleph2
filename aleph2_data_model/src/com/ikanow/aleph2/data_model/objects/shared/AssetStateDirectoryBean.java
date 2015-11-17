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
package com.ikanow.aleph2.data_model.objects.shared;

import java.io.Serializable;

/** A bean used to list all the user CRUD stores 
 * @author Alex
 */
public class AssetStateDirectoryBean implements Serializable {
	private static final long serialVersionUID = 8942542313350035626L;
	
	public enum StateDirectoryType { analytic_thread, enrichment, harvest, library }

	protected AssetStateDirectoryBean() {}
	
	/** User c'tor
	 * @param asset_path
	 * @param state_type
	 * @param collection_name
	 * @param database_location
	 */
	public AssetStateDirectoryBean(final String asset_path,
			final StateDirectoryType state_type, final String collection_name,
			final String database_location) {
		this.asset_path = asset_path;
		this.state_type = state_type;
		this.collection_name = collection_name;
		this._id = database_location;
	}

	/** The storage path of the asset (bucket/bean)
	 * @return
	 */
	public String asset_path() { return asset_path; }
		
	/** The datastore type (ie whether generated from IManagementDbService. getPerLibraryState/getBucketHarvestState/getBucketEnrichmentState/getBucketAnalyticThreadState
	 * @return
	 */
	public StateDirectoryType state_type() { return state_type; }
	
	/** The name of the collection specified in the IManagementDbService request for the CRUD
	 * @return
	 */
	public String collection_name() { return collection_name; }
	
	/** A string specific to the underlying technology that describes the location of the CRUD service within that technology
	 * @return
	 */
	public String _id() { return _id; }	
	
	private String asset_path;
	private StateDirectoryType state_type; 
	private String collection_name;
	private String _id;
}
