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

/**
 * Immutable object describing the data from this bucket
 * @author acp
 *
 */
public class DataSchemaBean {

	private ArchiveSchemaBean archive_schema;
	private ObjectDbSchemaBean object_schema;
	private SearchIndexSchemaBean search_index_schema;
	private ColumnarDbSchemaBean columnar_db_schema;
	private TemporalSchemaBean temporal_schema;
	private GeospatialSchemaBean geospatial_schema;
	private GraphDbSchemaBean graph_db_schema;
	
	public static class ArchiveSchemaBean {
		private Boolean enabled;
		//TODO 
	}
	public static class ObjectDbSchemaBean {
		private Boolean enabled;
		//TODO 
	}
	public static class SearchIndexSchemaBean {
		private Boolean enabled;
		//TODO 
	}
	public static class ColumnarDbSchemaBean {
		private Boolean enabled;
		//TODO 
	}
	public static class TemporalSchemaBean {
		private Boolean enabled;
		//TODO 
	}
	public static class GeospatialSchemaBean {
		private Boolean enabled;
		//TODO 
	}
	public static class GraphDbSchemaBean {
		private Boolean enabled;
		//TODO 
	}
	
}
