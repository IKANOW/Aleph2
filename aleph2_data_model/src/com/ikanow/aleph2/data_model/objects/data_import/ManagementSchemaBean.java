/*******************************************************************************
 * Copyright 2016, The IKANOW Open Source Project.
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
package com.ikanow.aleph2.data_model.objects.data_import;

import java.io.Serializable;

import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.ColumnarSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.LoggingSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.SearchIndexSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.StorageSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.TemporalSchemaBean;

/**
 * @author Burch
 *
 */
public class ManagementSchemaBean implements Serializable {

	private static final long serialVersionUID = -1325104245856394072L;
	
	public LoggingSchemaBean logging_schema() { return this.logging_schema; }
	public TemporalSchemaBean temporal_schema() { return this.temporal_schema; }
	public StorageSchemaBean storage_schema() { return this.storage_schema; }
	public SearchIndexSchemaBean search_index_schema() { return this.search_index_schema; }
	public ColumnarSchemaBean columnar_schema() { return this.columnar_schema; }
	
	private LoggingSchemaBean logging_schema;
	private TemporalSchemaBean temporal_schema;
	private StorageSchemaBean storage_schema;
	private SearchIndexSchemaBean search_index_schema;
	private ColumnarSchemaBean columnar_schema;
		
}
