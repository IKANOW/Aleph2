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

package com.ikanow.aleph2.analytics.data_model;

import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.GraphSchemaBean;

/** Optional config bean for graph
 *  NOTE: duplicated in each of the graph db services in order to avoid unnecessary dependencies (ick - sort this out later)
 * @author Alex
 *
 */
public class GraphConfigBean {

	/** Allows users to override the doc schema in order to use the dedup service as a standalone job
	 * @return
	 */
	public GraphSchemaBean graph_schema_override() { return graph_schema_override; }
	
	private GraphSchemaBean graph_schema_override;
}
