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

/** Defines GraphSON/Tinkerpop3 fields used in Aleph2
 * @author Alex
 *
 */
public class GraphAnnotationBean {

	// Common fields
	
	/** Searchabel for edges but not vertices
	 */
	public static final String label = "label";
	
	/** "edge" or "vertex"
	 */
	public static final String type = "type";
	
	/** A list of bucket paths that determine the permission of the node/edge/attribute
	 */
	public static final String _b = "_b";
	
	// Vertex fields
	
	/** In decomposition mode for vertices a map of indexed properties that is used for deduplication (if this is not unique, the right one will have to be chosen from the merge stage) 
	 *  Otherwise leave blank/as the populated value 
	 *  eg: { "name": "alex", "type": "person" }
	 *  In merge mode is the internal "id" assigned by the system, and should not be changed by the user
	 */
	public static final String id = "id";
		
	// Vertex property fields
	
	/** Allows permissions and other properties to be set on edge/vertex properties
	 *  "_b" is interpreted by Aleph2 to mean a bucket path on which the user must have read access
	 */
	public static final String properties = "properties";
	
	// Edge fields

	// Internal
	/** In decomposition mode should be formatted like the id field described above 
	 *  In merge mode points to the long ids
	 */
	public static final String inV = "inV";
	/** In decomposition mode should be formatted like the id field described above 
	 *  In merge mode points to the long ids
	 */
	public static final String outV = "outV";
	public static final String inVLabel = "inVLabel";
	public static final String outVLabel = "outVLabel";
	// Typical edge fields
	public static final String weight = "weight";

}
