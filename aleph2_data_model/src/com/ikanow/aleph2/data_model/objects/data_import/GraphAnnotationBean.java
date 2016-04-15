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
	
	/** label - for display, searchable for edges but not vertices
	 */
	public static final String label = "label";
	
	public enum ElementType { edge, vertex }
	/** "edge" or "vertex"
	 */
	public static final String type = "type";
	
	/** A list of bucket paths that determine the permission of the node/edge/attribute
	 */
	public static final String a2_p = "a2_p";
	/** Optionally, a list of references 
	 */
	public static final String a2_r = "a2_r";
	/** Created timestamp (long)
	 */
	public static final String a2_tc = "a2_tc";
	/** Modified timestamp (long)
	 */
	public static final String a2_tm = "a2_tm";
	
	/** Allows permissions and other properties to be set on edge/vertex properties
	 *  "a2_p" is interpreted by Aleph2 to mean a bucket path on which the user must have read access
	 */
	public static final String properties = "properties";
	
	public static final String value = "value"; // (the nested value inside properties, along with "id" and "properties, the contains the actual value)
	
	// Vertex fields
	
	/** In decomposition mode for vertices a map of indexed properties that is used for deduplication (if this is not unique, the right one will have to be chosen from the merge stage) 
	 *  Otherwise leave blank/as the populated value 
	 *  eg: { "name": "alex", "type": "person" }
	 *  In merge mode is the internal "id" assigned by the system, and should not be changed by the user
	 */
	public static final String id = "id";
		
	// Vertex property fields
	// Internal
	// Typical vertex properties
	public static final String name = "name";	
	// also use type as a property
	
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
	// Typical edge properties
	public static final String weight = "weight";
}
