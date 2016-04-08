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

/** Defines GraphSON fields used in Aleph2
 * @author Alex
 *
 */
public class GraphAnnotationBean {

	// Common fields
	
	/** "edge" or "vertex"
	 */
	public static final String _type = "_type";
	
	/** A list of bucket paths that determine the permission of the node/edge/attribute
	 */
	public static final String _b = "_b";
	
	// Vertex fields
	
	/** In decomposition mode for vertices a map of indexed attributes that is used for deduplication (if this is not unique, the right one will have to be chosen from the merge stage) 
	 *  Otherwise leave blank/as the populated value 
	 *  eg: { "name": "alex", "type": "person" }
	 */
	public static final String _id = "_id";
		
	// Vertex property fields
	
	/** Allows permissions and other properties to be set on edge/vertex properties
	 *  "_b" is interpreted by Aleph2 to mean a bucket path on which the user must have read access
	 */
	public static final String _meta = "_meta";
	
	// Edge fields

	// Internal
	public static final String _label = "_label";
	/** In decomposition mode should be formatted like the _id field described above 
	 *  In merge mode points to the long _ids
	 */
	public static final String _inV = "_inV";
	/** In decomposition mode should be formatted like the _id field described above 
	 *  In merge mode points to the long _ids
	 */
	public static final String _outV = "_outV";
	// Common
	public static final String _weight = "weight";

}
