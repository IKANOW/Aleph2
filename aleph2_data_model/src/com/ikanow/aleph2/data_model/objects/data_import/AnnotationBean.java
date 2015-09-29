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
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** A generic "underlay" for document-like data objects
 *  Can be inserted into a JSON object as the field "__a" and provides a lightweight mechanism for application-aware tagging
 *  ALL FIELDS ARE OPTIONAL - NO PART OF THE CORE WILL ASSUME ANY OF THEM EXIST
 * @author Alex
 */
@SuppressWarnings("unused")
public class AnnotationBean implements Serializable {
	private static final long serialVersionUID = 5347218352527147640L;

	// Standard metadata
	
	// (_id is in the parent document)
	private Date tc; //(time the document was created in the system)
	private Date tm; //(time the document was modified)
	private Date tp; //(time the document was published)
	
	private String el; // an externally-addressable URI of the doc
	private String il; // an internally-addressable URI of the doc
	
	private Set<String> tags; //tags
	
	// Entities and assocations
	
	private List<EntityBean> ee; //entities
	private List<AssociationBean> aa; //associations
	private Double ds; // (document-level sentiment)
	private List<GeoTemporalBean> dg; //geo tags 
	private List<AssociationBean> da; // associations between this document to other documents
	private List<AssociationBean> ea; // associations between this document and entities in this document
	
	// Document-level authentication
	
	private Set<String> d_auth;
	private Map<String, Set<String>> f_auth; // (field level authorizations)
	
	// User annotations
	
	//TODO (ALEPH-13): user annotations: comments, user tags, user entities, user associations etc.
	
	// The sub-structures:
	
	/** Encapsulates the concept of an "entity" in a document: a person, a place, an abstract concept
	 * @author Alex
	 */
	public static class EntityBean implements Serializable {
		private static final long serialVersionUID = -7918359605981338700L;
		
		private String i; //the index of the entity toLower(n)+'/'+toLower(t) 
		private String n; //the name of the entity
		private String t; //the type of the entity within the (unenforced) ontology
		private List<MentionBean> m; // a list of (non trivial) other ways in which the entity has been referenced in this document
		private Double es; // the aggregate sentiment of this entity mentions within this document
		private Double er; // the overall relevance of this entity within the doc
		private Double ec; // the overall confidence that the references refer to the disambiguated name
		
		private Map<String, String> attr; // an arbitrary set of attributes associated with this entity
		private Set<String> e_auth; // entity-level authorizations		
	}
	
	public static class MentionBean implements Serializable {
		private static final long serialVersionUID = -7849066971820389988L;
		
		private String mn; // the name of the mention
		private Long pos; // the position of the mention
		private Long p; // the paragraph reference of the mention (enables co-ref type analysis)
		private Double s; // the sentiment of this entity mention within this document
		private Double r; // the relevance of this entity mention within this document
		private Double c; // the confidence that the mention refer to the disambiguated name
	}
	
	/** Encapsulates the concept that "entities" in documents can be explicity associated more strongly 
	 *  than simply by co-reference
	 * @author Alex
	 */
	public static class AssociationBean implements Serializable {
		private static final long serialVersionUID = -5053561491981582999L;
		
		private List<String> ei; // the entities within the association
		private String ar; // the relationship
		private Date ts; // the start time of the relationship
		private Date te; // the end time of the relationship
		private Integer ad; // the direction of the associations (-1, 0, or 1; 0 == undirected)
		private List<GeoTemporalBean> ag; // geos associated with the association
		
		private Set<String> a_auth; // association-level authorizations		
	}	
	/** Encapsulates a geo-temporal associations within the document
	 * @author Alex
	 */
	public static class GeoTemporalBean implements Serializable {
		private static final long serialVersionUID = 7817964347288828798L;
		//TODO (ALEPH-13): define the class
	}
}
