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
package com.ikanow.aleph2.data_model.interfaces.shared_services;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.checkerframework.checker.nullness.qual.NonNull;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.ProjectBean;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent;

/** A generic interface to an "object" datastore with a "MongoDB like" interface
 * @author acp
 *
 * @param <T> the bean type served by this repository
 */
public interface ICrudService<O> {

	public static abstract class Cursor<O> implements Iterable<O>, AutoCloseable {
		public abstract long count();
	}
	
	//////////////////////////////////////////////////////

	// Authorization and project filtering:
	
	/** Returns a copy of the CRUD service that is filtered based on the client (user) and project rights
	 * @param authorization_fieldname the fieldname in the bean that determines where the per-bean authorization is held
	 * @param client_auth Optional specification of the user's access rights
	 * @param project_auth Optional specification of the projects's access rights
	 * @return The filtered CRUD repo
	 */
	@NonNull 
	ICrudService<O> getFilteredRepo(final @NonNull String authorization_fieldname, final Optional<AuthorizationBean> client_auth, final Optional<ProjectBean> project_auth);
	
	//////////////////////////////////////////////////////
	
	// *C*REATE
	
	/** Stores the specified object in the database, optionally failing if it is already present
	 *  If the "_id" field of the object is not set then it is assigned
	 * @param new_object
	 * @param replace_if_present if true then any object with the specified _id is overwritten
	 * @return A future containing the _id (filled in if not present in the object) - accessing the future will also report on errors via ExecutionException 
	 */
	@NonNull 
	CompletableFuture<Supplier<Object>> storeObject(final @NonNull O new_object, final boolean replace_if_present);

	/** Stores the specified object in the database, failing if it is already present
	 *  If the "_id" field of the object is not set then it is assigned
	 * @param new_object
	 * @return A future containing the _id (filled in if not present in the object) - accessing the future will also report on errors via ExecutionException 
	 */
	@NonNull 
	CompletableFuture<Supplier<Object>> storeObject(final @NonNull O new_object);
	
	/**
	 * @param objects - a list of objects to insert
	 * @param continue_on_error if true then duplicate objects are ignored (not inserted) but the store continues
	 * @return A future containing the list of _ids (filled in if not present in the object), and the number of docs retrieved - accessing the future will also report on errors via ExecutionException 
	 */
	@NonNull 
	CompletableFuture<Tuple2<Supplier<List<Object>>, Supplier<Long>>> storeObjects(final @NonNull List<O> new_objects, final boolean continue_on_error);
	
	/**
	 * @param objects - a list of objects to insert, failing out as soon as a duplicate is inserted 
	 * @return A future containing the list of _ids (filled in if not present in the object), and the number of docs retrieved - accessing the future will also report on errors via ExecutionException 
	 */
	@NonNull 
	CompletableFuture<Tuple2<Supplier<List<Object>>, Supplier<Long>>> storeObjects(final @NonNull List<O> new_objects);
	
	//////////////////////////////////////////////////////
	
	// *R*ETRIEVE
	
	/** Registers that you wish to optimize specific queries
	 * @param ordered_field_list a list of the fields in the query
	 * @return a future describing if the optimization was successfully completed - accessing the future will also report on errors via ExecutionException
	 */
	@NonNull 
	CompletableFuture<Boolean> optimizeQuery(final @NonNull List<String> ordered_field_list);	

	/** Inform the system that a specific query optimization is no longer required
	 * @param ordered_field_list a list of the fields in the query
	 * @return whether this optimization was registered in the first place
	 */
	@NonNull 
	boolean deregisterOptimizedQuery(final @NonNull List<String> ordered_field_list);	

	/** Returns the object (in optional form to handle its not existing) given a simple object template that contains a unique search field (but other params are allowed)
	 * @param unique_spec A specification (must describe at most one object) generated by CrudUtils.allOf(...) (all fields must be match) or CrudUtils.anyOf(...) (any fields must match) together with extra fields generated by .withAny(..), .withAll(..), present(...) or notPresent(...)   
	 * @return A future containing an optional containing the object, or Optional.empty() - accessing the future will also report on errors via ExecutionException 
	 */
	@NonNull 
	CompletableFuture<Optional<O>> getObjectBySpec(final @NonNull QueryComponent<O> unique_spec);

	/** Returns the object (in optional form to handle its not existing) given a simple object template that contains a unique search field (but other params are allowed)
	 * @param unique_spec A specification (must describe at most one object) generated by CrudUtils.allOf(...) (all fields must be match) or CrudUtils.anyOf(...) (any fields must match) together with extra fields generated by .withAny(..), .withAll(..), present(...) or notPresent(...)   
	 * @param field_list list of fields to return, supports "." nesting
	 * @param include - if true, the field list is to be included; if false, to be excluded
	 * @return A future containing an optional containing the object, or Optional.empty()  - accessing the future will also report on errors via ExecutionException
	 */
	@NonNull 
	CompletableFuture<Optional<O>> getObjectBySpec(final @NonNull QueryComponent<O> unique_spec, final @NonNull List<String> field_list, final boolean include);

	/** Returns the object given the id
	 * @param id the id of the object
	 * @return A future containing an optional containing the object, or Optional.empty() - accessing the future will also report on errors via ExecutionException 
	 */
	@NonNull 
	CompletableFuture<Optional<O>> getObjectById(final @NonNull Object id);	

	/** Returns the object given the id
	 * @param id the id of the object
	 * @param field_list List of fields to return, supports "." nesting
	 * @param include - if true, the field list is to be included; if false, to be excluded
	 * @return A future containing an optional containing the object, or Optional.empty() - accessing the future will also report on errors via ExecutionException 
	 */
	@NonNull 
	CompletableFuture<Optional<O>> getObjectById(final @NonNull Object id, final @NonNull List<String> field_list, final boolean include);	
	
	/** Returns the list of objects specified by the spec (all fields returned)
	 * @param spec A specification generated by CrudUtils.allOf(...) (all fields must be match) or CrudUtils.anyOf(...) (any fields must match) together with extra fields generated by .withAny(..), .withAll(..), present(...) or notPresent(...)   
	 * @return A future containing a (possibly empty) list of Os - accessing the future will also report on errors via ExecutionException 
	 */
	@NonNull 
	CompletableFuture<Cursor<O>> getObjectsBySpec(final @NonNull QueryComponent<O> spec);
	
	/** Returns the list of objects/order/limit specified by the spec. Note that the resulting object should be run within a try-with-resources or read fully.
	 * @param spec A specification generated by CrudUtils.allOf(...) (all fields must be match) or CrudUtils.anyOf(...) (any fields must match) together with extra fields generated by .withAny(..), .withAll(..), present(...) or notPresent(...)   
	 * @param field_list List of fields to return, supports "." nesting
	 * @param include - if true, the field list is to be included; if false, to be excluded
	 * @return A future containing a (possibly empty) list of Os - accessing the future will also report on errors via ExecutionException 
	 */
	@NonNull 
	CompletableFuture<Cursor<O>> getObjectsBySpec(final @NonNull QueryComponent<O> spec, final @NonNull List<String> field_list, final boolean include);

	/** Counts the number of objects specified by the spec (all fields returned)
	 * @param spec A specification generated by CrudUtils.allOf(...) (all fields must be match) or CrudUtils.anyOf(...) (any fields must match) together with extra fields generated by .withAny(..), .withAll(..), present(...) or notPresent(...)   
	 * @return A future containing the number of matching objects - accessing the future will also report on errors via ExecutionException 
	 */
	@NonNull 
	CompletableFuture<Long> countObjectsBySpec(final @NonNull QueryComponent<O> spec);
	
	/** Counts the number of objects in the data store
	 * @param spec A specification generated by CrudUtils.allOf(...) (all fields must be match) or CrudUtils.anyOf(...) (any fields must match) together with extra fields generated by .withAny(..), .withAll(..), present(...) or notPresent(...)   
	 * @return A future containing the number of matching objects - accessing the future will also report on errors via ExecutionException 
	 */
	@NonNull 
	CompletableFuture<Long> countObjects();
	
	//////////////////////////////////////////////////////
	
	// *U*PDATE
	
	/** Updates the specified object
	 * @param id the id of the object to update
	 * @param update A specification to update the object (UpdateOperator.unset with field "" deletes the object) 
	 * @return a future describing if the update was successful - accessing the future will also report on errors via ExecutionException
	 */
	@NonNull 
	CompletableFuture<Boolean> updateObjectById(final @NonNull Object id, final @NonNull UpdateComponent<O> update);

	/** Updates the specified object
	 * @param unique_spec A specification (must describe at most one object) generated by CrudUtils.allOf(...) (all fields must be match) or CrudUtils.anyOf(...) (any fields must match) together with extra fields generated by .withAny(..), .withAll(..), present(...) or notPresent(...)   
	 * @param upsert if specified and true then inserts the object if it doesn't exist
	 * @param update A specification to update the object (UpdateOperator.unset with field "" deletes the object) 
	 * @return a future describing if the update was successful - accessing the future will also report on errors via ExecutionException
	 */
	@NonNull 
	CompletableFuture<Boolean> updateObjectBySpec(final @NonNull QueryComponent<O> unique_spec, final Optional<Boolean> upsert, final @NonNull UpdateComponent<O> update);

	/** Updates the specified object
	 * @param spec A specification generated by CrudUtils.allOf(...) (all fields must be match) or CrudUtils.anyOf(...) (any fields must match) together with extra fields generated by .withAny(..), .withAll(..), present(...) or notPresent(...)   
	 * @param upsert if specified and true then inserts the object if it doesn't exist
	 * @param update A specification to update the object (UpdateOperator.unset with field "" deletes the object) 
	 * @return a future describing the number of objects updated - accessing the future will also report on errors via ExecutionException
	 */
	@NonNull 
	CompletableFuture<Long> updateObjectsBySpec(final @NonNull QueryComponent<O> spec, final Optional<Boolean> upsert, final @NonNull UpdateComponent<O> update);

	/** Updates the specified object, returning the updated version
	 * @param unique_spec A specification (must describe at most one object) generated by CrudUtils.allOf(...) (all fields must be match) or CrudUtils.anyOf(...) (any fields must match) together with extra fields generated by .withAny(..), .withAll(..), present(...) or notPresent(...)   
	 * @param upsert if specified and true then inserts the object if it doesn't exist
	 * @param update A specification to update the object (UpdateOperator.unset with field "" deletes the object) 
	 * @param before_updated if specified and "true" then returns the object _before_ it is modified
	 * @param field_list List of fields to return, supports "." nesting
	 * @param include - if true, the field list is to be included; if false, to be excluded
	 * @return a future containing the object, if found (or upserted) - accessing the future will also report on errors via ExecutionException
	 */
	@NonNull 
	CompletableFuture<Optional<O>> updateAndReturnObjectBySpec(final @NonNull QueryComponent<O> unique_spec, final Optional<Boolean> upsert, final @NonNull UpdateComponent<O> update, final Optional<Boolean> before_updated, final @NonNull List<String> field_list, final boolean include);
	
	//////////////////////////////////////////////////////
	
	// *D*ELETE
	
	/** Deletes the specific object
	 * @param id the id of the object to update
	 * @return a future describing if the delete was successful - accessing the future will also report on errors via ExecutionException
	 */
	@NonNull 
	CompletableFuture<Boolean> deleteObjectById(final @NonNull Object id);

	/** Deletes the specific object
	 * @param unique_spec A specification (must describe at most one object) generated by CrudUtils.allOf(...) (all fields must be match) or CrudUtils.anyOf(...) (any fields must match) together with extra fields generated by .withAny(..), .withAll(..), present(...) or notPresent(...)   
	 * @return a future describing if the delete was successful - accessing the future will also report on errors via ExecutionException
	 */
	@NonNull 
	CompletableFuture<Boolean> deleteObjectBySpec(final @NonNull QueryComponent<O> unique_spec);

	/** Deletes the specific object
	 * @param A specification that must be initialized via CrudUtils.anyOf(...) and then the desired fields added via .exists(<field or getter>)
	 * @return a future describing the number of objects updated - accessing the future will also report on errors via ExecutionException
	 */
	@NonNull 
	CompletableFuture<Long> deleteObjectsBySpec(final @NonNull QueryComponent<O> spec);

	/** Deletes the entire datastore and all documents, including mappings/indexes/metdata etc
	 * @return a future describing if the delete was successful - accessing the future will also report on errors via ExecutionException
	 */
	@NonNull 
	CompletableFuture<Boolean> deleteDatastore();
	
	//////////////////////////////////////////////////////
	
	// OTHER:

	/** Returns an identical version of this CRUD service but using JsonNode instead of beans (which may save serialization)
	 * @return the JsonNode-genericized version of this same CRUD service
	 */
	@NonNull
	ICrudService<JsonNode> getRawCrudService();
	
	/** Returns a simple searchable ("Lucene-like") view of the data
	 * @return a search service
	 */
	@NonNull 
	Optional<IBasicSearchService<O>> getSearchService();
	
	/** USE WITH CARE: this returns the driver to the underlying technology
	 *  shouldn't be used unless absolutely necessary!
	 * @param driver_class the class of the driver
	 * @param a string containing options in some technology-specific format
	 * @return a driver to the underlying technology. Will exception if you pick the wrong one!
	 */
	@NonNull 
	<T> T getUnderlyingPlatformDriver(final @NonNull Class<T> driver_class, final Optional<String> driver_options);
}
