package com.ikanow.aleph2.data_model.interfaces.shared;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Future;

import org.checkerframework.checker.nullness.qual.NonNull;

/** A generic interface to an "object" datastore with a "MongoDB like" interface
 * @author acp
 *
 * @param <T> the bean type served by this repository
 */
public interface ICrudRepository<O> {

	// *C*REATE
	
	//TODO
	Future<Boolean> storeObject(@NonNull O new_object, boolean replace_if_present);

	//TODO default: fail if present
	Future<Boolean> storeObject(@NonNull O new_object);
	
	// *R*ETRIEVE
	
	/** Returns the object (in optional form to handle its not existing) given a simple object template that contains a unique search field (but other params are allowed)
	 * @param spec simple object template, must contain an id (or other unique field, and then multiple simple fields, ANDed together - collections are also treated as ANDs, eg list.of(a, b) will only match if both a and b are present)
	 * @return A future containing an optional containing the object, or Optionl.empty() 
	 */
	Future<Optional<O>> getObjectBySpec(@NonNull O spec);

	/** Returns the object given the id
	 * @param id the id of the object
	 * @return A future containing an optional containing the object, or Optionl.empty() 
	 */
	Future<Optional<O>> getObjectById(@NonNull String id);	
	
	//TODO more sophisticated search
	Future<List<O>> getObjects(@NonNull O spec);

	// *U*PDATE
	
	//TODO
	Future<Boolean> updateObjectById(String id, Optional<O> set, Optional<O> add, Optional<O> remove);

	//TODO
	Future<Boolean> updateObjectBySpec(@NonNull O spec, Optional<O> set, Optional<O> add, Optional<O> remove);

	//TODO bulk update
	
	// *D*ELETE
	
	//TODO
	Future<Boolean> deleteObjectById(@NonNull String id);

	//TODO
	Future<Boolean> deleteObjectBySpec(@NonNull O spec);
	
	// OTHER:
	
	/** USE WITH CARE: this returns the driver to the underlying technology
	 *  shouldn't be used unless absolutely necessary!
	 * @param driver_class the class of the driver
	 * @param a string containing options in some technology-specific format
	 * @return a driver to the underlying technology. Will exception if you pick the wrong one!
	 */
	<T> T getUnderlyingPlatformDriver(Class<T> driver_class, Optional<String> driver_options);
}
