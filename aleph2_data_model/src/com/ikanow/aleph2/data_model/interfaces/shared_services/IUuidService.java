package com.ikanow.aleph2.data_model.interfaces.shared_services;


import com.fasterxml.jackson.databind.JsonNode;

/** A service for generating different UUID strings
 * @author acp
 */
public interface IUuidService {

	//TODO (ALEPH-20): hmm I think there's a really strong argument to implement this 
	// inside the data model as a Util, ie have a UuidUtils that implements this service
	// and a static accessor - then you can override it from guice, but there's a default
	// accessor that's in place?
	
	/** Generates a (type 1) UUID based string from now
	 * @return time-based UUID string
	 */
	public String getTimeBasedUuid();
	
	/** Generates a (type 1) UUID based string from time
	 * @param the timestamp of the UUID
	 * @return time-based UUID string
	 */
	public String getTimeBasedUuid(final long time);	
	
	/** The time associated with the UUID
	 * @param uuid - must be time based or the result is undefined
	 * @return the time of the UUID in milliseconds from Unix Epoch
	 */
	public long getTimeUuid(final String uuid);
	
	/** Returns a random (type 3) UUID 
	 * @return UUID string
	 */
	public String getRandomUuid();
	
	/** Generates a UUID "unique to" the specified bean
	 * @param bean
	 * @return a UUID based on the json-ification of the bean
	 */
	public <T> String getContentBasedUuid(final T bean);
	
	/** Generates a UUID "unique to" the specified JSON object (field order matters)
	 * @param JSON object
	 * @return a UUID based on the json-ification of the bean
	 */
	public <T> String getContentBasedUuid(final JsonNode json);
	
	/** Generates a UUID "unique to" the specified "blob"
	 * @param "blob"
	 * @return a UUID based on the json-ification of the bean
	 */
	public <T> String getContentBasedUuid(final byte[] binary);
}
