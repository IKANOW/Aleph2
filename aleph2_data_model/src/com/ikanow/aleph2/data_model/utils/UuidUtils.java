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
package com.ikanow.aleph2.data_model.utils;

import java.util.Date;
import java.util.Optional;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.eaio.uuid.UUIDGen;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IUuidService;

public class UuidUtils implements IUuidService {

	protected final ObjectMapper _object_mapper; 
	
	/** Internal c'tor
	 */
	protected UuidUtils() {
		_object_mapper = BeanTemplateUtils.configureMapper(Optional.empty());
	}
	
	protected static UuidUtils _singleton = null;
	
	/** Get a static reference to a default UUID library
	 * @return
	 */
	public static IUuidService get() {
		if (null == _singleton) {
			_singleton = new UuidUtils();
		}
		// Create one of every UUID:
		_singleton.getTimeBasedUuid();
		_singleton.getRandomUuid();
		_singleton.getContentBasedUuid(_singleton.hashCode());
		return _singleton;
	}
	
	/** Returns a NON_UNIQUE uuid based on the current date
	 * @return a string representation of the type 2 UUID
	 */
	@Override
	public @NonNull String getTimeBasedUuid() {
		return new com.eaio.uuid.UUID(new Date().getTime(), UUIDGen.getClockSeqAndNode()).toString();
	}

	/** Returns a NON_UNIQUE uuid based on the specified date
	 * @param java_time - the date in java time
	 * @return a string representation of the type 2 UUID
	 */
	@Override
	public @NonNull String getTimeBasedUuid(final long java_time) {
		return new com.eaio.uuid.UUID(java_time, 0L).toString();
	}

	/** Gets the time from a UUID - must be a type 2 UUID, or errors will occur
	 * @param uuid
	 * @return the time in java time
	 */
	@Override
	public @NonNull long getTimeUuid(final @NonNull String uuid) {
		return new com.eaio.uuid.UUID(uuid).getTime();
	}

	/** Creates a random (type1) UUID
	 * @return a string representation of the type 1 UUID
	 */
	@Override
	public @NonNull String getRandomUuid() {
		return java.util.UUID.randomUUID().toString();
	}

	/** Generates a UUID based on the bean
	 * @param binary
	 * @return a string representation of the type 3 UUID
	 */
	@Override
	public <T> @NonNull String getContentBasedUuid(final @NonNull T bean) {
		return getContentBasedUuid(_object_mapper.valueToTree(bean));
	}

	/** Generates a UUID based on the JSON (warnings: 1) slow, 2) sensitive to field order)
	 * @param binary
	 * @return a string representation of the type 3 UUID
	 */
	@Override
	public <T> @NonNull String getContentBasedUuid(final @NonNull JsonNode json) {
		return getContentBasedUuid(json.toString().getBytes());
	}

	/** Generates a UUID based on the binary blob (slow - us with caution)
	 * @param binary
	 * @return a string representation of the type 3 UUID
	 */
	@Override
	public <T> @NonNull String getContentBasedUuid(final @NonNull byte[] binary) {
		return java.util.UUID.nameUUIDFromBytes(binary).toString();
	}
}
