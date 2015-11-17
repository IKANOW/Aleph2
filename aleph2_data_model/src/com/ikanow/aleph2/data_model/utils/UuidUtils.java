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
 *******************************************************************************/
package com.ikanow.aleph2.data_model.utils;

import java.util.Optional;

import com.eaio.uuid.UUIDGen;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IUuidService;

/** Implementation of UUID service corresponding to com.eaio.uuid.UUIDGen
 *  Note that this code is tightly coupled to the internals of that code and cannot
 *  be swapped for another underlying library without modifying this one
 *  (Use the TestUuidUtils to check, including temporarily commenting the "debug test" in
 * @author Alex
 */
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
	
	/** Returns a UNIQUE uuid based on the current date (adjusts the time by a milli-second to make it unique where needed)
	 * @return a string representation of the type 2 UUID
	 */
	@Override
	public String getTimeBasedUuid() {
		return new com.eaio.uuid.UUID(UUIDGen.createTime(new java.util.Date().getTime()), UUIDGen.getClockSeqAndNode()).toString();
	}

	/** Returns a NON-UNIQUE uuid based on the specified date 
	 * @param java_time - the date in java time
	 * @return a string representation of the type 2 UUID
	 */
	@Override
	public String getTimeBasedUuid(final long java_time) {
		// (taken code from UUIDGen.createTime but without the monotonicity enforcement)
		long new_time = (java_time * 10000) + 0x01B21DD213814000L;
		new_time = new_time << 32 | ((new_time & 0xFFFF00000000L) >> 16) | (0x1000 | ((new_time >> 48) & 0x0FFF));
		
		return new com.eaio.uuid.UUID(new_time, UUIDGen.getClockSeqAndNode()).toString();
	}

	/** Gets the time from a UUID - must be a type 2 UUID, or errors will occur
	 * @param uuid
	 * @return the time in java time
	 */
	@Override
	public long getTimeUuid(final String uuid) {
		return getTimeFromTimeField(new com.eaio.uuid.UUID(uuid).getTime());
	}

	/** Returns the 
	 * @param time_field
	 * @return
	 */
	public long getTimeFromTimeField(long time_field) {
		long tmp_time = ((time_field >> 32) & 0x00000000FFFFFFFFL) | ((time_field << 16) & 0x0000FFFF00000000L) | ((time_field & 0x0000000000000FFF) << 48);

		tmp_time -= 0x01B21DD213814000L;
		tmp_time /= 10000L;
		
		return tmp_time;
	}
	
	/** Creates a random (type1) UUID
	 * @return a string representation of the type 1 UUID
	 */
	@Override
	public String getRandomUuid() {
		return java.util.UUID.randomUUID().toString();
	}

	/** Generates a UUID based on the bean
	 * @param binary
	 * @return a string representation of the type 3 UUID
	 */
	@Override
	public <T> String getContentBasedUuid(final T bean) {
		return getContentBasedUuid(_object_mapper.valueToTree(bean));
	}

	/** Generates a UUID based on the JSON (warnings: 1) slow, 2) sensitive to field order)
	 * @param binary
	 * @return a string representation of the type 3 UUID
	 */
	@Override
	public <T> String getContentBasedUuid(final JsonNode json) {
		return getContentBasedUuid(json.toString().getBytes());
	}

	/** Generates a UUID based on the binary blob (slow - us with caution)
	 * @param binary
	 * @return a string representation of the type 3 UUID
	 */
	@Override
	public <T> String getContentBasedUuid(final byte[] binary) {
		return java.util.UUID.nameUUIDFromBytes(binary).toString();
	}
}
