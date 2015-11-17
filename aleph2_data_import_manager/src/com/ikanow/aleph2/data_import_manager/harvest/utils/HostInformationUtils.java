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
package com.ikanow.aleph2.data_import_manager.harvest.utils;

import java.net.InetAddress;

import com.ikanow.aleph2.data_model.utils.UuidUtils;

/** Provides static information about the host and process
 * @author acp
 *
 */
public class HostInformationUtils {

	private static String _hostname = null;
	private final static String _uuid;
	static {
		_uuid = UuidUtils.get().getRandomUuid();
	}
	
	/** Returns a UUID unique to this process
	 * @return the UUID (type 1)
	 */
	public static String getProcessUuid() {
		return _uuid;
	}
	
	/** Returns the hostname
	 * @return
	 */
	public static String getHostname() {
		// (just get the hostname once)
		if (null == _hostname) {
			try {
				_hostname = InetAddress.getLocalHost().getHostName();
			} catch (Exception e) {
				_hostname = "UNKNOWN";
			}
		}		
		return _hostname;
	}//TESTED		
}
