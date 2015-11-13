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
package com.ikanow.aleph2.distributed_services.utils;

import java.net.InetAddress;
import java.util.stream.Collectors;

import com.typesafe.config.Config;

/** Zookeeper configuration utils
 * @author Alex
 */
public class ZookeeperUtils {
	/** Mainly for debugging
	 * @param host
	 */
	public static void overrideHostname(final String host) {
		_hostname = host;
	}
	
	/** Builds a standard connection string (list of host:port) from the zoo.cfg file
	 * @param zookeeper_config
	 * @return the connection string
	 */
	public static String buildConnectionString(final Config zookeeper_config) {
		final int port = zookeeper_config.getInt("clientPort");
		final Config servers = zookeeper_config.getConfig("server");
		return servers.root().entrySet().stream().map(kv -> kv.getValue().unwrapped().toString().split(":")[0] + ":" + port).collect(Collectors.joining(","));
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
	private static String _hostname;
	
}
