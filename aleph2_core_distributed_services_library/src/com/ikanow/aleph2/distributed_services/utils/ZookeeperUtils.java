/*******************************************************************************
* Copyright 2015, The IKANOW Open Source Project.
* 
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License, version 3,
* as published by the Free Software Foundation.
* 
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Affero General Public License for more details.
* 
* You should have received a copy of the GNU Affero General Public License
* along with this program. If not, see <http://www.gnu.org/licenses/>.
******************************************************************************/
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
