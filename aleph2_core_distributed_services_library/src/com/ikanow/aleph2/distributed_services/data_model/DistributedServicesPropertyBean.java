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
package com.ikanow.aleph2.distributed_services.data_model;

import java.util.Collections;
import java.util.Map;


/** Holds configuration information relating to the Core Distrubuted Services
 * @author acp
 *
 */
public class DistributedServicesPropertyBean {

	// Constants for "raw" access
	public static final String PROPERTIES_ROOT = "CoreDistributedServices"; 
	public static final String ZOOKEEPER_CONNECTION = "CoreDistributedServices.zookeeper_connection"; 
	public static final String BROKER_LIST = "CoreDistributedServices.broker_list";
	public static final String APPLICATION_NAME = "CoreDistributedServices.application_name";
	public static final String __DEFAULT_ZOOKEEPER_CONNECTION = "localhost:2181";
	public static final String __DEFAULT_BROKER_LIST = "localhost:6667";	 
	public static final String __DEFAULT_CLUSTER_NAME = "aleph2";	 
	
	public static final String ZOOKEEPER_APPLICATION_LOCK = "/app/aleph2/locks/zookeeper";
	
	/** User c'tor
	 * @param zookeeper_connection
	 */
	protected DistributedServicesPropertyBean(final String zookeeper_connection, final String broker_list, final String application_name, final Map<String, Integer> application_port) {
		this.zookeeper_connection = zookeeper_connection;
		this.broker_list = broker_list;
		this.application_name = application_name;
		this.application_port = application_port;
	}

	/** Serializer c'tor
	 */
	public DistributedServicesPropertyBean() {}
	
	/** The connection string for zookeeper - taken either from config, or from <YARN_CONFIG_PATH>/zoo.cfg, or defaulting to localhost:2181
	 */
	public String zookeeper_connection() { return zookeeper_connection; }	
	
	private String zookeeper_connection;

	/** The broker list for Kafka - taken from zookeeper if possible, else from this config param. or defaulting to localhost:6667
	 */
	public String broker_list() { return broker_list; }
	
	private String broker_list;

	/** The name of this application in the cluster - that plus the hostname are required to be unique per node
	 * @return
	 */
	public String application_name() { return application_name; }
	
	private String application_name;
	
	/** A map of application names vs fixed ports for the akka cluster
	 * @return immutable copy of map
	 */
	public Map<String, Integer> application_port() { return null != application_port ? Collections.unmodifiableMap(application_port) : null; }
	
	public Map<String, Integer> application_port;
	
	/** Allows users to overwrite the default cluster name from "aleph2" (note not all services - eg ZK) have the concept of a cluster
	 * @return
	 */
	public String cluster_name() { return cluster_name; }
	private String cluster_name;
}
