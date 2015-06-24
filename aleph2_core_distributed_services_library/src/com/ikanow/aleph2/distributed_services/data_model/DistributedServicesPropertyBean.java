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


/** Holds configuration information relating to the Core Distrubuted Services
 * @author acp
 *
 */
public class DistributedServicesPropertyBean {

	// Constants for "raw" access
	public static final String PROPERTIES_ROOT = "CoreDistributedServices"; 
	public static final String ZOOKEEPER_CONNECTION = "CoreDistributedServices.zookeeper_connection"; 
	public static final String BROKER_LIST = "CoreDistributedServices.broker_list"; 
	public static final String __DEFAULT_ZOOKEEPER_CONNECTION = "localhost:2181";
	public static final String __DEFAULT_BROKER_LIST = "localhost:6667";	 
	
	/** User c'tor
	 * @param zookeeper_connection
	 */
	protected DistributedServicesPropertyBean(final String zookeeper_connection, final String broker_list) {
		this.zookeeper_connection = zookeeper_connection;
		this.broker_list = broker_list;
	}

	/** Serializer c'tor
	 */
	public DistributedServicesPropertyBean() {}
	
	/** The connection string for Zookeeper
	 * @return
	 */
	public String zookeeper_connection() { return zookeeper_connection; }	
	
	private String zookeeper_connection;

	public String broker_list() { return broker_list; }
	
	private String broker_list;
}
