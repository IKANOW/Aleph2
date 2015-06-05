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
	public static final String __DEFAULT_ZOOKEEPER_CONNECTION = "localhost:2181"; 
	
	/** User c'tor
	 * @param zookeeper_connection
	 */
	protected DistributedServicesPropertyBean(final String zookeeper_connection) {
		this.zookeeper_connection = zookeeper_connection;
	}

	/** Serializer c'tor
	 */
	public DistributedServicesPropertyBean() {}
	
	/** The connection string for Zookeeper
	 * @return
	 */
	public String zookeeper_connection() { return zookeeper_connection; }	
	
	private String zookeeper_connection;
}
