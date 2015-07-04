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
package com.ikanow.aleph2.data_import.streaming_enrichment.storm;

import org.junit.Before;
import org.junit.Test;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;

public class TestPassthroughTopology {

	LocalCluster local_cluster;
	
	//TODO
	//@Before
	public void testSetup() {
		local_cluster = new LocalCluster();
		
		//TODO: also set up a service context with the usual suspects
	}

	//TODO
	//@Test
	public void test_passthroughTopology() {
		
		final StormTopology topology = null; //TODO
		
		final Config config = new Config();
		config.setDebug(true);
		local_cluster.submitTopology("test_passthroughTopology", config, topology);
		
		//TODO wait until complete?
		
		//TODO write some JsonNode objects into the kafka q
		
		//TODO wait
		
		//TODO check they appear in the appropriate CRUD service
		
	}
	
}
