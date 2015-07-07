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

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;

public class TestZookeeperUtils {

	@Test
	public void test_buildConnectionString() throws IOException {
	
		final Config config = ConfigFactory.parseResources("com/ikanow/aleph2/distributed_services/utils/zoo.cfg", 
				ConfigParseOptions.defaults().setSyntax(ConfigSyntax.PROPERTIES));
				
		assertEquals("Zookeeper string is correct", "api001.dev.ikanow.com:2181,db001.dev.ikanow.com:2181", ZookeeperUtils.buildConnectionString(config));		
	}	
}
