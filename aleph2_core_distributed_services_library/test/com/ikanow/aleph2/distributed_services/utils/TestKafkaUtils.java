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

import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.distributed_services.services.MockCoreDistributedServices;

public class TestKafkaUtils {

	
	MockCoreDistributedServices _cds;
	
	@Before
	public void setupCoreDistributedServices() throws Exception {
		_cds = new MockCoreDistributedServices();	
	}
	
	@Test
	public void test_kafkaConnectionFromZookeeper() throws Exception {
		final ObjectMapper mapper = BeanTemplateUtils.configureMapper(Optional.empty());
		assertEquals(Integer.toString(_cds.getKafkaBroker().getBrokerPort()), 
				KafkaUtils.getBrokerListFromZookeeper(_cds.getCuratorFramework(), Optional.empty(), mapper).split(":")[1]);
		assertEquals(Integer.toString(_cds.getKafkaBroker().getBrokerPort()), 
				KafkaUtils.getBrokerListFromZookeeper(_cds.getCuratorFramework(), Optional.of("/brokers/ids"), mapper).split(":")[1]);
	}
	
}
