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

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.Test;

import com.ikanow.aleph2.data_model.interfaces.data_access.samples.ICustomService;
import com.ikanow.aleph2.data_model.interfaces.data_access.samples.SampleCustomServiceOne;
import com.ikanow.aleph2.data_model.interfaces.data_access.samples.SampleCustomServiceTwo;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class TestPropertiesUtils {

	@Test
	public void testGetSubConfig() {
		
		Map<String, Object> configMap = new HashMap<String, Object>();
		configMap.put("service.SampleCustomServiceOne.interface", ICustomService.class.getCanonicalName());
		configMap.put("service.SampleCustomServiceOne.service", SampleCustomServiceOne.class.getCanonicalName());	
		configMap.put("service.SampleCustomServiceOne.default", true);
		configMap.put("service.SampleCustomServiceTwo.interface", ICustomService.class.getCanonicalName());
		configMap.put("service.SampleCustomServiceTwo.service", SampleCustomServiceTwo.class.getCanonicalName());	
		configMap.put("service.SampleCustomServiceTwo.default", true);
		Config cfg = ConfigFactory.parseMap(configMap);
		
		Optional<Config> cfg1 = PropertiesUtils.getSubConfig(cfg, "service");
		Optional<Config> cfg2 = PropertiesUtils.getSubConfig(cfg, "serviceXXX");
		Optional<Config> cfg3 = PropertiesUtils.getSubConfig(cfg, "service.SampleCustomServiceOne");
		Optional<Config> cfg4 = PropertiesUtils.getSubConfig(cfg, "service.SampleCustomServiceOneXXX");
		Optional<Config> cfg5 = PropertiesUtils.getSubConfig(cfg, "service.SampleCustomServiceOne.default");
		Optional<Config> cfg6 = PropertiesUtils.getSubConfig(cfg, "service.SampleCustomServiceOne.defaultXXX");

		assertEquals(Optional.empty(), cfg2);
		assertEquals(Optional.empty(), cfg4);
		assertEquals(Optional.empty(), cfg5);
		assertEquals(Optional.empty(), cfg6);
		
		assertEquals(true, cfg1.isPresent());
		assertEquals(true, cfg3.isPresent());		
	}
}
