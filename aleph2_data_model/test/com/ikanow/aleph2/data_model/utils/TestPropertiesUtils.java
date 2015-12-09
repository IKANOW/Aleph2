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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import com.ikanow.aleph2.data_model.interfaces.data_access.samples.ICustomService;
import com.ikanow.aleph2.data_model.interfaces.data_access.samples.SampleCustomServiceOne;
import com.ikanow.aleph2.data_model.interfaces.data_access.samples.SampleCustomServiceTwo;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class TestPropertiesUtils {

	@Test
	public void test_GetSubConfig() {
		
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
	
	@Test
	public void test_mergeProperties() throws IOException {
		
		final String temp_dir = System.getProperty("java.io.tmpdir") + File.separator;
		final String conf_dir_str = temp_dir + "test_merge_conf_dir";
		
		final File dir = new File(conf_dir_str);
		final File default_conf = new File(temp_dir + "default.properties");
		default_conf.delete();
		assertFalse(default_conf.exists());
		FileUtils.forceMkdir(dir);
		final Collection<File> conf_files = FileUtils.listFiles(dir, Arrays.asList("conf", "properties", "json").toArray(new String[0]), false);
		conf_files.forEach(f -> f.delete());
		final Collection<File> conf_files_deleted = FileUtils.listFiles(dir, Arrays.asList("conf", "properties", "json").toArray(new String[0]), false);
		assertTrue(conf_files_deleted.isEmpty());
		
		// Fallback file:
		FileUtils.write(default_conf, "a=test_1\nb=test2");
		
		final File merge1 = new File(conf_dir_str + File.separator + "a_test.properties");
		final File merge2 = new File(conf_dir_str + File.separator + "b_test.properties");
		final File merge3 = new File(conf_dir_str + File.separator + "c_test.properties");
		
		FileUtils.write(merge1, "b=test_2.1\nc=test3");
		FileUtils.write(merge2, "c=test_3.1\nd=test4");
		FileUtils.write(merge3, "d=test_4.1\ne=test5");
		
		final Config conf = PropertiesUtils.getMergedConfig(Optional.of(dir), default_conf);
		
		assertEquals(Arrays.asList("a", "b", "c", "d", "e"), conf.root().keySet().stream().sorted().collect(Collectors.toList()));
		assertEquals("test_1", conf.getString("a"));
		assertEquals("test_2.1", conf.getString("b"));
		assertEquals("test_3.1", conf.getString("c"));
		assertEquals("test_4.1", conf.getString("d"));
		assertEquals("test5", conf.getString("e"));
		
		final Config conf2 = PropertiesUtils.getMergedConfig(Optional.empty(), default_conf);
		assertEquals(Arrays.asList("a", "b"), conf2.root().keySet().stream().sorted().collect(Collectors.toList()));
		
	}
}
