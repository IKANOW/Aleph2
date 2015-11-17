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
package com.ikanow.aleph2.data_model.objects.shared;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;

public class TestGlobalPropertiesBean {

	@Test
	public void testUnpopulatedGlobals() {
		
		GlobalPropertiesBean globals = BeanTemplateUtils.build(GlobalPropertiesBean.class).done().get();
		
		assertEquals(globals.local_cached_jar_dir(), GlobalPropertiesBean.__DEFAULT_LOCAL_CACHED_JARS_DIR);
		assertEquals(globals.local_root_dir(), GlobalPropertiesBean.__DEFAULT_LOCAL_ROOT_DIR);
		assertEquals(globals.local_yarn_config_dir(), GlobalPropertiesBean.__DEFAULT_LOCAL_YARN_CONFIG_DIR);
		assertEquals(globals.distributed_root_dir(), GlobalPropertiesBean.__DEFAULT_DISTRIBUTED_ROOT_DIR);
	}
	
	@Test
	public void testPopulatedGlobals() {
		
		GlobalPropertiesBean globals = new GlobalPropertiesBean("a", "b", "c", "d");
		
		assertEquals(globals.local_root_dir(), "a");
		assertEquals(globals.local_yarn_config_dir(), "b");		
		assertEquals(globals.local_cached_jar_dir(), "c");
		assertEquals(globals.distributed_root_dir(), "d");		
	}
	
}
