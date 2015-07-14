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
******************************************************************************/
package com.ikanow.aleph2.data_import_manager.stream_enrichment.utils;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.ikanow.aleph2.data_import_manager.stream_enrichment.utils.StormControllerUtil;

public class TestStormControllerUtil {

	@Test
	public void test_createTopologyName() {
		assertEquals("path-to-bucket__", StormControllerUtil.bucketPathToTopologyName("/path/to/bucket"));
		assertEquals("path-to-bucket-more__", StormControllerUtil.bucketPathToTopologyName("/path/to/bucket/more"));
		assertEquals("path-_----more__", StormControllerUtil.bucketPathToTopologyName("/path/+-__////more"));
	}
	
}
