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

import static org.junit.Assert.*;

import org.junit.Test;

import com.ikanow.aleph2.data_model.objects.shared.AssetStateDirectoryBean.StateDirectoryType;

public class TestAssetStateDirectoryBean {

	@Test
	public void test_assetStateDirectoryBean() {
		
		new AssetStateDirectoryBean();
		
		AssetStateDirectoryBean t1 = new AssetStateDirectoryBean("test1", StateDirectoryType.harvest, "test2", "test3");
		
		assertEquals("test1", t1.asset_path());
		assertEquals(StateDirectoryType.harvest, t1.state_type());
		assertEquals("test2", t1.collection_name());
		assertEquals("test3", t1._id());		
	}
}
