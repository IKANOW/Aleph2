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
package com.ikanow.aleph2.core.shared.services;

import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Optional;

import org.junit.Test;

import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.MockManagementCrudService;
import com.ikanow.aleph2.data_model.utils.CrudUtils;

public class TestReadOnlyMultiCrudService {

	//TODO: test interception
	
	@Test
	public void test_multiWrite() {
		//TODO: handle the multi stuff
		
//		assertEquals(mock_crud.countObjects().join(), ro_crud.countObjects().join());
//		assertEquals(mock_crud.countObjectsBySpec(CrudUtils.allOf(String.class)).join(), ro_crud.countObjectsBySpec(CrudUtils.allOf(String.class)).join());
//		assertEquals(mock_crud.getObjectById("").join(), ro_crud.getObjectById("").join());
//		assertEquals(mock_crud.getObjectById("", Arrays.asList(), true).join(), ro_crud.getObjectById("", Arrays.asList(), true).join());
//		assertEquals(mock_crud.getObjectBySpec(CrudUtils.allOf(String.class)).join(), ro_crud.getObjectBySpec(CrudUtils.allOf(String.class)).join());
//		assertEquals(mock_crud.getObjectsBySpec(CrudUtils.allOf(String.class)).join().count(), ro_crud.getObjectsBySpec(CrudUtils.allOf(String.class)).join().count());
//		assertEquals(mock_crud.getObjectBySpec(CrudUtils.allOf(String.class), Arrays.asList(), true).join(), ro_crud.getObjectBySpec(CrudUtils.allOf(String.class), Arrays.asList(), true).join());
//		assertEquals(mock_crud.getObjectsBySpec(CrudUtils.allOf(String.class), Arrays.asList(), true).join().count(), ro_crud.getObjectsBySpec(CrudUtils.allOf(String.class), Arrays.asList(), true).join().count());
//		
//		// Check some simple derived types logic
//		
//		assertEquals(Optional.empty(), ro_crud.getSearchService());
//		assertEquals(Optional.empty(), ro_crud.getUnderlyingPlatformDriver(String.class, Optional.empty()));
//		{
//			ICrudService<JsonNode> json_ro_crud = ro_crud.getRawService();
//			
//			try {
//				json_ro_crud.updateObjectById("test", CrudUtils.update());
//				fail("Should have thrown: updateObjectById");
//			}
//			catch (Exception e) {}
//			assertEquals(ro_crud.countObjects().join(), json_ro_crud.countObjects().join());
//		}
//		{
//			ICrudService<String> filtered_crud = ro_crud.getFilteredRepo("test", Optional.empty(), Optional.empty());
//	
//			try {
//				filtered_crud.updateObjectById("test", CrudUtils.update(String.class));
//				fail("Should have thrown: updateObjectById");
//			}
//			catch (Exception e) {}
//			assertEquals(ro_crud.countObjects().join(), filtered_crud.countObjects().join());
//		}		
		
	}

	
	@Test
	public void test_readOnly() {
		
		final ICrudService<String> mock_crud = new MockManagementCrudService<String>();
		ICrudService<String> ro_crud = ReadOnlyMultiCrudService.from(mock_crud);
		
		// Try all the write operations:
		try {
			ro_crud.deleteDatastore();
			fail("Should have thrown: deleteDatastore");
		}
		catch (Exception e) {}
		try {
			ro_crud.deleteObjectById("id");
			fail("Should have thrown: deleteObjectById");
		}
		catch (Exception e) {}
		try {
			ro_crud.deleteObjectBySpec(CrudUtils.allOf(String.class));
			fail("Should have thrown: deleteObjectBySpec");
		}
		catch (Exception e) {}
		try {
			ro_crud.deleteObjectsBySpec(CrudUtils.allOf(String.class));
			fail("Should have thrown: deleteObjectsBySpec");
		}
		catch (Exception e) {}
		try {
			ro_crud.deregisterOptimizedQuery(Arrays.asList());
			fail("Should have thrown: deregisterOptimizedQuery");
		}
		catch (Exception e) {}
		try {
			ro_crud.optimizeQuery(Arrays.asList());
			fail("Should have thrown: optimizeQuery");
		}
		catch (Exception e) {}
		try {
			ro_crud.storeObject("test");
			fail("Should have thrown: storeObject");
		}
		catch (Exception e) {}
		try {
			ro_crud.storeObject("test", true);
			fail("Should have thrown: storeObject");
		}
		catch (Exception e) {}
		try {
			ro_crud.storeObjects(Arrays.asList("test"));
			fail("Should have thrown: storeObjects");
		}
		catch (Exception e) {}
		try {
			ro_crud.storeObjects(Arrays.asList("test"), true);
			fail("Should have thrown: storeObjects");
		}
		catch (Exception e) {}
		try {
			ro_crud.updateAndReturnObjectBySpec(CrudUtils.allOf(String.class), Optional.empty(), CrudUtils.update(String.class), Optional.empty(), Arrays.asList(), false);
			fail("Should have thrown: updateAndReturnObjectBySpec");
		}
		catch (Exception e) {}
		try {
			ro_crud.updateObjectById("test", CrudUtils.update(String.class));
			fail("Should have thrown: updateObjectById");
		}
		catch (Exception e) {}
		try {
			ro_crud.updateObjectBySpec(CrudUtils.allOf(String.class), Optional.empty(), CrudUtils.update(String.class));
			fail("Should have thrown: updateObjectBySpec");
		}
		catch (Exception e) {}
		try {
			ro_crud.updateObjectsBySpec(CrudUtils.allOf(String.class), Optional.empty(), CrudUtils.update(String.class));
			fail("Should have thrown: updateObjectsBySpec");
		}
		catch (Exception e) {}

	}
}
