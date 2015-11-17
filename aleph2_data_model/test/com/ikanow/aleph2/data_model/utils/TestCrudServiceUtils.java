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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;
import java.util.function.BiFunction;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.MockManagementCrudService;
import com.ikanow.aleph2.data_model.utils.CrudServiceUtils.ReadOnlyCrudService;
import com.ikanow.aleph2.data_model.utils.CrudServiceUtils.ReadOnlyManagementCrudService;

import fj.Unit;

public class TestCrudServiceUtils {

	@Test
	public void test_readOnlyCrudService() {
		final MockManagementCrudService<String> mock_crud = new MockManagementCrudService<String>();
		final ReadOnlyCrudService<String> ro_crud = new ReadOnlyCrudService<>(mock_crud);
		
		// Add a few dummy entries
		mock_crud.setMockValues(Arrays.asList("test1", "test2"));

		test_readOnly(ro_crud, mock_crud);
		
		assertEquals(Optional.of(ro_crud), ro_crud.getCrudService());		
	}
	@Test
	public void test_readOnlyManagementCrudService() {
		final MockManagementCrudService<String> mock_crud = new MockManagementCrudService<String>();
		final ReadOnlyManagementCrudService<String> ro_crud = new ReadOnlyManagementCrudService<>(mock_crud);
		
		// Add a few dummy entries
		mock_crud.setMockValues(Arrays.asList("test1", "test2"));

		test_readOnly(ro_crud, mock_crud);
		
		assertEquals(Optional.of(ro_crud), ro_crud.getCrudService());
	}
	public void test_readOnly(ICrudService<String> ro_crud, MockManagementCrudService<String> mock_crud) {
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
		
		assertEquals(mock_crud.countObjects().join(), ro_crud.countObjects().join());
		assertEquals(mock_crud.countObjectsBySpec(CrudUtils.allOf(String.class)).join(), ro_crud.countObjectsBySpec(CrudUtils.allOf(String.class)).join());
		assertEquals(mock_crud.getObjectById("").join(), ro_crud.getObjectById("").join());
		assertEquals(mock_crud.getObjectById("", Arrays.asList(), true).join(), ro_crud.getObjectById("", Arrays.asList(), true).join());
		assertEquals(mock_crud.getObjectBySpec(CrudUtils.allOf(String.class)).join(), ro_crud.getObjectBySpec(CrudUtils.allOf(String.class)).join());
		assertEquals(mock_crud.getObjectsBySpec(CrudUtils.allOf(String.class)).join().count(), ro_crud.getObjectsBySpec(CrudUtils.allOf(String.class)).join().count());
		assertEquals(mock_crud.getObjectBySpec(CrudUtils.allOf(String.class), Arrays.asList(), true).join(), ro_crud.getObjectBySpec(CrudUtils.allOf(String.class), Arrays.asList(), true).join());
		assertEquals(mock_crud.getObjectsBySpec(CrudUtils.allOf(String.class), Arrays.asList(), true).join().count(), ro_crud.getObjectsBySpec(CrudUtils.allOf(String.class), Arrays.asList(), true).join().count());
		
		// Check some simple derived types logic
		
		assertEquals(Optional.empty(), ro_crud.getSearchService());
		assertEquals(Optional.empty(), ro_crud.getUnderlyingPlatformDriver(String.class, Optional.empty()));
		{
			ICrudService<JsonNode> json_ro_crud = ro_crud.getRawService();
			
			try {
				json_ro_crud.updateObjectById("test", CrudUtils.update());
				fail("Should have thrown: updateObjectById");
			}
			catch (Exception e) {}
			assertEquals(ro_crud.countObjects().join(), json_ro_crud.countObjects().join());
		}
		{
			ICrudService<String> filtered_crud = ro_crud.getFilteredRepo("test", Optional.empty(), Optional.empty());
	
			try {
				filtered_crud.updateObjectById("test", CrudUtils.update(String.class));
				fail("Should have thrown: updateObjectById");
			}
			catch (Exception e) {}
			assertEquals(ro_crud.countObjects().join(), filtered_crud.countObjects().join());
		}		
	}

	@Test
	public void test_interceptCrud() {
		final MockManagementCrudService<String> mock_crud = new MockManagementCrudService<String>();
		
		final SetOnce<Unit> default_interceptor_called = new SetOnce<>();
		final SetOnce<Unit> other_interceptor_called = new SetOnce<>();
		
		final BiFunction<Object, Object[], Object> default_interceptor = (o1, o2) -> {
			default_interceptor_called.set(Unit.unit());
			return o1;
		};
		
		final HashMap<String, BiFunction<Object, Object[], Object>> interceptors = new HashMap<>();
		interceptors.put("updateObjectById", (o1, o2) -> {
			other_interceptor_called.set(Unit.unit());
			return o1;			
		});
		
		// Add a few dummy entries
		mock_crud.setMockValues(Arrays.asList("test1", "test2"));
		
		ICrudService<String> intercepted_crud = CrudServiceUtils.intercept(String.class, mock_crud, Optional.of(CrudUtils.allOf(String.class)), interceptors, Optional.of(default_interceptor));
		
		assertEquals(mock_crud.countObjectsBySpec(CrudUtils.allOf(String.class)).join(), intercepted_crud.countObjects().join());
		assertTrue("Called the default interceptor", default_interceptor_called.isSet());
		assertFalse("Didn't call the method specific interceptor", other_interceptor_called.isSet());
		
		assertEquals(Optional.of("test2"), intercepted_crud.getObjectById("test").join());
		assertEquals(Optional.of("test2"), intercepted_crud.getObjectById("test", Arrays.asList(), true).join());
		assertFalse("Didn't call the method specific interceptor", other_interceptor_called.isSet());
		
		assertEquals(true, intercepted_crud.deleteDatastore().join());
		assertEquals(false, intercepted_crud.deleteDatastore().join()); // (converts to a deleteBySpec hence returns false because nothing now present)
		assertFalse("Didn't call the method specific interceptor", other_interceptor_called.isSet());

		mock_crud.setMockValues(Arrays.asList("test1", "test2"));
		assertEquals(true, intercepted_crud.deleteObjectById(null).join());
		assertFalse("Didn't call the method specific interceptor", other_interceptor_called.isSet());
		
		// JSON version (+ test method specific interceptor)
		
		ICrudService<JsonNode> intercepted_json_crud = intercepted_crud.getRawService();
		
		assertEquals(true, intercepted_json_crud.updateObjectById(null, CrudUtils.update()).join());
		assertTrue("Called the method specific interceptor", other_interceptor_called.isSet());
		
		// Check it behaves normally with nothing inserted (+chaining)
		
		final ReadOnlyCrudService<String> ro_crud = new ReadOnlyCrudService<>(mock_crud);		
		ICrudService<String> intercepted_ro_crud = CrudServiceUtils.intercept(String.class, ro_crud, Optional.empty(), Collections.emptyMap(), Optional.empty());
		test_readOnly(intercepted_ro_crud, mock_crud);

		// Check read-only-ness is preserved:
		
		ICrudService<String> ro_intercepted_crud = intercepted_crud.readOnlyVersion();
		try {
			ro_intercepted_crud.deleteDatastore();
			fail("Should have thrown: deleteDatastore");
		}
		catch (Exception e) {}
		
	}
}
