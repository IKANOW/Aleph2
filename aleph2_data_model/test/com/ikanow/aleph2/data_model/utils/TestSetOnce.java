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

import org.junit.Test;

public class TestSetOnce {

	@SuppressWarnings("deprecation")
	@Test
	public void testSetOnce() {
		
		SetOnce<Integer> set_once = new SetOnce<Integer>();
		
		assertTrue("Initially not set", !set_once.isSet());
		
		try {
			set_once.get();
			fail("Should have thrown an exception");
		}
		catch (Exception e) {
			assertEquals(RuntimeException.class, e.getClass());
		}
		
		assertTrue("Gets set: ", set_once.set(1));
		
		assertTrue("Now set", set_once.isSet());
		
		assertEquals((int)1, (int)set_once.get());
		
		assertTrue("Second set fails: ", !set_once.set(2));

		assertEquals((int)1, (int)set_once.get());
		
		assertEquals("override works", (int)2, (int)set_once.forceSet(2));
		
		assertEquals((int)2, (int)set_once.get());
		
		try {
			set_once.trySet(3);
			fail("Should have thrown exception");
		}
		catch (Exception e) {
			assertEquals((int)2, (int)set_once.get());			
		}
		
		SetOnce<Integer> set_once_2 = new SetOnce<Integer>(2);
		
		assertTrue("Starts set", set_once_2.isSet());
		
		assertEquals((int)2, (int)set_once_2.get());
		
		// Check the two other set methods work when used first
		
		SetOnce<Integer> set_once_3 = new SetOnce<Integer>();
		
		assertEquals((int)3, (int)set_once_3.trySet(3));
		
		SetOnce<Integer> set_once_4 = new SetOnce<Integer>();
		
		assertEquals((int)4, (int)set_once_4.forceSet(4));
		
	}
}
