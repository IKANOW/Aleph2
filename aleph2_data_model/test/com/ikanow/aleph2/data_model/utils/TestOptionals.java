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
package com.ikanow.aleph2.data_model.utils;

import static org.junit.Assert.*;

import java.util.Optional;

import org.junit.Test;

public class TestOptionals {

	public static class Chain1 {
		public static class Chain2 {
			public static class Chain3 {
				String val;
			}
			Chain3 chain3;
		}
		Chain2 chain2;
	}
	
	@Test
	public void test_accessorChain() {
		
		Chain1 c1 = new Chain1();
		Chain1.Chain2 c2 = new Chain1.Chain2();
		Chain1.Chain2.Chain3 c3 = new Chain1.Chain2.Chain3();
		c3.val = "test";
		c2.chain3 = c3;
		c1.chain2 = c2;		
		assertEquals("test", Optionals.of(() -> c1.chain2.chain3.val).get());		
		c1.chain2 = null;		
		assertEquals(Optional.empty(), Optionals.of(() -> c1.chain2.chain3.val));
		
	}
	
	//TODO (ALEPH-3): very quick 
}
