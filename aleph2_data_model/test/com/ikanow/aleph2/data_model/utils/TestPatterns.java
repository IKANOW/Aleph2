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

import java.util.Date;
import java.util.Optional;

import org.junit.Test;

public class TestPatterns {

	@Test
	public void test_MatchReturnTest_ClassAndConditions() {
		Object o = new Double(5.0); 
		String s = Patterns.match(o).<String>andReturn()
			.when(String.class, ss -> ss + ": string")
			.when(Double.class, t -> t < 4.0, d -> "1: " + d.getClass().toString())
			.when(Double.class, d -> "2: " + d.getClass().toString())
			.when(Boolean.class, b -> b ? "true" : "false")
			.otherwise(oo -> "unknown");

		assertEquals("Should fail condition and return on class match", "2: class java.lang.Double", s);
		
		o = new Double(3.0); 
		s = Patterns.match(o).<String>andReturn()
				.when(String.class, ss -> ss + ": string")
				.when(Double.class, t -> t < 4.0, d -> "1: " + d.getClass().toString())
				.when(Double.class, d -> "2: " + d.getClass().toString())
				.when(Boolean.class, b -> b ? "true" : "false")
				.otherwise(oo -> "unknown");		
		
		assertEquals("Should pass condition and return accordingly", "1: class java.lang.Double", s);
		
		// Test the default otherwise cases:
		
		try {			
			o = new Date(); 
			s = Patterns.match(o).<String>andReturn()
					.when(String.class, ss -> ss + ": string")
					.when(Double.class, t -> t < 4.0, d -> "1: " + d.getClass().toString())
					.when(Double.class, d -> "2: " + d.getClass().toString())
					.when(Boolean.class, b -> b ? "true" : "false")
					.otherwiseAssert();
			
			fail("Should have asserted:" + s);
		}
		catch (Exception e) {}
		try {			
			o = new Date(); 
			s = Patterns.match(o).<String>andReturn()
					.when(String.class, ss -> ss + ": string")
					.when(Double.class, t -> t < 4.0, d -> "1: " + d.getClass().toString())
					.when(Double.class, d -> "2: " + d.getClass().toString())
					.when(Boolean.class, b -> b ? "true" : "false")
					.otherwiseAssert(Optional.of("OtherwiseTest"));
			
			fail("Should have asserted:" + s);
		}
		catch (Exception e) {
			assertTrue("Should contain the assertion: " + e.getMessage(), e.getMessage().contains("OtherwiseTest"));
		}
		
	}

	private String _retVal;
	
	@Test
	public void test_MatchActTest_ClassAndConditions() {
		Object o = new Double(5.0); 
		_retVal = null;
		Patterns.match(o).andAct()
			.when(String.class, ss -> _retVal = (ss + ": string"))
			.when(Double.class, t -> t < 4.0, d -> _retVal = ("1: " + d.getClass().toString()))
			.when(Double.class, d -> _retVal = ("2: " + d.getClass().toString()))
			.when(Boolean.class, b -> _retVal = (b ? "true" : "false"))
			.otherwise(oo -> _retVal = ("unknown: " + oo));
		
		assertEquals("Should fail condition and assign on class match", "2: class java.lang.Double", _retVal);
		
		o = new Double(3.0); 
		_retVal = null;
		Patterns.match(o).andAct()
			.when(String.class, ss -> _retVal = (ss + ": string"))
			.when(Double.class, t -> t < 4.0, d -> _retVal = ("1: " + d.getClass().toString()))
			.when(Double.class, d -> _retVal = ("2: " + d.getClass().toString()))
			.when(Boolean.class, b -> _retVal = (b ? "true" : "false"))
			.otherwise(oo -> _retVal = ("unknown: " + oo));
		
		assertEquals("Should pass condition and assign accordingly", "1: class java.lang.Double", _retVal);
		
		try {
			o = new Date(); 
			Patterns.match(o).andAct()
			.when(String.class, ss -> _retVal = (ss + ": string"))
			.when(Double.class, t -> t < 4.0, d -> _retVal = ("1: " + d.getClass().toString()))
			.when(Double.class, d -> _retVal = ("2: " + d.getClass().toString()))
			.when(Boolean.class, b -> _retVal = (b ? "true" : "false"))
			.otherwiseAssert();			
		}
		catch (Exception e) {}
		
		try {
			o = new Date(); 
			Patterns.match(o).andAct()
			.when(String.class, ss -> _retVal = (ss + ": string"))
			.when(Double.class, t -> t < 4.0, d -> _retVal = ("1: " + d.getClass().toString()))
			.when(Double.class, d -> _retVal = ("2: " + d.getClass().toString()))
			.when(Boolean.class, b -> _retVal = (b ? "true" : "false"))
			.otherwiseAssert(Optional.of("OtherwiseTest"));
			
			fail("Should have asserted");
		}
		catch (Exception e) {
			assertTrue("Should contain the assertion: " + e.getMessage(), e.getMessage().contains("OtherwiseTest"));
		}
	}

	@Test
	public void MatchActTest_ClassAndConditions_AllowMultiple() {
		Object o = new Double(5.0); 
		_retVal = null;
		Patterns.match(o).andAct()
			.allowMultiple()
			.when(String.class, ss -> _retVal = (ss + ": string"))
			.when(Double.class, t -> t < 4.0, d -> _retVal = ("1: " + d.getClass().toString()))
			.when(Double.class, d -> _retVal = ("2: " + d.getClass().toString()))
			.when(Boolean.class, b -> _retVal = (b ? "true" : "false"))
			.when(Object.class, ooo -> _retVal += (" (object)"))
			.otherwise(oo -> _retVal = ("unknown: " + oo));
		
		assertEquals("Should fail condition and assign on class match", "2: class java.lang.Double (object)", _retVal);
		
		o = new Double(3.0); 
		_retVal = null;
		Patterns.match(o).andAct()
			.allowMultiple()
			.when(String.class, ss -> _retVal = (ss + ": string"))
			.when(Double.class, t -> t < 4.0, d -> _retVal = ("1: " + d.getClass().toString()))
			.when(Double.class, d -> _retVal += (" 2: " + d.getClass().toString()))
			.when(Boolean.class, b -> _retVal = (b ? "true" : "false"))
			.otherwise(oo -> _retVal = ("unknown: " + oo));
		
		assertEquals("Should pass condition and assign accordingly", "1: class java.lang.Double 2: class java.lang.Double", _retVal);
	}
	//TODO (ALEPH-3): handle otherwise cases
}
