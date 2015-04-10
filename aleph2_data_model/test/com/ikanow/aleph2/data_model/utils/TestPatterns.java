/*******************************************************************************
 * Copyright 2015, The IKANOW Open Source Project.
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package com.ikanow.aleph2.data_model.utils;

import static org.junit.Assert.*;

import org.junit.Test;

public class TestPatterns {

	@Test
	public void MatchReturnTest_ClassAndConditions() {
		Object o = new Double(5.0); 
		String s = Patterns.matchAndReturn(o)
			.when(String.class, ss -> ss + ": string")
			.when(Double.class, t -> t < 4.0, d -> "1: " + d.getClass().toString())
			.when(Double.class, d -> "2: " + d.getClass().toString())
			.when(Boolean.class, b -> b ? "true" : "false")
			.otherwise("unknown");

		assertEquals("Should fail condition and return on class match", "2: class java.lang.Double", s);
		
		o = new Double(3.0); 
		s = Patterns.matchAndReturn(o)
				.when(String.class, ss -> ss + ": string")
				.when(Double.class, t -> t < 4.0, d -> "1: " + d.getClass().toString())
				.when(Double.class, d -> "2: " + d.getClass().toString())
				.when(Boolean.class, b -> b ? "true" : "false")
				.otherwise("unknown");		
		
		assertEquals("Should pass condition and return accordingly", "1: class java.lang.Double", s);
		
	}

	private String _retVal;
	
	@Test
	public void MatchActTest_ClassAndConditions() {
		Object o = new Double(5.0); 
		_retVal = null;
		Patterns.matchAndAct(o)
			.when(String.class, ss -> _retVal = (ss + ": string"))
			.when(Double.class, t -> t < 4.0, d -> _retVal = ("1: " + d.getClass().toString()))
			.when(Double.class, d -> _retVal = ("2: " + d.getClass().toString()))
			.when(Boolean.class, b -> _retVal = (b ? "true" : "false"))
			.otherwise(oo -> _retVal = ("unknown: " + oo));
		
		assertEquals("Should fail condition and assign on class match", "2: class java.lang.Double", _retVal);
		
		o = new Double(3.0); 
		_retVal = null;
		Patterns.matchAndAct(o)
			.when(String.class, ss -> _retVal = (ss + ": string"))
			.when(Double.class, t -> t < 4.0, d -> _retVal = ("1: " + d.getClass().toString()))
			.when(Double.class, d -> _retVal = ("2: " + d.getClass().toString()))
			.when(Boolean.class, b -> _retVal = (b ? "true" : "false"))
			.otherwise(oo -> _retVal = ("unknown: " + oo));
		
		assertEquals("Should pass condition and assign accordingly", "1: class java.lang.Double", _retVal);
	}

	@Test
	public void MatchActTest_ClassAndConditions_AllowMultiple() {
		Object o = new Double(5.0); 
		_retVal = null;
		Patterns.matchAndAct(o)
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
		Patterns.matchAndAct(o)
			.allowMultiple()
			.when(String.class, ss -> _retVal = (ss + ": string"))
			.when(Double.class, t -> t < 4.0, d -> _retVal = ("1: " + d.getClass().toString()))
			.when(Double.class, d -> _retVal += (" 2: " + d.getClass().toString()))
			.when(Boolean.class, b -> _retVal = (b ? "true" : "false"))
			.otherwise(oo -> _retVal = ("unknown: " + oo));
		
		assertEquals("Should pass condition and assign accordingly", "1: class java.lang.Double 2: class java.lang.Double", _retVal);
	}
	//TODO: handle otherwise cases
}
