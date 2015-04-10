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

public class TestObjectUtils {

	public static class TestBean {
		public String _testField() { return testField; } /** Test field */
		
		private String testField;
	}
	
	@Test
	public void testSingleMethodHelper() {
		
		String test1 = ObjectUtils.from(TestBean.class).field(TestBean::_testField);
		TestBean t = new TestBean();
		String test2 = ObjectUtils.from(t).field(TestBean::_testField);
		assertEquals("The type safe reference should resolve correctly (class ref)", "testField", test1);
		assertEquals("The type safe reference should resolve correctly (object ref)", "testField", test2);
	}
	
}
