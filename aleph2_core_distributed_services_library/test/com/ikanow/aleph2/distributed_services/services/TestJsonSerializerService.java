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
package com.ikanow.aleph2.distributed_services.services;

import static org.junit.Assert.*;

import java.io.UnsupportedEncodingException;

import org.junit.Test;

public class TestJsonSerializerService {

	public static class TestBean {
		protected TestBean() {}
		public String test() { return test; }
		private String test;
	};
	
	@Test
	public void standaloneSerializerTest() throws UnsupportedEncodingException {
		final JsonSerializerService sut = new JsonSerializerService();
		
		final TestBean test = new TestBean();
		test.test = "test";
		
		final byte[] test_bytes = sut.toBinary(test);
		final String test_str = new String(test_bytes, "UTF-8");
		assertEquals("{\"test\":\"test\"}", test_str);
		
		final TestBean test2 = (TestBean) sut.fromBinary(test_bytes, TestBean.class);
		assertEquals(test.test, test2.test);
	}
}
