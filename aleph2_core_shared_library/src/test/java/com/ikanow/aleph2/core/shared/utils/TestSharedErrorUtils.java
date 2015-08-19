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
package com.ikanow.aleph2.core.shared.utils;

import static org.junit.Assert.*;

import java.util.Date;

import org.junit.Test;

import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;

public class TestSharedErrorUtils {

	public static class TestMessage {		
	};	
	
	@Test
	public void testGenerateMessageBean() {

		final String ref_name = "test";
		
		final BasicMessageBean test1 = 
				SharedErrorUtils.buildErrorMessage(ref_name, new TestMessage(), "TEST ERROR NO PARAMS {0}");
		
		assertEquals(test1.command(), "TestMessage");
		assertEquals((double)test1.date().getTime(), (double)((new Date()).getTime()), 1000.0);
		assertEquals(test1.details(), null);
		assertEquals(test1.message(), "TEST ERROR NO PARAMS {0}");
		assertEquals(test1.message_code(), null);
		assertEquals(test1.source(), ref_name);
		assertEquals(test1.success(), false);
		
		final BasicMessageBean test2 = 
				SharedErrorUtils.buildErrorMessage(ref_name, new TestMessage(), SharedErrorUtils.SHARED_LIBRARY_NAME_NOT_FOUND, 2L);
		
		assertEquals(test2.command(), "TestMessage");
		assertEquals((double)test2.date().getTime(), (double)((new Date()).getTime()), 1000.0);
		assertEquals(test2.details(), null);
		assertEquals(test2.message(), "Shared library {1} not found: 2");
		assertEquals(test2.message_code(), null);
		assertEquals(test2.source(), ref_name);
		assertEquals(test2.success(), false);
		
		final BasicMessageBean test3 = 
				SharedErrorUtils.buildErrorMessage(ref_name, new TestMessage(), SharedErrorUtils.SHARED_LIBRARY_NAME_NOT_FOUND, "3a", "3b");
		
		assertEquals(test3.command(), "TestMessage");
		assertEquals((double)test3.date().getTime(), (double)((new Date()).getTime()), 1000.0);
		assertEquals(test3.details(), null);
		assertEquals(test3.message(), "Shared library 3b not found: 3a");
		assertEquals(test3.message_code(), null);
		assertEquals(test3.source(), ref_name);
		assertEquals(test3.success(), false);
		
	}
}
