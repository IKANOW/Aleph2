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
package com.ikanow.aleph2.data_import_manager.utils;

import static org.junit.Assert.*;

import java.util.regex.Pattern;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestPatternUtils {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testCreatePatternFromGlob() {
		final String glob1 = "a*";
		final String glob2 = "sdf?d\\f";
		final String glob3 = "asdf.*";		
		assertTrue(PatternUtils.createPatternFromGlob(glob1).pattern().equals("^a.*$"));
		assertTrue(PatternUtils.createPatternFromGlob(glob2).pattern().equals("^sdf.d\\\\f$"));
		assertTrue(PatternUtils.createPatternFromGlob(glob3).pattern().equals("^asdf\\..*$"));
	}
	
	@Test
	public void testCreatePatternFromRegex() {
		final String regex1 = "/aaa/i";
		final String regex2 = "/aaa/asdfghjkl";
		final String regex3 = "/dfd\\ddfk(f).*fk/i";
		final String regex4 = "/a/";
		assertTrue(PatternUtils.createPatternFromRegex(regex1).pattern().equals("aaa"));
		assertEquals(PatternUtils.createPatternFromRegex(regex1).flags(), PatternUtils.createPatternFromRegex(regex1).flags() & Pattern.CASE_INSENSITIVE);
		assertTrue(PatternUtils.createPatternFromRegex(regex2).pattern().equals("aaa"));
		assertEquals(PatternUtils.createPatternFromRegex(regex2).flags(), PatternUtils.createPatternFromRegex(regex2).flags() & (Pattern.DOTALL + Pattern.UNIX_LINES));
		assertTrue(PatternUtils.createPatternFromRegex(regex3).pattern().equals("dfd\\ddfk(f).*fk"));
		assertEquals(PatternUtils.createPatternFromRegex(regex3).flags(), PatternUtils.createPatternFromRegex(regex1).flags() & Pattern.CASE_INSENSITIVE);
		assertTrue(PatternUtils.createPatternFromRegex(regex4).pattern().equals("a"));
		assertEquals(0, PatternUtils.createPatternFromRegex(regex4).flags());
	}
	
	@Test
	public void testCreatePatternFromRegexOrGlob() {
		final String glob1 = "a*";
		final String regex1 = "/aaa/i";
		assertTrue(PatternUtils.createPatternFromRegexOrGlob(glob1).pattern().equals("^a.*$"));
		assertTrue(PatternUtils.createPatternFromRegexOrGlob(regex1).pattern().equals("aaa"));
		assertEquals(PatternUtils.createPatternFromRegexOrGlob(regex1).flags(), PatternUtils.createPatternFromRegexOrGlob(regex1).flags() & Pattern.CASE_INSENSITIVE);
	}

}
