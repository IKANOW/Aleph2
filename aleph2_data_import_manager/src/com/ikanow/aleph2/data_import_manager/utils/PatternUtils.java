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

import java.util.regex.Pattern;

/**
 * Util class for converting strings into Patterns.
 * 
 * @author Burch
 *
 */
public class PatternUtils {
	/**
	 * Converts a string into a pattern, see {@link createPatternFromRegex}
	 * or {@link createPatternFromGlob} for expected formats.
	 * 
	 * @param regex_or_glob
	 * @return
	 */
	public static Pattern createPatternFromRegexOrGlob(String regex_or_glob) {
		if ( regex_or_glob.startsWith("/") ) {
			return createPatternFromRegex(regex_or_glob);
		} else {
			return createPatternFromGlob(regex_or_glob);
		}
	}
	
	/**
	 * Creates a pattern from a regex, expects the input format to be:
	 *	/pattern/flags (e.g. /www\..*\.com/i )
	 * 
	 * @param regex
	 * @return
	 */
	public static Pattern createPatternFromRegex(final String regex) {
		final String r = regex.substring(1, regex.indexOf("/", 1));
		final int f = parseFlags(regex.substring(regex.indexOf("/", 1)));
		return Pattern.compile(r, f); 
	}
	
	/**
	 * Creates a pattern from a glob, expects the input format to be anything, 
	 * will always return in the pattern ^input$ with the replacements:
	 * * == .*
	 * ? == .
	 * . == \\.
	 * \\ == \\\\
	 * 
	 * @param glob
	 * @return
	 */
	public static Pattern createPatternFromGlob(final String glob) {
	    final StringBuilder out = new StringBuilder("^");
	    for(int i = 0; i < glob.length(); ++i) {
	        final char c = glob.charAt(i);
	        switch(c)
	        {
	        case '*': out.append(".*"); break;
	        case '?': out.append('.'); break;
	        case '.': out.append("\\."); break;
	        case '\\': out.append("\\\\"); break;
	        default: out.append(c);
	        }
	    }
	    out.append('$');
	    return Pattern.compile(out.toString(), Pattern.CASE_INSENSITIVE);
	}
	
	/**
	 * Converts a string of regex flags into a single int representing those
	 * flags for using in the java Pattern object
	 * 
	 * @param flagsStr
	 * @return
	 */
	public static int parseFlags(final String flagsStr) {
		int flags = 0;
		for (int i = 0; i < flagsStr.length(); ++i) {
			switch (flagsStr.charAt(i)) {
			case 'i':
				flags |= Pattern.CASE_INSENSITIVE;
				break;
			case 'x':
				flags |= Pattern.COMMENTS;
				break;
			case 's':
				flags |= Pattern.DOTALL;
				break;
			case 'm':
				flags |= Pattern.MULTILINE;
				break;
			case 'u':
				flags |= Pattern.UNICODE_CASE;
				break;
			case 'd':
				flags |= Pattern.UNIX_LINES;
				break;
			}
		}
		return flags;
	}
}
