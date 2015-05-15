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

import java.text.MessageFormat;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.google.common.collect.ObjectArrays;

public class ErrorUtils {

	/** Returns a formatting error string
	 * @param template - the template message in MessageFormat
	 * @param vars - the set of substituion variables
	 * @return - the formatted string
	 */
	@NonNull
	public static String get(final @NonNull String template, final @NonNull Object... vars) {
		return MessageFormat.format(template, vars);		
	}

	/** Returns a formatting error string, designed for exceptions
	 * @param template - the template message in MessageFormat
	 * @param t - the throwable, always has position {0}
	 * @param vars - the set of substituion variables
	 * @return - the formatted string
	 */
	@NonNull
	public static String get(final @NonNull String template, final Throwable t, final @NonNull Object... vars) {
		return MessageFormat.format(template, ObjectArrays.concat((Object)shortExceptionMessage(t), vars));		
	}

	/** Returns a formatting error string, designed for exceptions
	 * @param template - the template message in MessageFormat
	 * @param t - the throwable, always has position {0}
	 * @param vars - the set of substituion variables
	 * @return - the formatted string
	 */
	@NonNull
	public static String getLongForm(final @NonNull String template, final Throwable t, final @NonNull Object... vars) {
		return MessageFormat.format(template, ObjectArrays.concat((Object)longExceptionMessage(t), vars));		
	}

	/**Internal helper to format exception messages (short form)
	 * @param t - the throwable 
	 * @return - the formatted message
	 */
	@NonNull
	protected static String shortExceptionMessage(final @NonNull Throwable t) {
	    return ((t.getLocalizedMessage() == null) ? "(null)" : t.getLocalizedMessage()) 
	    		+ (t.getCause() == null ? "" : (" (" + shortExceptionMessage(t.getCause()) + ")"));
	}	
	
	/**Internal helper to format exception messages (long form)
	 * @param t - the throwable 
	 * @return - the formatted message
	 */
	@NonNull
	protected static String longExceptionMessage(final @NonNull Throwable t) {
		int n = 0;
		StringBuffer sb = new StringBuffer();
		String lastMethodName = null;
		String lastClassName = null;
		String lastFileName = null;
		StackTraceElement firstEl = null;
		StackTraceElement lastEl = null;
		sb.append("[").append(t.getMessage()).append(": ").append(t.getClass().getSimpleName()).append("]:");
		for (StackTraceElement el: t.getStackTrace()) {
			if (el.getClassName().contains("com.ikanow.") && (n < 20)) {
				if ((lastEl != null) && (lastEl != firstEl)) { // last non-ikanow element before the ikanow bit
					sb.append("[").append(lastEl.getFileName()).append(":").append(lastEl.getLineNumber()).append(":").append(lastEl.getClassName()).append(":").append(lastEl.getMethodName()).append("]");
					n += 2;				
					firstEl = null;
					lastEl = null;
				}//TESTED
				
				if (el.getClassName().equals(lastClassName) && el.getMethodName().equalsIgnoreCase(lastMethodName)) { // (overrides)
					sb.append("[").append(el.getLineNumber()).append("]");
					// (don't increment n in this case)
				}//(c/p of other clauses)
				else if (el.getClassName().equals(lastClassName)) { // different methods in the same class
					sb.append("[").append(el.getLineNumber()).append(":").append(el.getMethodName()).append("]");
					n++; // (allow more of these)
				}//TESTED
				else if (el.getFileName().equals(lastFileName)) { // different methods in the same class					
					sb.append("[").append(el.getLineNumber()).append(":").append(el.getClassName()).append(":").append(el.getMethodName()).append("]");
					n += 2;
				}//(c/p of other clauses)
				else {
					sb.append("[").append(el.getFileName()).append(":").append(el.getLineNumber()).append(":").append(el.getClassName()).append(":").append(el.getMethodName()).append("]");
					n += 3;
				}//TESTED
				lastMethodName = el.getMethodName();
				lastClassName = el.getClassName();
				lastFileName = el.getFileName();
			}
			else if (0 == n) {
				firstEl = el;
				sb.append("[").append(el.getFileName()).append(":").append(el.getLineNumber()).append(":").append(el.getClassName()).append(":").append(el.getMethodName()).append("]");
				n += 3;
			}//TESTED
			else if (null != firstEl) {
				lastEl = el;
			}
		}	
		if (null != t.getCause()) {
			sb.append(" (").append(longExceptionMessage(t.getCause())).append(")");
		}
		return sb.toString();
	}	
	
}
