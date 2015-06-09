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
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import org.apache.logging.log4j.Logger;

import com.google.common.collect.ObjectArrays;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.FutureUtils.ManagementFuture;

public class ErrorUtils {

	// Generic error messages
	
	public static final String INVALID_CONFIG_ERROR = "Invalid configuration for {0}: {1}";	
	
	// Interface
	
	/** Returns a formatting error string
	 * @param template - the template message in MessageFormat
	 * @param vars - the set of substituion variables
	 * @return - the formatted string
	 */
	public static String get(final String template, final Object... vars) {
		return MessageFormat.format(template, vars);		
	}

	/** Returns a formatting error string, designed for exceptions
	 * @param template - the template message in MessageFormat
	 * @param t - the throwable, always has position {0}
	 * @param vars - the set of substituion variables
	 * @return - the formatted string
	 */
	public static String get(final String template, final Throwable t, final Object... vars) {
		return MessageFormat.format(template, ObjectArrays.concat((Object)shortExceptionMessage(t), vars));		
	}

	/** Returns a formatting error string, designed for exceptions
	 * @param template - the template message in MessageFormat
	 * @param t - the throwable, always has position {0}
	 * @param vars - the set of substituion variables
	 * @return - the formatted string
	 */
	public static String getLongForm(final String template, final Throwable t, final Object... vars) {
		return MessageFormat.format(template, ObjectArrays.concat((Object)longExceptionMessage(t), vars));		
	}

	/**Internal helper to format exception messages (short form)
	 * @param t - the throwable 
	 * @return - the formatted message
	 */
	protected static String shortExceptionMessage(final Throwable t) {
	    return ((t.getLocalizedMessage() == null) ? "(null)" : t.getLocalizedMessage()) 
	    		+ (t.getCause() == null ? "" : (" (" + shortExceptionMessage(t.getCause()) + ")"));
	}	
	
	/**Internal helper to format exception messages (long form)
	 * @param t - the throwable 
	 * @return - the formatted message
	 */
	protected static String longExceptionMessage(final Throwable t) {
		int n = 0;
		StringBuffer sb = new StringBuffer();
		String lastMethodName = null;
		String lastClassName = null;
		String lastFileName = null;
		StackTraceElement firstEl = null;
		StackTraceElement lastEl = null;
		String message = "unknown_message_too_large";
		try {
			message = t.getMessage();
		}
		catch (Exception e) {
			// Try to handle known offenders for this:
			if (t instanceof com.google.inject.ConfigurationException) {
				com.google.inject.ConfigurationException ce1 = (com.google.inject.ConfigurationException) t;
				if (!ce1.getErrorMessages().isEmpty()) {
					message = ce1.getErrorMessages().iterator().next().toString();
				}
			}
			else if (t instanceof com.google.inject.CreationException) {
				com.google.inject.CreationException ce1 = (com.google.inject.CreationException) t;
				if (!ce1.getErrorMessages().isEmpty()) {
					message = ce1.getErrorMessages().iterator().next().toString();
				}				
			}
			else if (t instanceof com.google.inject.ProvisionException) {
				com.google.inject.ProvisionException ce1 = (com.google.inject.ProvisionException) t;
				if (!ce1.getErrorMessages().isEmpty()) {
					message = ce1.getErrorMessages().iterator().next().toString();
				}				
			}
			//else just carry on
		} 
		sb.append("[").append(message).append(": ").append(t.getClass().getSimpleName()).append("]:");
		
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
				else if ( el.getFileName() != null ) {
					if ( el.getFileName().equals(lastFileName)) { // different methods in the same class					
						sb.append("[").append(el.getLineNumber()).append(":").append(el.getClassName()).append(":").append(el.getMethodName()).append("]");
						n += 2;
					}//(c/p of other clauses)
					else {
						sb.append("[").append(el.getFileName()).append(":").append(el.getLineNumber()).append(":").append(el.getClassName()).append(":").append(el.getMethodName()).append("]");
						n += 3;
					}//TESTED
				}
				else { //filename is null, i've only noticed this occur in lambdas
					sb.append("[").append(el.getLineNumber()).append(":");
					sb.append(ErrorUtils.stripLambdaMethodNumber(el.getClassName()));
					sb.append(":").append(el.getMethodName()).append("]");
					n++;
				}
				lastMethodName = el.getMethodName();
				lastClassName = el.getClassName();
				lastFileName = el.getFileName();
			}
			else if (0 == n) {
				firstEl = el;
				sb.append("[");
				if ( el.getFileName() != null ) //lambdas don't have filenames
					sb.append(el.getFileName()).append(":");
				sb.append(el.getLineNumber()).append(":").append(el.getClassName()).append(":").append(el.getMethodName()).append("]");
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
	
	/**
	 * Lambdas make up a method name so it's not v helpful, strips it off
	 * e.g. com.ikanow.aleph2.data_model.utils.TestErrorUtils$$Lambda$1/1301664418
	 * becomes com.ikanow.aleph2.data_model.utils.TestErrorUtils$$Lambda$1
	 * 
	 * @param methodName
	 * @return
	 */
	private static String stripLambdaMethodNumber(final String methodName) {
		final int indexOfSlash = methodName.lastIndexOf("/");
		if (indexOfSlash < 0 )
			return methodName;
		else
			return methodName.substring(0, indexOfSlash);
	}
	
	/** This class encapsulates a basic message bean in an unchecked exception
	 * @author Alex
	 */
	public static class BasicMessageException extends RuntimeException {
		private static final long serialVersionUID = -322389853093290491L;
		protected final BasicMessageBean _message;
		public BasicMessageException(BasicMessageBean message) {
			_message = message;
		}
		public BasicMessageBean getMessageBean() { return _message; }
		@Override
		public String getMessage() {
			return _message.message();
		}
		@Override
		public String getLocalizedMessage() {
			return _message.message();
		}
	}

	/** Creates a string version of BasicMessageBean
	 * @param msg
	 * @param include_success - turn this on for prints, for logging turn off and use info/error based on success
	 * @return
	 */
	public static String show(final BasicMessageBean msg) {
		return "[" + msg.date() + "] " + msg.source() + " (" + msg.command() + "): " + ((msg.success() ? "INFO" : "ERROR")) + ": " + msg.message();		
	}
	
	/** Logs the side channel of a management future - when you only have the management future (ie mgmt_future.getManagementResults
	 * @param logger
	 * @param mgmt_future - the original management future
	 */
	public static void logManagedFuture(final Logger logger, final ManagementFuture<?> mgmt_future) {
		logManagedFutureSideChannel(logger, mgmt_future.getManagementResults());
	}
	
	/** Logs the side channel of a management future - when you only have the side channel future (ie mgmt_future.getManagementResults)
	 * @param logger
	 * @param mgmt_future_side_channel - the return value from mgmt_future.getManagementResults(), or the exceptionallyWithSideChannel method
	 */
	public static void logManagedFutureSideChannel(final Logger logger, final CompletableFuture<Collection<BasicMessageBean>> mgmt_future_side_channel) {
		mgmt_future_side_channel.thenAccept(msgs -> logManagedFutureSideChannel(logger, msgs))
		.exceptionally(t -> {
			logger.error(ErrorUtils.getLongForm("managementFuture.getManagementResults failed: {0}", t));
			return null;
		});
	}
	
	/** Logs the side channel of a management future - when you have the result of the future (ie mgmt_future.getManagementResults)
	 * @param logger
	 * @param mgmt_future_side_channel - the return value from mgmt_future.getManagementResults().join/get/then*, or from combineWithSideChannel
	 */
	public static void logManagedFutureSideChannel(final Logger logger, Collection<BasicMessageBean> mgmt_future_side_channel) {
		for (BasicMessageBean msg : mgmt_future_side_channel) {
			final String log = show(msg); 
			if (msg.success())
				logger.info(log);
			else
				logger.error(log);
		}			
	}
	
}
