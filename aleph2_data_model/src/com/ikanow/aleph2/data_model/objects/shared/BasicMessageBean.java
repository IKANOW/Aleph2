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
package com.ikanow.aleph2.data_model.objects.shared;

import java.util.Date;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Very simple status/log message format within Aleph2
 * @author acp
 */
public class BasicMessageBean {

	public BasicMessageBean() {}
	
	/** User constructor
	 */
	public BasicMessageBean(@NonNull Date date, 								
								@Nullable String source,
								@Nullable String command,
								@Nullable Integer message_code,
								@NonNull String message) {
		this.date = date;
		this.source = source;
		this.message = message;
		this.message_code = message_code;
		this.command = command;
	}
	/** the date when the message was generated
	 * @return the date when the message was generated
	 */
	public Date date() {
		return date;
	}
	/** A freeform string representing the source of the message
	 * @return the source of the message
	 */
	public String source() {
		return source;
	}
	/** A freeform string representing the command that resulted in this message
	 * @return the command that spawned the message
	 */
	public String command() {
		return command;
	}
	/** An integer representing a message code for non-human
	 * @return the command that spawned the message
	 */
	public Integer message_code() {
		return message_code;
	}
	/** A freeform string containing the message itself
	 * @return
	 */
	public String message() {
		return message;
	}
	
	private Date date;
	private String source;
	private String command;
	private Integer message_code;
	private String message;
}
