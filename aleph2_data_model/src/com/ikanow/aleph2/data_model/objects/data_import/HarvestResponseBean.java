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
package com.ikanow.aleph2.data_model.objects.data_import;

import java.util.Map;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A fairly flexible object returned by the harvester
 * to indicate the status of the command it received. 
 * @author acp
 *
 */
public class HarvestResponseBean {

	public HarvestResponseBean() {}
	
	/** User constructor
	 */
	public HarvestResponseBean(@NonNull Boolean success, @NonNull String command, 
								@NonNull Integer code, @Nullable String message, @Nullable Map<String, String> details)
	{
		this.success = success; 
		this.command = command; 
		this.code = code; 
		this.message = message; 
		this.details = details; 
	}									
	
	/**
	 * @return the success (optional, assumes true if not present)
	 */
	public Boolean success() {
		return success;
	}
	/**
	 * @return the command (optional, for display purposes)
	 */
	public String command() {
		return command;
	}
	/**
	 * @return the code (optional, for display purposes)
	 */
	public Integer code() {
		return code;
	}
	/**
	 * @return the message (optional, for display purposes)
	 */
	public String message() {
		return message;
	}
	/**
	 * @return the details (optional, for display purposes)
	 */
	public Map<String, String> details() {
		return details;
	}
	private Boolean success;
	private String command;
	private Integer code;
	private String message;
	private Map<String, String> details;	
}
