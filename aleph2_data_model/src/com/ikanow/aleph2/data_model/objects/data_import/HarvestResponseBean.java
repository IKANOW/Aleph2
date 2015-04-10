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

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * A fairly flexible object returned by the harvester
 * to indicate the status of the command it received. 
 * @author acp
 *
 */
public class HarvestResponseBean {

	/** Create an immutable harvest response bean
	 * (use ObjectUtils to set its values(
	 */
	public HarvestResponseBean() {}
	
	/** Create an immutable harvest response bean
	 * @param success
	 * @param command
	 * @param code
	 * @param message
	 * @param details
	 */
	public HarvestResponseBean(Optional<Boolean> success, Optional<String> command, 
									Optional<Integer> code, Optional<String> message, Optional<Map<String, String>> details)
	{
		this.success = success.orElse(null); 
		this.command = command.orElse(null); 
		this.code = code.orElse(null); 
		this.message = message.orElse(null); 
		if (details.isPresent()) {
			this.details = Collections.unmodifiableMap(details.get()); 
		}
		else {
			this.details = null;
		}
	}									
	
	/**
	 * @return the success (optional, assumes true if not present)
	 */
	public Boolean _success() {
		return success;
	}
	/**
	 * @return the command (optional, for display purposes)
	 */
	public String _command() {
		return command;
	}
	/**
	 * @return the code (optional, for display purposes)
	 */
	public Integer _code() {
		return code;
	}
	/**
	 * @return the message (optional, for display purposes)
	 */
	public String _message() {
		return message;
	}
	/**
	 * @return the details (optional, for display purposes)
	 */
	public Map<String, String> _details() {
		return details;
	}
	private Boolean success;
	private String command;
	private Integer code;
	private String message;
	private Map<String, String> details;	
}
