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
package com.ikanow.aleph2.management_db.data_model;

import java.util.Date;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;

/** A simple class to encapsulate messages that need to be retried because they have failed
 * @author acp
 *
 */
public class BucketActionRetryMessage {

	/** Jackson constructor
	 */
	protected BucketActionRetryMessage() {}
	
	/** User constructor to create a retry message for a host/message combo
	 * @param source - the source that failed
	 * @param message - the message that failed
	 */
	public <T> BucketActionRetryMessage(
			final String source,
			final T message
			)
	{
		inserted = new Date();
		last_checked = new Date();
		this.source = source;
		message_clazz = message.getClass().getName();
		this.message = BeanTemplateUtils.toJson(message);
	}
	public String _id() { return _id; }
	public Date inserted() { return inserted; }
	public Date last_checked() { return last_checked; }
	public String source() { return source; }
	public String message_clazz() { return message_clazz; }
	public JsonNode message() { return message; }
	
	private String _id; // (auto-inserted by system, otherwise onyl retrieved by Jackson)
	private Date inserted;
	private Date last_checked;
	private String source;
	private String message_clazz;
	
	@JsonProperty("message")
	public void setMessage(JsonNode json_node) {
		message = json_node;
	}
	JsonNode message;
}
