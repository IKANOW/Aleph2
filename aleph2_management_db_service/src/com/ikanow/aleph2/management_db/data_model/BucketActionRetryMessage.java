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
package com.ikanow.aleph2.management_db.data_model;

import java.util.Date;

import org.checkerframework.checker.nullness.qual.NonNull;

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
	
	/** User constructor
	 * @param source - the source that failed
	 * @param message - the message that failed
	 */
	public <T> BucketActionRetryMessage(
			final @NonNull String source,
			final @NonNull T message
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
