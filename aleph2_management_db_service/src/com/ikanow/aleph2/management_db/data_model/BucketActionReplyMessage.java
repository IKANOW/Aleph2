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

import java.util.List;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;

/** Just a top level message type for handling bucket actions 
 * @author acp
 */
public class BucketActionReplyMessage {

	public static class BucketActionTimeoutMessage extends BucketActionReplyMessage {}
	
	public static class BucketActionCollectedRepliesMessage extends BucketActionReplyMessage {
		public BucketActionCollectedRepliesMessage(List<BasicMessageBean> replies) {
			this.replies = replies;
		}		
		public List<BasicMessageBean> replies() { return replies; }
		private final List<BasicMessageBean> replies;
	}
	
	public static class BucketActionIgnoredMessage extends BucketActionReplyMessage {
		
		public BucketActionIgnoredMessage(final @NonNull String uuid) {
			this.uuid = uuid;
		}
		public String uuid() { return uuid; }
		private final String uuid;
	}
	
	public static class BucketActionHandlerMessage extends BucketActionReplyMessage {
		
		public BucketActionHandlerMessage(final @NonNull String uuid, final @NonNull BasicMessageBean reply) {
			this.uuid = uuid;
			this.reply = reply;
		}
		public String uuid() { return uuid; }
		public BasicMessageBean reply() { return reply; }
		
		private final String uuid;
		private final BasicMessageBean reply;
	}
}
