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
import java.util.Set;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;

/** Just a top level message type for handling bucket actions 
 * @author acp
 */
public class BucketActionReplyMessage {

	private BucketActionReplyMessage() {}
	
	/** System message indicating that not all messages have been replied to in time
	 * @author acp
	 */
	public static class BucketActionTimeoutMessage extends BucketActionReplyMessage {
		@SuppressWarnings("unused")
		private BucketActionTimeoutMessage() {}
		public BucketActionTimeoutMessage(final @NonNull String source) {
			this.source = source;
		}
		@NonNull public String source() { return source; }
		private String source;
	}
	
	/** The message a BucketAction*Actor sends out when it is complete
	 * @author acp
	 */
	public static class BucketActionCollectedRepliesMessage extends BucketActionReplyMessage {
		@SuppressWarnings("unused")
		private BucketActionCollectedRepliesMessage() {}
		public BucketActionCollectedRepliesMessage(final @NonNull List<BasicMessageBean> replies, Set<String> timed_out)
		{
			this.replies = replies;
			this.timed_out = timed_out;			
		}		
		public List<BasicMessageBean> replies() { return replies; }
		public Set<String> timed_out() { return timed_out; }
		private List<BasicMessageBean> replies;
		private Set<String> timed_out;
	}
	
	/** When a data import manager will accept a bucket action
	 * @author acp
	 */
	public static class BucketActionWillAcceptMessage extends BucketActionReplyMessage {
		@SuppressWarnings("unused")
		private BucketActionWillAcceptMessage() {}
		
		public BucketActionWillAcceptMessage(final @NonNull String source) {
			this.source = source;
		}
		public String source() { return source; }
		private String source;
	}
	
	/** When a data import manager cannot or does not wish to handle a bucket action message
	 * @author acp
	 */
	public static class BucketActionIgnoredMessage extends BucketActionReplyMessage {
		@SuppressWarnings("unused")
		private BucketActionIgnoredMessage() {}
		
		public BucketActionIgnoredMessage(final @NonNull String source) {
			this.source = source;
		}
		public String source() { return source; }
		private String source;
	}
	
	/** Encapsulates the reply from any requested bucket action
	 * @author acp
	 */
	public static class BucketActionHandlerMessage extends BucketActionReplyMessage {
		@SuppressWarnings("unused")
		private BucketActionHandlerMessage() {}
		
		public BucketActionHandlerMessage(final @NonNull String source, final @NonNull BasicMessageBean reply) {
			this.source = source;
			this.reply = reply;
		}
		public String source() { return source; }
		public BasicMessageBean reply() { return reply; }
		
		private String source;
		private BasicMessageBean reply;
	}
}
