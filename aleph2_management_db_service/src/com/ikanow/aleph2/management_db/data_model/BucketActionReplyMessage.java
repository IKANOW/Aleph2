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

import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.distributed_services.data_model.IJsonSerializable;

/** Just a top level message type for handling bucket actions 
 * @author acp
 */
public class BucketActionReplyMessage implements IJsonSerializable {

	private BucketActionReplyMessage() {}
	
	/** System message indicating that not all messages have been replied to in time
	 * @author acp
	 */
	public static class BucketActionTimeoutMessage extends BucketActionReplyMessage {
		@SuppressWarnings("unused")
		private BucketActionTimeoutMessage() {}
		/** User c'tor for creating a timeout message
		 * @param source - the source (host) that timed out
		 */
		public BucketActionTimeoutMessage(final String source) {
			this.source = source;
		}
		public String source() { return source; }
		private String source;
	}
	
	/** The message a BucketAction*Actor sends out when it is complete
	 * @author acp
	 */
	public static class BucketActionCollectedRepliesMessage extends BucketActionReplyMessage {
		@SuppressWarnings("unused")
		private BucketActionCollectedRepliesMessage() {}
		/** User c'tor for creating a message encapsulating the reply from 0+ sources (hosts) that handled a message
		 * @param replies - replies from any hosts that handled the message or timed out trying
		 * @param timed_out - the set of hosts that timed out
		 */
		public BucketActionCollectedRepliesMessage(final List<BasicMessageBean> replies, Set<String> timed_out)
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
		
		/** User c'tor for a message indicating that the given data import manager can handle a bucket message
		 * @param source - the source accepting the offer
		 */
		public BucketActionWillAcceptMessage(final String source) {
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
		
		/** User c'tor for a message indicating that the given data import manager _will not_ handle a bucket message
		 * @param source - the source accepting the offer
		 */
		public BucketActionIgnoredMessage(final String source) {
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
		
		/** Creates a message encapsulating the reply from any bucket action
		 * @param source - the handling source (host)
		 * @param reply - the reply from tha source
		 */
		public BucketActionHandlerMessage(final String source, final BasicMessageBean reply) {
			this.source = source;
			this.reply = reply;
		}
		public String source() { return source; }
		public BasicMessageBean reply() { return reply; }
		
		private String source;
		private BasicMessageBean reply;
	}
}
