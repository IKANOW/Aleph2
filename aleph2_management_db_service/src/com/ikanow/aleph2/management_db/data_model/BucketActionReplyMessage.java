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

import java.io.Serializable;
import java.util.List;
import java.util.Set;

import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;

/** Just a top level message type for handling bucket actions 
 * @author acp
 */
public class BucketActionReplyMessage implements Serializable {
	private static final long serialVersionUID = -4001957953720739902L;

	private BucketActionReplyMessage() {}
	
	/** System message indicating that not all messages have been replied to in time
	 * @author acp
	 */
	public static class BucketActionTimeoutMessage extends BucketActionReplyMessage implements Serializable {
		private static final long serialVersionUID = -4567739291666424032L;
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
	public static class BucketActionCollectedRepliesMessage extends BucketActionReplyMessage implements Serializable {
		private static final long serialVersionUID = -4541373129314672428L;
		@SuppressWarnings("unused")
		private BucketActionCollectedRepliesMessage() {}
		/** User c'tor for creating a message encapsulating the reply from 0+ sources (hosts) that handled a message
		 * @param replies - replies from any hosts that handled the message or timed out trying
		 * @param timed_out - the set of hosts that timed out
		 */
		public BucketActionCollectedRepliesMessage(final String source, final List<BasicMessageBean> replies, Set<String> timed_out, Set<String> rejected)
		{
			this.source = source;
			this.replies = replies;
			this.timed_out = timed_out;
			this.rejected = rejected;
		}		
		public String source() { return source; }
		public List<BasicMessageBean> replies() { return replies; }
		public Set<String> timed_out() { return timed_out; }
		public Set<String> rejected() { return rejected; }
		private List<BasicMessageBean> replies;
		private Set<String> timed_out;
		private Set<String> rejected;
		private String source;
	}
	
	/** When a data import manager will accept a bucket action
	 * @author acp
	 */
	public static class BucketActionWillAcceptMessage extends BucketActionReplyMessage implements Serializable {
		private static final long serialVersionUID = -3486747240844926323L;
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
	public static class BucketActionIgnoredMessage extends BucketActionReplyMessage implements Serializable {
		private static final long serialVersionUID = 4808455789257375566L;
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
	public static class BucketActionHandlerMessage extends BucketActionReplyMessage implements Serializable {
		private static final long serialVersionUID = 8098547692966611294L;
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
	
	/** A reply that is ignored by everyone and should not be sent on when encountered
	 * @author Alex
	 */
	public static class BucketActionNullReplyMessage extends BucketActionReplyMessage implements Serializable {
		private static final long serialVersionUID = 5399489379330665287L;
		public BucketActionNullReplyMessage() {}
	}
}
