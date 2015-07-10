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

import java.util.Collections;
import com.ikanow.aleph2.distributed_services.data_model.IJsonSerializable;
import java.util.Set;

import akka.actor.ActorRef;

import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.distributed_services.data_model.IBroadcastEventBusWrapper;

/** Just a top level message type for handling bucket actions 
 * @author acp
 */
public class BucketActionMessage implements IJsonSerializable {
	protected BucketActionMessage() {} // (for bean template utils)
	private BucketActionMessage(final DataBucketBean bucket) { this.bucket = bucket; handling_clients = Collections.emptySet(); }
	private BucketActionMessage(final DataBucketBean bucket, final Set<String> handling_clients) { 
		this.bucket = bucket; 		
		this.handling_clients = handling_clients;
	}
	public Set<String> handling_clients() { return handling_clients; }
	private Set<String> handling_clients;
	public DataBucketBean bucket() { return bucket; };
	private DataBucketBean bucket;
	
	
	/** An internal class used to wrap event bus publications
	 * @author acp
	 */
	public static class BucketActionEventBusWrapper implements IBroadcastEventBusWrapper<BucketActionMessage> {
		protected BucketActionEventBusWrapper() { }
		/** User c'tor for wrapping a BucketActionMessage to be sent over the bus
		 * @param sender - the sender of the message
		 * @param message - the message to be wrapped
		 */
		public BucketActionEventBusWrapper(final ActorRef sender, final BucketActionMessage message) {
			this.sender = sender;
			this.message = message;
		}	
		@Override
		public ActorRef sender() { return sender; };
		@Override
		public BucketActionMessage message() { return message; };
		
		protected ActorRef sender;
		protected BucketActionMessage message;
	}	

	/** Offer a single node bucket to see who wants it
	 * @author acp
	 */
	public static class BucketActionOfferMessage extends BucketActionMessage {
		protected BucketActionOfferMessage() { super(null, null); }
		/** User c'tor for creating a message to see which nodes can handle the given bucket
		 * @param bucket - the bucket with an outstanding operation to apply
		 */
		public BucketActionOfferMessage(final DataBucketBean bucket) {
			super(bucket);
		}
	}	
	
	/** Send a new bucket action message to one (single node) or many (distributed node) buckets
	 * @author acp
	 */
	public static class NewBucketActionMessage extends BucketActionMessage {
		protected NewBucketActionMessage() { super(null, null); }
		/** User c'tor for creating a message to create a bucket
		 * @param bucket - the bucket to create
		 * @param is_suspended - whether the initial state is suspended
		 */
		public NewBucketActionMessage(final DataBucketBean bucket, final boolean is_suspended) {
			super(bucket);
			this.is_suspended = is_suspended;
		}
		public Boolean is_suspended() { return is_suspended; }
		protected Boolean is_suspended;
	}
	
	/** Updates an existing bucket
	 * @author acp
	 */
	public static class UpdateBucketActionMessage extends BucketActionMessage {
		protected UpdateBucketActionMessage() { super(null, null); }
		/** User c'tor for creating a message to update a bucket
		 * @param new_bucket - the updated bucket
		 * @param old_bucket - the old bucket
		 * @param handling_clients the nodes handling this bucket
		 */
		public UpdateBucketActionMessage(final DataBucketBean new_bucket,  final boolean is_enabled,
											final DataBucketBean old_bucket, 
												final Set<String> handling_clients)
		{
			super(new_bucket, handling_clients);
			this.old_bucket = old_bucket;
			this.is_enabled = is_enabled;
		}
		public DataBucketBean old_bucket() { return old_bucket; }
		public Boolean is_enabled() { return is_enabled; }
		protected DataBucketBean old_bucket;
		protected Boolean is_enabled;		
	}

	/** Updates the state of an existing bucket
	 * @author acp
	 */
	public static class UpdateBucketStateActionMessage extends BucketActionMessage {
		protected UpdateBucketStateActionMessage() { super(null, null); }
		/** User c'tor for creating a message to update a bucket's state
		 * @param bucket - the bucket whose state is being changed
		 * @param is_suspended - the suspension state
		 * @param handling_clients - the nodes handling this bucket
		 */
		public UpdateBucketStateActionMessage(final DataBucketBean bucket, 
				final boolean is_suspended, final Set<String> handling_clients)
		{
			super(bucket, handling_clients);
			this.is_suspended = is_suspended;
		}
		public Boolean is_suspended() { return is_suspended; }
		protected Boolean is_suspended;
	}
	
	/** Send a delete bucket action message with a set of hosts on which the harvester is believed to be running
	 * @author acp
	 */
	public static class DeleteBucketActionMessage extends BucketActionMessage {
		protected DeleteBucketActionMessage() { super(null, null); }
		/** User c'tor for creating a message to delete a bucket
		 * @param bucket - the bucket to delete
		 * @param handling_clients - the nodes handling this bucket
		 */
		public DeleteBucketActionMessage(final DataBucketBean bucket, final Set<String> handling_clients) {
			super(bucket, handling_clients);
		}
	}
}
