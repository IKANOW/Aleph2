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
import java.util.Set;

import org.checkerframework.checker.nullness.qual.NonNull;

import akka.actor.ActorRef;

import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;

/** Just a top level message type for handling bucket actions 
 * @author acp
 */
public class BucketActionMessage {
	private BucketActionMessage(final @NonNull DataBucketBean bucket) { this.bucket = bucket; handling_clients = Collections.emptySet(); }
	private BucketActionMessage(final @NonNull DataBucketBean bucket, final @NonNull Set<String> handling_clients) { 
		this.bucket = bucket; 		
		this.handling_clients = handling_clients;
	}
	@NonNull public Set<String> handling_clients() { return handling_clients; }
	private final Set<String> handling_clients;
	@NonNull public DataBucketBean bucket() { return bucket; };
	private final DataBucketBean bucket;
	
	
	/** An internal class used to wrap event bus publications
	 * @author acp
	 */
	public static class BucketActionEventBusWrapper {
		@SuppressWarnings("unused")
		private BucketActionEventBusWrapper() { }
		public BucketActionEventBusWrapper(final @NonNull ActorRef sender, final @NonNull BucketActionMessage message) {
			this.sender = sender;
			this.message = message;
		}	
		@NonNull
		public ActorRef sender() { return sender; };
		@NonNull
		public BucketActionMessage message() { return message; };
		
		protected ActorRef sender;
		protected BucketActionMessage message;
	}	

	/** Offer a single node bucket to see who wants it
	 * @author acp
	 */
	public static class BucketActionOfferMessage extends BucketActionMessage {
		private BucketActionOfferMessage() { super(null, null); }
		public BucketActionOfferMessage(final @NonNull DataBucketBean bucket) {
			super(bucket);
		}
	}	
	
	/** Send a new bucket action message to one (single node) or many (distributed node) buckets
	 * @author acp
	 */
	public static class NewBucketActionMessage extends BucketActionMessage {
		private NewBucketActionMessage() { super(null, null); }
		public NewBucketActionMessage(final @NonNull DataBucketBean bucket, final boolean is_suspended) {
			super(bucket);
			this.is_suspended = is_suspended;
		}
		protected Boolean is_suspended() { return is_suspended; }
		protected Boolean is_suspended;
	}
	
	/** Send a delete bucket action message with a set of hosts on which the harvester is believed to be running
	 * @author acp
	 */
	public static class DeleteBucketActionMessage extends BucketActionMessage {
		private DeleteBucketActionMessage() { super(null, null); }
		public DeleteBucketActionMessage(final @NonNull DataBucketBean bucket, final @NonNull Set<String> handling_clients) {
			super(bucket, handling_clients);
		}
	}
}
