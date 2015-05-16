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

import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;

/** Just a top level message type for handling bucket actions 
 * @author acp
 */
public class BucketActionMessage {

	private BucketActionMessage() {}

	/** Offer a single node bucket to see who wants it
	 * @author acp
	 */
	public static class BucketActionOfferMessage extends BucketActionMessage {
		public BucketActionOfferMessage(DataBucketBean bucket) {
			this.bucket = bucket;
		}
		public DataBucketBean bucket() { return bucket; };
		private final DataBucketBean bucket;
	}	
	
	/** Send a new bucket action message to one (single node) or many (distributed node) buckets
	 * @author acp
	 */
	public static class NewBucketActionMessage extends BucketActionMessage {
		//TODO (ALEPH-19)
	}
}
