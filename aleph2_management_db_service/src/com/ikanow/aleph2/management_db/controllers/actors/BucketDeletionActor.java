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
package com.ikanow.aleph2.management_db.controllers.actors;

/** This actor is responsible for ensuring that the data for the bucket is actually deleted
 *  Then deletes the bucket itself
 *  Replies to the bucket deletion singleton actor on success (or if the failure indicates that the bucket is already deleted...)
 * @author Alex
 */
public class BucketDeletionActor {

	//TODO: launch one per node via the ManagementDbActorContext
	// attach self to round robin queue
}
