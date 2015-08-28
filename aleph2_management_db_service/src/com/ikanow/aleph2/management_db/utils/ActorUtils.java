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
package com.ikanow.aleph2.management_db.utils;

/** Just provides a list of commonly accessor actor paths
 * @author acp
 *
 */
public class ActorUtils {
	public final static String BUCKET_ACTION_ZOOKEEPER = "/app/aleph2/bucket_actions";
	public final static String BATCH_ENRICHMENT_ZOOKEEPER = "/app/aleph2/batch_enrichment";
	public final static String STREAMING_ENRICHMENT_ZOOKEEPER = "/app/aleph2/streaming_enrichment";
	
	public final static String BUCKET_ACTION_EVENT_BUS = BUCKET_ACTION_ZOOKEEPER;
	public final static String STREAMING_ENRICHMENT_EVENT_BUS = STREAMING_ENRICHMENT_ZOOKEEPER;
	public final static String BUCKET_DELETION_BUS = "/app/aleph2/deletion_round_robin";

	
	public final static String BUCKET_ACTION_SUPERVISOR = "bucket_actions_supervisor";
	public final static String BUCKET_ACTION_HANDLER = "bucket_actions_handler";
	public final static String BUCKET_TEST_CYCLE_SINGLETON_ACTOR = "test_cycle_singleton";
	public final static String BUCKET_DELETION_SINGLETON_ACTOR = "deletion_singleton";
	public final static String BUCKET_DELETION_WORKER_ACTOR = "deletion_worker";
	
}
