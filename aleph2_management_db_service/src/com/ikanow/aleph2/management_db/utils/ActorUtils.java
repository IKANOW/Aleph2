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
package com.ikanow.aleph2.management_db.utils;

/** Just provides a list of commonly accessor actor paths
 * @author acp
 *
 */
public class ActorUtils {
	public final static String BUCKET_ACTION_ZOOKEEPER = "/app/aleph2/bucket_actions";
	public final static String BATCH_ENRICHMENT_ZOOKEEPER = "/app/aleph2/batch_enrichment";
	public final static String BUCKET_ANALYTICS_ZOOKEEPER = "/app/aleph2/bucket_analytics";
	public final static String BUCKET_ANALYTICS_TRIGGER_ZOOKEEEPER = "/app/aleph2/analytics_triggers/";
	
	public final static String BUCKET_ACTION_EVENT_BUS = BUCKET_ACTION_ZOOKEEPER;
	public final static String BUCKET_ANALYTICS_EVENT_BUS = BUCKET_ANALYTICS_ZOOKEEPER;
	public final static String BUCKET_DELETION_BUS = "/app/aleph2/deletion_round_robin";
	public final static String ANALYTICS_TRIGGER_BUS = "/app/aleph2/analytics_trigger_round_robin";
	
	public final static String BUCKET_ACTION_SUPERVISOR = "bucket_actions_supervisor";
	public final static String BUCKET_ACTION_HANDLER = "bucket_actions_handler";
	public final static String BUCKET_TEST_CYCLE_SINGLETON_ACTOR = "test_cycle_singleton";
	public final static String BUCKET_DELETION_SINGLETON_ACTOR = "deletion_singleton";
	public final static String BUCKET_DELETION_WORKER_ACTOR = "deletion_worker";
	public final static String BUCKET_POLL_FREQUENCY_SINGLETON_ACTOR = "poll_freq_singleton";
	public final static String SECURITY_CACHE_INVALIDATION_SINGLETON_ACTOR = "security_cache_invalidation_singleton";
}
