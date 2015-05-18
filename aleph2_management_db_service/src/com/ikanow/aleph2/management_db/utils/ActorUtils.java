package com.ikanow.aleph2.management_db.utils;

/** Just provides a list of commonly accessor actor paths
 * @author acp
 *
 */
public class ActorUtils {
	public final static String BUCKET_ACTION_ZOOKEEPER = "/app/aleph2/bucket_actions";
	public final static String BUCKET_ACTION_EVENT_BUS = BUCKET_ACTION_ZOOKEEPER;
	public final static String BUCKET_ACTION_SUPERVISOR = "bucket_actions_supervisor";
	public final static String BUCKET_ACTION_HANDLER = "bucket_actions_handler";
}
