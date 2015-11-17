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
package com.ikanow.aleph2.data_model.interfaces.data_import;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;


import com.ikanow.aleph2.data_model.objects.data_import.BucketDiffBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.ProcessingTestSpecBean;

/** Harvesters are responsible from taking objects in arbitrary formats over arbitrary transport protocols,
 *  and doing one of the following things:
 *   - writing JSON objects to a real-time queue provided by the context
 *   - dumping JSON/XML/CSV/Seq files in the designated HDFS directory
 * @author acp
 *
 */
public interface IHarvestTechnologyModule {

	/** This function is guaranteed to always be called before any callback is called, and can therefore be used to store the context
	 *  It can be used to store the context (which is always passed in anyway) and therefore set up any code that needs to be run.
	 *  It should return as quickly as possible, eg not block on any lengthy operations.
	 * @param context - the context for which an instance of this module has been created
	 */
	void onInit(final IHarvestContext context);
	
	/** This function should check the local environment, decide whether the technology module can run on this node
	 *  (eg is the external software installed? are the environment variables set up correctly etc) and return 
	 *  true/false as quickly as possibly. (Will often default to just 'return true;') 
	 * @param bucket - the bucket to check against (mostly this will be ignored, ie the function will just decide based on the technology module alone - but this enables the code to be cleverer, eg check the sub-modules as well)
	 * @param context - the context for which an instance of this module has been created
	 * @return true if this node can run this module's functionality
	 */
	boolean canRunOnThisNode(final DataBucketBean bucket, final IHarvestContext context);
	
	/** Whether this engine can be run in multi-node (defaults to true)
	 * @param bucket - the bucket to check against (mostly this will be ignored, ie the function will just decide based on the technology module alone - but this enables the code to be cleverer, eg check the sub-modules as well)
	 * @param context - the context for which an instance of this module has been created
	 * @return whether this engine can be run in multi-node (defaults to true)
	 */
	default boolean supportsMultiNode(final DataBucketBean bucket, final IHarvestContext context) {
		return true;
	}
	
	/**
	 * Handles either a new bucket associated with this harvester, or an existing bucket
	 * that was previously associated with a different harvester.
	 * 
	 * @param new_bucket - a new bucket associated with this harvester
	 * @param context - the context available to this harvester
	 * @param enabled - whether the bucket is enabled
	 * @return A future for the response
	 */
	CompletableFuture<BasicMessageBean> onNewSource(final DataBucketBean new_bucket, final IHarvestContext context, final boolean enabled);
	
	/**
	 * Handles changes to an existing bucket
	 * 
	 * @param old_bucket - the updated bucket
	 * @param new_bucket - the updated bucket
	 * @param is_enabled - whether the bucket is currently enabled (note - you cannot infer anything about the previous enabled/suspended state unless the diff optional is present)
	 * @param diff - optionally what information has changed (including changes to shared library beans). If this is not present (it will not be in early versions of the platform), developers must assume everything has changed, eg restarting the external service. 
	 * @param context - the context available to this harvester
	 * @return A future for the response
	 */
	CompletableFuture<BasicMessageBean> onUpdatedSource(final DataBucketBean old_bucket, final DataBucketBean new_bucket, final boolean is_enabled, final Optional<BucketDiffBean> diff, final IHarvestContext context);
	
	/**
	 * Notification that all data for this bucket is to be purged
	 * Note that the actual purging is performed by the framework, so this is in 
	 * case the state needs to be updated etc.
	 * 
	 * @param to_purge - the bucket that is going to be purged
	 * @param context - the context available to this harvester
	 * @return A future for the response
	 */
	CompletableFuture<BasicMessageBean> onPurge(final DataBucketBean to_purge, final IHarvestContext context);
	
	/**
	 * Notification that this bucket is being deleted.
	 * The framework is responsible for removing all data associated with the bucket
	 * It is the harvester's responsibility to stop collecting/forwarding data (and not resolve
	 * the BasicMessageBean until then).
	 * 
	 * @param to_delete - the bucket that is being deleted
	 * @param context - the context available to this harvester
	 * @return A future for the response
	 */
	CompletableFuture<BasicMessageBean> onDelete(final DataBucketBean to_delete, final IHarvestContext context);
	
	/**
	 * Notification that this bucket is no longer the responsibility of this node (eg the node failed for too long and was removed from the bucket's node affinity).
	 * This method should check that any external processes running on this node are cleaned up - but the bucket itself's state has not changed, therefore eg if 
	 * managing a distributed technology then the bucket shoud be left alone.
	 * 
	 * @param to_delete - the bucket that is being deleted
	 * @param context - the context available to this harvester
	 * @return A future for the response
	 */
	CompletableFuture<BasicMessageBean> onDecommission(final DataBucketBean to_decommission, final IHarvestContext context);
	
	/**
	 * Periodic poll for statistics collection, health checks, etc.
	 * The poll frequency is determined by the bucket
	 * @param polled_bucket The bucket being polled
	 * @param context - the context available to this harvester
	 * @return A future for the response
	 */
	CompletableFuture<BasicMessageBean> onPeriodicPoll(final DataBucketBean polled_bucket, final IHarvestContext context);
	
	/**
	 * For batch type harvest technologies (eg file not streaming), this callback is called when a batch is complete
	 * @param completed_bucket The bucket whose batch has completed
	 * @param context - the context available to this harvester
	 * @return A future for the response
	 */
	CompletableFuture<BasicMessageBean> onHarvestComplete(final DataBucketBean completed_bucket, final IHarvestContext context);
	
	/**
	 * Handles either a new bucket associated with this harvester, or an existing bucket
	 * that was previously associated with a different harvester.
	 * 
	 * @param test_bucket - the bucket to test
	 * @param test_spec - a specification describing the test related overrides
	 * @param context - the context available to this harvester
	 * @return A future for the response (only completes when the test is complete)
	 */
	CompletableFuture<BasicMessageBean> onTestSource(final DataBucketBean test_bucket, final ProcessingTestSpecBean test_spec, final IHarvestContext context);		
}
