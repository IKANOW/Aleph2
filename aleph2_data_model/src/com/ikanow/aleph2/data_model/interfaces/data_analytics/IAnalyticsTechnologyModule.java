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
 ******************************************************************************/
package com.ikanow.aleph2.data_model.interfaces.data_analytics;

import java.util.concurrent.CompletableFuture;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.ProcessingTestSpecBean;

/** Defines the interface of a module capable of performing batch or streaming processing via internal or external functions 
 * @author acp
 */
public interface IAnalyticsTechnologyModule {

	/** This function should check the local environment, decide whether the technology module can run on this node
	 *  (eg is the external software installed? are the environment variables set up correctly etc) and return 
	 *  true/false as quickly as possibly. (Will often default to just 'return true;') 
	 * @param thread - the analytic thread to check against (mostly this will be ignored, ie the function will just decide based on the technology module alone - but this enables the code to be cleverer, eg check the sub-modules as well)
	 * @return true if this node can run this module's functionality
	 */
	boolean canRunOnThisNode(final @NonNull AnalyticThreadBean thread);
	
	/**
	 * Handles a new thread being created - note this notification does not mean that the thread should be started
	 * That is handle via the onStartThread call. This just enables any preparatory work to occur, if applicable.
	 * 
	 * @param new_thread - a new analytic thread
	 * @param context - the context available to this thread
	 * @return A future for the response
	 */
	@NonNull
	CompletableFuture<BasicMessageBean> onNewThread(final @NonNull AnalyticThreadBean new_thread, final @NonNull IAnalyticsContext context);
	
	/**
	 * Handles changes to an existing analytic thread - note this notification does not mean that the thread should be started
	 * That is handle via the onStartThread call. This just enables any internal state to be updated, if required
	 * 
	 * @param olducket - the updated analytic thread
	 * @param newBucket - the updated analytic thread
	 * @param context - the context available to this thread
	 * @return A future for the response
	 */
	@NonNull
	CompletableFuture<BasicMessageBean> onUpdatedThread(final @NonNull AnalyticThreadBean old_bucket, final @NonNull AnalyticThreadBean new_bucket, final @NonNull IAnalyticsContext context);
	
	/**
	 * Instruction to start a thread (including re-starting a suspended thread). Should return success==false and code==EAGAIN if already started
	 * 
	 * @param to_start - the analytic thread that has been scheduled to run.
	 * @param context - the context available to this analytic thread
	 * @return A future for the response
	 */
	@NonNull
	CompletableFuture<BasicMessageBean> onStart(final @NonNull AnalyticThreadBean to_start, final @NonNull IAnalyticsContext context);
	
	/**
	 * Instruction to suspend the analytic thread processing
	 * 
	 * @param suspended - the analytic thread that needs to be suspended
	 * @param context - the context available to this analytic thread
	 * @return A future for the response
	 */
	@NonNull
	CompletableFuture<BasicMessageBean> onSuspend(final @NonNull AnalyticThreadBean to_suspend, final @NonNull IAnalyticsContext context);
	
	/**
	 * Notification that all data for a bucket associated with this thread is to be purged
	 * Note that the actual purging is performed by the framework, so this is in 
	 * case the state needs to be updated etc.
	 * 
	 * @param related_thread - the thread that writes to the bucket being purged
	 * @param bucket_being_purged - the bucket that is being purged
	 * @param context - the context available to this analytic thread
	 * @return A future for the response
	 */
	@NonNull
	CompletableFuture<BasicMessageBean> onPurge(final @NonNull AnalyticThreadBean related_thread, final @NonNull DataBucketBean bucket_being_purged, final @NonNull IAnalyticsContext context);
	
	/**
	 * Notification that this analytic thread is being deleted.
	 * The framework is responsible for removing all data associated with the bucket
	 * It is the thread's responsibility to stop processing data (and not resolve
	 * the BasicMessageBean until then).
	 * 
	 * @param to_delete - the thred that is being deleted
	 * @param context - the context available to this analytic thread
	 * @return A future for the response
	 */
	@NonNull
	CompletableFuture<BasicMessageBean> onDelete(final @NonNull AnalyticThreadBean to_delete, final @NonNull IAnalyticsContext context);
	
	/**
	 * Periodic poll for statistics collection, health checks, etc.
	 * The poll frequency is determined by the bucket
	 * @param polled_thread The analytic thread being polled
	 * @param context - the context available to this analytic thread
	 * @return A future for the response
	 */
	@NonNull
	CompletableFuture<BasicMessageBean> onPeriodicPoll(final @NonNull AnalyticThreadBean polled_thread, final @NonNull IAnalyticsContext context);
	
	/**
	 * For batch type harvest technologies (eg file not streaming), this callback is called when a batch is complete
	 * @param completed_bucket The bucket whose batch has completed
	 * @param context - the context available to this harvester
	 * @return A future for the response
	 */
	@NonNull
	CompletableFuture<BasicMessageBean> onThreadComplete(final @NonNull AnalyticThreadBean completed_bucket, final @NonNull IAnalyticsContext context);
	
	/**
	 * Handles either a new bucket associated with this harvester, or an existing bucket
	 * that was previously associated with a different harvester.
	 * 
	 * @param test_bucket - the bucket to test
	 * @param context - the context available to this harvester
	 * @return A future for the response (only completes when the test is complete)
	 */
	@NonNull
	CompletableFuture<BasicMessageBean> onTestThread(final @NonNull AnalyticThreadBean test_bucket, final @NonNull ProcessingTestSpecBean test_spec, final @NonNull IAnalyticsContext context);		
}
