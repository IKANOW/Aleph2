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
package com.ikanow.aleph2.data_model.interfaces.data_analytics;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean.AnalyticThreadComplexTriggerBean;
import com.ikanow.aleph2.data_model.objects.data_import.BucketDiffBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.ProcessingTestSpecBean;
import com.ikanow.aleph2.data_model.utils.FutureUtils.ManagementFuture;

/** Defines the interface of a module capable of performing batch or streaming processing via internal or external functions 
 * @author acp
 */
public interface IAnalyticsTechnologyModule {

	//////////////////////////////////////////////////////////
	
	// BASIC INITIALIZATION	
	
	/** This function is guaranteed to always be called before any callback is called, and can therefore be used to store the context
	 *  It can be used to store the context (which is always passed in anyway) and therefore set up any code that needs to be run.
	 *  It should return as quickly as possible, eg not block on any lengthy operations.
	 * @param context - the context for which an instance of this technology has been created
	 */
	void onInit(final IAnalyticsContext context);
	
	/** This function should check the local environment, decide whether the technology module can run on this node
	 *  (eg is the external software installed? are the environment variables set up correctly etc) and return 
	 *  true/false as quickly as possibly. (Will often default to just 'return true;')
	 *  This method is called for each analytic technology that is part of a thread
	 *   
	 * @param analytic_bucket - the bucket containing the analytic thread to check against (mostly this will be ignored, ie the function will just decide based on the technology module alone - but this enables the code to be cleverer, eg check the sub-modules as well)
	 * @param jobs - the list of jobs from the analytic thread that this technology actually handles (mostly this will be ignored, ie the function will just decide based on the technology module alone - but this enables the code to be cleverer, eg check the sub-modules as well)
	 * @return true if this node can run this module's functionality
	 */
	boolean canRunOnThisNode(final DataBucketBean analytic_bucket, final Collection<AnalyticThreadJobBean> jobs, final IAnalyticsContext context);
	
	/** Whether this engine can be run in multi-node (defaults to false)
	 * @param analytic_bucket - the bucket containing the analytic thread to check against (mostly this will be ignored, ie the function will just decide based on the technology module alone - but this enables the code to be cleverer, eg check the sub-modules as well)
	 * @param jobs - the list of jobs from the analytic thread that this technology actually handles (mostly this will be ignored, ie the function will just decide based on the technology module alone - but this enables the code to be cleverer, eg check the sub-modules as well)
	 * @return true if this node can run this module's functionality
	 * @return whether this engine can be run in multi-node (defaults to false)
	 */
	default boolean supportsMultiNode(final DataBucketBean analytic_bucket, final Collection<AnalyticThreadJobBean> jobs, final IAnalyticsContext context) {
		return false;
	}
	
	//////////////////////////////////////////////////////////
	
	// THREAD-WIDE MAJOR OPERATIONS		
	
	/** Handles a new thread being created - Note this notification does not mean that the thread should be started, 
	 *  (EXCEPT optionally for stream jobs when the bucket is active)
	 *  ie it can usually just be "stubbed out" (but can be used for any required administrative actions/updating internal state etc)
	 *  (That is handled via the a set of calls to startAnalyticJob (and notified via onThreadExecute))
	 *  This method is called for each analytic technology that is part of a thread
	 * 
	 * @param new_analytic_bucket - the bucket containing the new analytic thread
	 * @param jobs - the list of jobs from the analytic thread that this technology actually handles (mostly this will be ignored, ie the function will just decide based on the technology module alone - but this enables the code to be cleverer, eg check the sub-modules as well)
	 * @param context - the context available to this thread
	 * @param enabled - whether the bucket is enabled
	 * @return A future for the response
	 */
	CompletableFuture<BasicMessageBean> onNewThread(final DataBucketBean new_analytic_bucket, final Collection<AnalyticThreadJobBean> jobs, final IAnalyticsContext context, final boolean enabled);
	
	/** Handles changes to an existing analytic thread - note this notification does not mean that the thread should be started,
	 *  ie it can usually just be "stubbed out" (but can be used for any required administrative actions/updating internal state etc)
	 *  (That is handled via the set of calls to startAnalyticJob (and notified via onThreadExecute))
	 *  This method is called for each analytic technology that is part of a thread
	 *  If the thread is being suspended as part of the update, this callback has no responsibility for stopping anything, that will be handled by separate calls to suspendAnalyticJob
	 *  (EXCEPT optionally for stream jobs when the bucket is active)
	 * 
	 * @param old_analytic_bucket - the old bucket containing the analytic thread
	 * @param new_analytic_bucket - the new bucket containing the analytic thread
	 * @param jobs - the list of jobs from the analytic thread that this technology actually handles (mostly this will be ignored, ie the function will just decide based on the technology module alone - but this enables the code to be cleverer, eg check the sub-modules as well)
	 * @param is_enabled - whether the bucket is now enabled (note - you cannot infer anything about the previous enabled/suspended state unless the diff optional is present)
	 * @param diff - optionally what information has changed (including changes to shared library beans). If this is not present (it will not be in early versions of the platform), developers must assume everything has changed, eg restarting the external service. 
	 * @param context - the context available to this thread
	 * @return A future for the response
	 */
	CompletableFuture<BasicMessageBean> onUpdatedThread(final DataBucketBean old_analytic_bucket, final DataBucketBean new_analytic_bucket, final Collection<AnalyticThreadJobBean> jobs, final boolean is_enabled, final Optional<BucketDiffBean> diff, final IAnalyticsContext context);

	/**
	 * Notification that the bucket containing this analytic thread is being deleted.
	 * The framework is responsible for removing all data associated with the bucket
	 * The framework will also call stopAnalyticJob for each active job, so this callback does not need to stop anything
	 * It is responsible for cleaning up any external state it might have generated
	 *  (EXCEPT optionally for stream jobs, depending on the implementation)
	 * 
	 * @param to_delete_analytic_bucket - the data bucket (containing this analytic thread) to be deleted
	 * @param jobs - the list of jobs from the analytic thread that this technology actually handles (mostly this will be ignored, ie the function will just decide based on the technology module alone - but this enables the code to be cleverer, eg check the sub-modules as well)
	 * @param context - the context available to this analytic thread
	 * @return A future for the response
	 */
	CompletableFuture<BasicMessageBean> onDeleteThread(final DataBucketBean to_delete_analytic_bucket, final Collection<AnalyticThreadJobBean> jobs, final IAnalyticsContext context);	
	
	//////////////////////////////////////////////////////////
	
	// THREAD-WIDE MINOR OPERATIONS		
	
	/** 
	 * Users can set up custom triggers that are satisfied by arbitrary logic in the technology code (using the trigger fields for customization)
	 * This callback is called by the system at regular intervals
	 * 
	 * @param analytic_bucket - the bucket containing the analytic thread whose triggers are being examined
	 * @param trigger - the specific trigger configuration to check against
	 * @param context - the context available to this analytic thread
	 * @return a management future - true or false to fire the trigger, the side channel can be used to log errors
	 */
	ManagementFuture<Boolean> checkCustomTrigger(final DataBucketBean analytic_bucket, final AnalyticThreadComplexTriggerBean trigger, final IAnalyticsContext context);
	
	/**
	 * Invoked when an analytic bucket is triggered. This callback should not start any jobs, that is handled by startAnalyticJob, so will not normally do much.
	 * (note the individual jobs will only execute if this callback returns a message bean with success==true)
	 * @param new_analytic_bucket - the bucket containing the analytic thread
	 * @param jobs - the jobs within the analytic thread that are handled by this analytic technology
	 * @param matching_triggers - a list of matching triggers (empty if it's a streaming job or a scheduled start)
	 * @param context - the context available to this analytic thread
	 * @return A future for the response - note the individual jobs will only execute if this returns success==true
	 */
	CompletableFuture<BasicMessageBean> onThreadExecute(final DataBucketBean new_analytic_bucket, final Collection<AnalyticThreadJobBean> jobs, final Collection<AnalyticThreadComplexTriggerBean> matching_triggers, final IAnalyticsContext context); 
	
	/**
	 * For batch type harvest technologies (eg file not streaming), this callback is called when a batch is complete. Normally no action is required from this callback
	 * @param completed_analytic_bucket The bucket whose batch has completed
	 * @param jobs - the jobs within the analytic thread that are handled by this analytic technology
	 * @param context - the context available to this analytic thread
	 * @return A future for the response
	 */
	CompletableFuture<BasicMessageBean> onThreadComplete(final DataBucketBean completed_analytic_bucket, final Collection<AnalyticThreadJobBean> jobs, final IAnalyticsContext context);
	
	/** Notification that all data for a bucket associated with this thread is to be purged
	 * Note that the actual purging is performed by the framework, so this is in 
	 * case the state needs to be updated etc.
	 * 
	 * @param purged_analytic_bucket - the bucket that is being purged
	 * @param jobs - the jobs within the analytic thread that are handled by this analytic technology
	 * @param context - the context available to this analytic thread
	 * @return A future for the response
	 */
	CompletableFuture<BasicMessageBean> onPurge(final DataBucketBean purged_analytic_bucket, final Collection<AnalyticThreadJobBean> jobs, final IAnalyticsContext context);
	
	/**
	 * Periodic poll for statistics collection, health checks, etc.
	 * The poll frequency is determined by the bucket
	 * @param polled_analytic_bucket The bucket containing the analytic thread being polled
	 * @param jobs - the jobs within the analytic thread that are handled by this analytic technology
	 * @param context - the context available to this analytic thread
	 * @return A future for the response
	 */
	CompletableFuture<BasicMessageBean> onPeriodicPoll(final DataBucketBean polled_analytic_bucket, final Collection<AnalyticThreadJobBean> jobs, final IAnalyticsContext context);
	
	/**
	 * Notifies this analytic thread that a job is about to start a test. The actual tests are invoked by startAnalyticJobTest, so this is just for setting up any persistent state etc, ie normally no action is required.
	 * 
	 * @param test_bucket - the bucket to test
	 * @param jobs - the jobs within the analytic thread that are handled by this analytic technology
	 * @param context - the context available to this analytic thread
	 * @return A future for the response (only completes when the test is complete)
	 */
	CompletableFuture<BasicMessageBean> onTestThread(final DataBucketBean test_bucket, final Collection<AnalyticThreadJobBean> jobs, final ProcessingTestSpecBean test_spec, final IAnalyticsContext context);			
	
	//////////////////////////////////////////////////////////
	
	// JOB-SPECIFIC OPERATIONS		
	
	/**
	 * Responsible for actually starting the specified analytic job
	 * @param analytic_bucket - the bucket containing the analytic thread that this job is part of
	 * @param jobs - a list of all jobs handled by this thread
	 * @param job_to_start - the specific job
	 * @param context - the context available to this analytic thread
	 * @return A future for the response
	 */
	CompletableFuture<BasicMessageBean> startAnalyticJob(final DataBucketBean analytic_bucket, final Collection<AnalyticThreadJobBean> jobs, final AnalyticThreadJobBean job_to_start, final IAnalyticsContext context);

	/**
	 * Responsible for actually stopping the specified analytic job (eg if the job is deleted)
	 * @param analytic_bucket - the bucket containing the analytic thread that this job is part of
	 * @param jobs - a list of all jobs handled by this thread
	 * @param job_to_stop - the specific job
	 * @param context - the context available to this analytic thread
	 * @return A future for the response
	 */
	CompletableFuture<BasicMessageBean> stopAnalyticJob(final DataBucketBean analytic_bucket, final Collection<AnalyticThreadJobBean> jobs, final AnalyticThreadJobBean job_to_stop, final IAnalyticsContext context);
	
	/**
	 * Responsible for re-activating the specified analytic job (which has previously been suspended)
	 * Normally suspend/resume will just call stop/start since most analytic platforms don't have explicit support for suspension/resumption
	 * @param analytic_bucket - the bucket containing the analytic thread that this job is part of
	 * @param jobs - a list of all jobs handled by this thread
	 * @param job_to_resume - the specific job
	 * @param context - the context available to this analytic thread
	 * @return A future for the response
	 */
	CompletableFuture<BasicMessageBean> resumeAnalyticJob(final DataBucketBean analytic_bucket, final Collection<AnalyticThreadJobBean> jobs, final AnalyticThreadJobBean job_to_resume, final IAnalyticsContext context);

	/**
	 * Responsible for suspending the specified analytic job
	 * Normally suspend/resume will just call stop/start since most analytic platforms don't have explicit support for suspension/resumption
	 * @param analytic_bucket - the bucket containing the analytic thread that this job is part of
	 * @param jobs - a list of all jobs handled by this thread
	 * @param job_to_suspend - the specific job
	 * @param context - the context available to this analytic thread
	 * @return A future for the response
	 */
	CompletableFuture<BasicMessageBean> suspendAnalyticJob(final DataBucketBean analytic_bucket, final Collection<AnalyticThreadJobBean> jobs, final AnalyticThreadJobBean job_to_suspend, final IAnalyticsContext context);
	
	/**
	 * Similar to startAnalyticJob, but with the knowledge this is only a test. It isn't the callback's responsibility to ensure the processing test spec
	 * is respected, though it is welcome to as an extra piece of safety
	 * @param analytic_bucket - the bucket containing the analytic thread that this job is part of
	 * @param jobs - a list of all jobs handled by this thread
	 * @param job_to_test - the specific job
	 * @param context - the context available to this analytic thread
	 * @return A future for the response
	 */
	CompletableFuture<BasicMessageBean> startAnalyticJobTest(final DataBucketBean analytic_bucket, final Collection<AnalyticThreadJobBean> jobs, final AnalyticThreadJobBean job_to_test, final ProcessingTestSpecBean test_spec, final IAnalyticsContext context);
	
	/**
	 * Checks the job's progress: return true if complete, false if still going (optionally, "in progress" information can be returned via the management side-channel)
	 * @param analytic_bucket - the bucket containing the analytic thread that this job is part of
	 * @param jobs - a list of all jobs handled by this thread
	 * @param job_to_check - the specific job
	 * @param context - the context available to this analytic thread
	 * @return A future for the response
	 */
	ManagementFuture<Boolean> checkAnalyticJobProgress(final DataBucketBean analytic_bucket, final Collection<AnalyticThreadJobBean> jobs, final AnalyticThreadJobBean job_to_check, final IAnalyticsContext context);
	
}
