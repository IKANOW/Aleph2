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
package com.ikanow.aleph2.data_model.interfaces.data_import;

import java.util.concurrent.Future;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.ProcessingTestSpecBean;

/** Harvesters are responsible from taking objects in arbitrary formats over arbitrary transport protocols,
 *  and doing one of the following things:
 *   - 
 * @author acp
 *
 */
public interface IHarvestTechnologyModule {

	/** This function should check the local environment, decide whether the technology module can run on this node
	 *  (eg is the external software installed? are the environment variables set up correctly etc) and return 
	 *  true/false as quickly as possibly. (Will often default to just 'return true;') 
	 * @param bucket - the bucket to check against (mostly this will be ignored, ie the function will just decide based on the technology module alone - but this enables the code to be cleverer, eg check the sub-modules as well)
	 * @return true if this node can run this module's functionality
	 */
	boolean canRunOnThisNode(@NonNull DataBucketBean bucket);
	
	/**
	 * Handles either a new bucket associated with this harvester, or an existing bucket
	 * that was previously associated with a different harvester.
	 * 
	 * @param new_bucket - a new bucket associated with this harvester
	 * @param context - the context available to this harvester
	 * @return A future for the response
	 */
	Future<BasicMessageBean> onNewSource(@NonNull DataBucketBean new_bucket, @NonNull IHarvestContext context);
	
	/**
	 * Handles changes to an existing bucket
	 * 
	 * @param olducket - the updated bucket
	 * @param newBucket - the updated bucket
	 * @param context - the context available to this harvester
	 * @return A future for the response
	 */
	Future<BasicMessageBean> onUpdatedSource(@NonNull DataBucketBean old_bucket, @NonNull DataBucketBean new_bucket, @NonNull IHarvestContext context);
	
	/**
	 * Instruction to suspend the bucket processing
	 * 
	 * @param suspended - the bucket that needs to be suspended
	 * @param context - the context available to this harvester
	 * @return A future for the response
	 */
	Future<BasicMessageBean> onSuspend(@NonNull DataBucketBean to_suspend, @NonNull IHarvestContext context);
	
	/**
	 * Instruction to re-activate a previously suspended bucket
	 * 
	 * @param to_resume - the bucket that needs to be re-activated
	 * @param context - the context available to this harvester
	 * @return A future for the response
	 */
	Future<BasicMessageBean> onResume(@NonNull DataBucketBean to_resume, @NonNull IHarvestContext context);
	
	/**
	 * Notification that all data for this bucket is to be purged
	 * Note that the actual purging is performed by the framework, so this is in 
	 * case the state needs to be updated etc.
	 * 
	 * @param to_purge - the bucket that is going to be purged
	 * @param context - the context available to this harvester
	 * @return A future for the response
	 */
	Future<BasicMessageBean> onPurge(@NonNull DataBucketBean to_purge, @NonNull IHarvestContext context);
	
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
	Future<BasicMessageBean> onDelete(@NonNull DataBucketBean to_delete, @NonNull IHarvestContext context);
	
	/**
	 * Periodic poll for statistics collection, health checks, etc.
	 * The poll frequency is determined by the bucket
	 * @param polled_bucket The bucket being polled
	 * @param context - the context available to this harvester
	 * @return A future for the response
	 */
	Future<BasicMessageBean> onPeriodicPoll(@NonNull DataBucketBean polled_bucket, @NonNull IHarvestContext context);
	
	/**
	 * For batch type harvest technologies (eg file not streaming), this callback is called when a batch is complete
	 * @param completed_bucket The bucket whose batch has completed
	 * @param context - the context available to this harvester
	 * @return A future for the response
	 */
	Future<BasicMessageBean> onHarvestComplete(@NonNull DataBucketBean completed_bucket, @NonNull IHarvestContext context);
	
	/**
	 * Handles either a new bucket associated with this harvester, or an existing bucket
	 * that was previously associated with a different harvester.
	 * 
	 * @param test_bucket - the bucket to test
	 * @param test_spec - a specification describing the test related overrides
	 * @param context - the context available to this harvester
	 * @return A future for the response (only completes when the test is complete)
	 */
	Future<BasicMessageBean> onTestSource(@NonNull DataBucketBean test_bucket, @NonNull ProcessingTestSpecBean test_spec, IHarvestContext context);		
}
