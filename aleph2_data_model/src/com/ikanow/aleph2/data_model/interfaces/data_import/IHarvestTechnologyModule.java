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
import com.ikanow.aleph2.data_model.objects.data_import.HarvestResponseBean;

/** Harvesters are responsible from taking objects in arbitrary formats over arbitrary transport protocols,
 *  and doing one of the following things:
 *   - 
 * @author acp
 *
 */
public interface IHarvestTechnologyModule {

	/**
	 * Handles either a new bucket associated with this harvester, or an existing bucket
	 * that was previously associated with a different harvester.
	 * 
	 * @param newBucket - a new bucket associated with this harvester
	 * @param context - the context available to this harvester
	 * @return A future for the response
	 */
	Future<HarvestResponseBean> onNewSource(@NonNull DataBucketBean new_bucket, @NonNull IHarvestContext context);
	
	/**
	 * Handles changes to an existing bucket
	 * 
	 * @param olducket - the updated bucket
	 * @param newBucket - the updated bucket
	 * @param context - the context available to this harvester
	 * @return A future for the response
	 */
	Future<HarvestResponseBean> onUpdatedSource(@NonNull DataBucketBean old_bucket, @NonNull DataBucketBean new_bucket, @NonNull IHarvestContext context);
	
	/**
	 * Instruction to suspend the bucket processing
	 * 
	 * @param suspended - the bucket that needs to be suspended
	 * @param context - the context available to this harvester
	 * @return A future for the response
	 */
	Future<HarvestResponseBean> onSuspend(@NonNull DataBucketBean to_suspend, @NonNull IHarvestContext context);
	
	/**
	 * Instruction to re-activate a previously suspended bucket
	 * 
	 * @param to_resume - the bucket that needs to be re-activated
	 * @param context - the context available to this harvester
	 * @return A future for the response
	 */
	Future<HarvestResponseBean> onResume(@NonNull DataBucketBean to_resume, @NonNull IHarvestContext context);
	
	/**
	 * Notification that all data for this bucket is to be purged
	 * Note that the actual purging is performed by the framework, so this is in 
	 * case the state needs to be updated etc.
	 * 
	 * @param to_resume - the bucket that is going to be purged
	 * @param context - the context available to this harvester
	 * @return A future for the response
	 */
	Future<HarvestResponseBean> onPurge(@NonNull DataBucketBean to_purge, @NonNull IHarvestContext context);
	
	/**
	 * Notification that this bucket is being deleted.
	 * The framework is responsible for removing all data associated with the bucket
	 * It is the harvester's responsibility to stop collecting/forwarding data (and not resolve
	 * the HarvestResponseBean until then).
	 * 
	 * @param to_resume - the bucket that needs to be re-activated
	 * @param context - the context available to this harvester
	 * @return A future for the response
	 */
	Future<HarvestResponseBean> onDelete(@NonNull DataBucketBean to_delete, @NonNull IHarvestContext context);
	
	/**
	 * Periodic poll for statistics collection, health checks, etc.
	 * The poll frequency is determined by the bucket
	 * @param polled_bucket The bucket being polled
	 * @param context - the context available to this harvester
	 * @return A future for the response
	 */
	Future<HarvestResponseBean> onPeriodicPoll(@NonNull DataBucketBean polled_bucket, @NonNull IHarvestContext context);
	
	/**
	 * For batch type harvest technologies (eg file not streaming), this callback is called when a batch is complete
	 * @param completed_bucket The bucket whose batch has completed
	 * @param context - the context available to this harvester
	 * @return A future for the response
	 */
	Future<HarvestResponseBean> onHarvestComplete(@NonNull DataBucketBean completed_bucket, @NonNull IHarvestContext context);
	
}
