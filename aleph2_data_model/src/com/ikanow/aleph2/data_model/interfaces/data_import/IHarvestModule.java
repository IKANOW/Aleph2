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
package com.ikanow.aleph2.data_model.interfaces.data_import;

import java.util.concurrent.Future;

import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.HarvestResponseBean;

/** Harvesters are responsible from taking objects in arbitrary formats over arbitrary transport protocols,
 *  and doing one of the following things:
 *   - 
 * @author acp
 *
 */
public interface IHarvestModule {

	/**
	 * Handles either a new bucket associated with this harvester, or an existing bucket
	 * that was previously associated with a different harvester.
	 * 
	 * @param newBucket - a new bucket associated with this harvester
	 * @param context - the context available to this harvester
	 * @return A future for the response
	 */
	Future<HarvestResponseBean> onNewSource(DataBucketBean new_bucket, IHarvestContext context);
	
	/**
	 * Handles changes to an existing bucket
	 * 
	 * @param olducket - the updated bucket
	 * @param newBucket - the updated bucket
	 * @param context - the context available to this harvester
	 * @return A future for the response
	 */
	Future<HarvestResponseBean> onUpdatedSource(DataBucketBean old_bucket, DataBucketBean new_bucket, IHarvestContext context);
	
	/**
	 * Instruction to suspend the bucket processing
	 * 
	 * @param suspended - the bucket that needs to be suspended
	 * @return A future for the response
	 */
	Future<HarvestResponseBean> onSuspend(DataBucketBean to_suspend, IHarvestContext context);
	
	/**
	 * Instruction to re-activate a previously suspended bucket
	 * 
	 * @param to_resume - the bucket that needs to be re-activated
	 * @return A future for the response
	 */
	Future<HarvestResponseBean> onResume(DataBucketBean to_resume, IHarvestContext context);
	
	/**
	 * Notification that all data for this bucket is to be purged
	 * Note that the actual purging is performed by the framework, so this is in 
	 * case the state needs to be updated etc.
	 * 
	 * @param to_resume - the bucket that is going to be purged
	 * @return A future for the response
	 */
	Future<HarvestResponseBean> onPurge(DataBucketBean to_purge, IHarvestContext context);
	
	/**
	 * Notification that this bucket is being deleted.
	 * The framework is responsible for removing all data associated with the bucket
	 * It is the harvester's responsibility to stop collecting/forwarding data (and not resolve
	 * the HarvestResponseBean until then).
	 * 
	 * @param to_resume - the bucket that needs to be re-activated
	 * @return A future for the response
	 */
	Future<HarvestResponseBean> onDelete(DataBucketBean to_delete, IHarvestContext context);
}
