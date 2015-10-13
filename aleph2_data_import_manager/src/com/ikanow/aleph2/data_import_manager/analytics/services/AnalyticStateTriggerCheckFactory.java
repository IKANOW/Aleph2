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
package com.ikanow.aleph2.data_import_manager.analytics.services;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean.AnalyticThreadComplexTriggerBean.TriggerType;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerStateBean;

import scala.Tuple2;

/** Factory class for generating
 * @author Alex
 */
public class AnalyticStateTriggerCheckFactory {

	/** Interface for performing the trigger check
	 * @author Alex
	 */
	public static interface AnalyticStateChecker {
		CompletableFuture<Tuple2<Boolean, Long>> check(final DataBucketBean bucket, final Optional<AnalyticThreadJobBean> job, final AnalyticTriggerStateBean trigger);
	}
	
	public AnalyticStateChecker getChecker(final TriggerType trigger_type) {
		//TODO (ALEPH-12): return various analytic triggers
		return null;
	}
}
