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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean.MasterEnrichmentType;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.Tuples;

import scala.Tuple3;

/** Utility code for handling analytic buckets
 * @author Alex
 */
public class AnalyticActorUtils {

	/** Splits a bucket into a set of buckets with the jobs grouped by entry point/analytic thread id
	 *  Which we'll then use to generate a set of messages
	 * @param bucket
	 * @return
	 */
	public static Map<Tuple3<String, String, MasterEnrichmentType>, DataBucketBean> splitAnalyticBuckets(final DataBucketBean bucket) {
		Map<Tuple3<String, String, MasterEnrichmentType>, List<AnalyticThreadJobBean>> res1 = 
			Optionals.of(() -> bucket.analytic_thread().jobs()).orElse(Collections.emptyList())
						.stream()
						.collect(Collectors.groupingBy(job -> 
							Tuples._3T(job.analytic_technology_name_or_id(), job.entry_point(), job.analytic_type())));
		
		return res1.entrySet()
					.stream()
					.collect(Collectors
						.<Entry<Tuple3<String, String, MasterEnrichmentType>, 
							List<AnalyticThreadJobBean>>, Tuple3<String, String, MasterEnrichmentType>, DataBucketBean>
						toMap(
							kv -> kv.getKey()
							,
							kv -> BeanTemplateUtils.clone(bucket)
									.with(DataBucketBean::analytic_thread, 
										BeanTemplateUtils.clone(bucket.analytic_thread())
											.with(AnalyticThreadBean::jobs, kv.getValue())
										.done()
									)
									.done() 
					));
	}
	
}

