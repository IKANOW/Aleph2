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

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
	public static Map<Tuple3<String, String, MasterEnrichmentType>, DataBucketBean> splitAnalyticBuckets(final DataBucketBean bucket, final Optional<DataBucketBean> old_bucket) {
		
		final HashSet<Tuple3<String, String, MasterEnrichmentType>> mutable_tech_set = new HashSet<>();
		
		final Map<Tuple3<String, String, MasterEnrichmentType>, List<AnalyticThreadJobBean>> res1 = 
			Optionals.of(() -> bucket.analytic_thread().jobs()).orElse(Collections.emptyList())
						.stream()
						.peek(job -> mutable_tech_set.add(Tuples._3T(job.analytic_technology_name_or_id(), job.entry_point(), job.analytic_type()))) //INTERNAL SIDE EFFECT!
						.collect(Collectors
								.
								groupingBy(
									job -> Tuples._3T(job.analytic_technology_name_or_id(), job.entry_point(), job.analytic_type())
									,
									() -> new LinkedHashMap<Tuple3<String, String, MasterEnrichmentType>, List<AnalyticThreadJobBean>>()
									,
									Collectors.<AnalyticThreadJobBean>toList()
								))
								;
		
		// If there's an update bucket we're going to look for analytic techs that aren't present in the new bucket so we can send "empty" messages to the DIM
		
		final Map<Tuple3<String, String, MasterEnrichmentType>, List<AnalyticThreadJobBean>> res2 = old_bucket.map(old -> {
			return Optionals.of(() -> old.analytic_thread().jobs()).orElse(Collections.emptyList())
								.stream()
								.filter(job -> !mutable_tech_set.contains(Tuples._3T(job.analytic_technology_name_or_id(), job.entry_point(), job.analytic_type()))) // (ie only things that no longer appear)
								.collect(Collectors
										.<AnalyticThreadJobBean, Tuple3<String, String, MasterEnrichmentType>, List<AnalyticThreadJobBean>>
										toMap((AnalyticThreadJobBean job) -> Tuples._3T(job.analytic_technology_name_or_id(), job.entry_point(), job.analytic_type())
												,
												job -> Collections.emptyList()
												,
												(acc1, acc2) -> Collections.emptyList()
												))
										;
		})
		.orElse(Collections.emptyMap());
		
		return Stream.concat(res1.entrySet().stream(), res2.entrySet().stream())
					.collect(Collectors
						.
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
							,
							(a, b) -> a, // (can't occur by construction)
							() -> new LinkedHashMap<Tuple3<String, String, MasterEnrichmentType>, DataBucketBean>()
					));
	}
	
}

