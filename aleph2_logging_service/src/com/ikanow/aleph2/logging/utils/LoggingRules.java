/*******************************************************************************
 * Copyright 2016, The IKANOW Open Source Project.
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
package com.ikanow.aleph2.logging.utils;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import scala.Tuple2;

import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;

/**
 * @author Burch
 *
 */
public class LoggingRules {
	/**
	 * Rule that returns true if current count of log messages is divisible by count.  Every log message that passes the Level
	 * filter increments the count (including log messages with different rules).
	 * @param count
	 * @return
	 */
	public static Function<Tuple2<BasicMessageBean, Map<String,Object>>, Boolean> logEveryCount(final long count) {
		return (t)->{
			final Long c = Optional.ofNullable((long)t._2.get(LoggingUtils.LOG_COUNT_FIELD)).orElse(0L) + 1L; //add 1 to the count	 
			return (c % count) == 0;
		};
	}
	
	/**
	 * Rule that returns true if current time minus last log time is greater than milliseconds.  Every log message that is output sets the
	 * latest timestamp (including log messages with different rules).
	 * @param milliseconds
	 * @return
	 */
	public static Function<Tuple2<BasicMessageBean, Map<String,Object>>, Boolean> logEveryMilliseconds(final long milliseconds) {
		return (t)->{
			final Long ms = Optional.ofNullable((long)t._2.get(LoggingUtils.LAST_LOG_TIMESTAMP_FIELD)).orElse(0L); 
			return (System.currentTimeMillis() - ms) > milliseconds;
		};
	}
	
	/**
	 * Rule that returns true if double in BMB.details().value_field is greater than max_threshold or lower than min_threshold
	 * @param value_field
	 * @param min_threshold
	 * @param max_threshold
	 * @return
	 */
	public static Function<Tuple2<BasicMessageBean, Map<String, Object>>, Boolean> logOutsideThreshold(final String value_field,
			final Optional<Double> min_threshold, final Optional<Double> max_threshold) {
		return (t)->{
			final Double val = Optional.ofNullable(LoggingUtils.getDetailsMapValue(t._1, value_field, Double.class)).orElse(0.0D);
			
			return min_threshold.map(m -> val < m).orElse(false) || 
					max_threshold.map(m -> val > m).orElse(false);
		};
	}
}
