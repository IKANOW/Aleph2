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
package com.ikanow.aleph2.analytics.utils;

import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.commons.lang.time.DateUtils;

import scala.Tuple2;
import scala.Tuple3;

import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean.AnalyticThreadJobInputConfigBean;
import com.ikanow.aleph2.data_model.utils.Patterns;
import com.ikanow.aleph2.data_model.utils.TimeUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;

/** A few utils to handle time sliced dirs
 * @author alex
 */
public class TimeSliceDirUtils {

	/** Given a pair of (optional) human readable strings (which are assumed to refer to the past)
	 *  returns a pair of optional dates
	 * @param input_config
	 * @return
	 */
	public static Tuple2<Optional<Date>, Optional<Date>> getQueryTimeRange(final AnalyticThreadJobInputConfigBean input_config, final Date now)
	{		
		Function<Optional<String>, Optional<Date>> parseDate = 
				maybe_date -> maybe_date
								.map(datestr -> TimeUtils.getSchedule(datestr, Optional.of(now)))
								.filter(res -> res.isSuccess())
								.map(res -> res.success())
								// OK so this wants to be backwards in time always...
								.map(date -> {
									if (date.getTime() > now.getTime()) {
										final long diff = date.getTime() - now.getTime();
										return Date.from(now.toInstant().minusMillis(diff));
									}
									else return date;
								})
								;											

		final Optional<Date> tmin = parseDate.apply(Optional.ofNullable(input_config.time_min()));										
		final Optional<Date> tmax = parseDate.apply(Optional.ofNullable(input_config.time_max()));
		
		return Tuples._2T(tmin, tmax);
	}
	
	
	/** Takes a stream of strings (paths) and adds the date range
	 * @param dir_listing
	 * @return
	 */
	public static Stream<Tuple3<String, Date, Date>> annotateTimedDirectories(final Stream<String> dir_listing) {
		//(first get to last element in path, then if it ends _<date> then grab <date>, else assume it's the whole thing)
		return dir_listing
			.map(dir -> dir.endsWith("/") ? dir.substring(0, dir.length() - 1) : dir)
			.map(dir -> Tuples._2T(dir, dir.lastIndexOf("/"))) // if not present, returns -1 which means the substring below grabs the entire thing
			.map(dir_date -> Tuples._2T(dir_date._1(), dir_date._1().substring(1 + dir_date._2())))
			.map(dir_date -> Tuples._3T(dir_date._1(), dir_date._2(), dir_date._2().lastIndexOf("_"))) // if not present, returns -1 which means the substring below grabs the entire thing
			.map(dir_date_index -> Tuples._2T(dir_date_index._1(), dir_date_index._2().substring(1 + dir_date_index._3())))
			.map(dir_date -> Tuples._3T(dir_date._1(), dir_date._2(), TimeUtils.getDateFromSuffix(dir_date._2())))
			.filter(dir_datestr_date -> dir_datestr_date._3().isSuccess())
			.map(dir_datestr_date -> {
				final Optional<Tuple2<String, ChronoUnit>> info = TimeUtils.getFormatInfoFromDateString(dir_datestr_date._2());
				return Tuples._3T(dir_datestr_date._1(), info, dir_datestr_date._3());
			})
			.filter(dir_datestr_date -> dir_datestr_date._2().isPresent())
			.map(dir_datestr_date -> 
					Tuples._3T(
							dir_datestr_date._1(), 
							dir_datestr_date._3().success(), 
							adjustTime(Date.from(dir_datestr_date._3().success().toInstant()), dir_datestr_date._2().get()._2())
							)
			)
			;
	}
	
	/** Filters out non matching directories
	 * @param in
	 * @param filter
	 * @return
	 */
	public static Stream<String> filterTimedDirectories(Stream<Tuple3<String, Date, Date>> in, Tuple2<Optional<Date>, Optional<Date>> filter) {
		
		return in.filter(t3 -> filter._1()
									.map(tmin -> { //lower bound
										return tmin.getTime() < t3._3().getTime(); // just has to be smaller than the largest time in the group
									}).orElse(true))
					.filter(t3 -> filter._2()
									.map(tmax -> { //lower bound
										return tmax.getTime() >= t3._2().getTime(); // just has to be larger than the smallest time in the group
									}).orElse(true))
					.map(t3 -> t3._1())
					;
	}
	
	/** Low level util because java8 time "plus" is odd
	 * @param to_adjust
	 * @param increment
	 * @return
	 */
	private static Date adjustTime(Date to_adjust, ChronoUnit increment) {
		return Patterns.match(increment).<Date>andReturn()
				.when(t -> t == ChronoUnit.SECONDS, __ -> DateUtils.addSeconds(to_adjust, 1))
				.when(t -> t == ChronoUnit.MINUTES, __ -> DateUtils.addMinutes(to_adjust, 1))
				.when(t -> t == ChronoUnit.HOURS, __ -> DateUtils.addHours(to_adjust, 1))
				.when(t -> t == ChronoUnit.DAYS, __ -> DateUtils.addDays(to_adjust, 1))
				.when(t -> t == ChronoUnit.WEEKS, __ -> DateUtils.addWeeks(to_adjust, 1))
				.when(t -> t == ChronoUnit.MONTHS, __ -> DateUtils.addMonths(to_adjust, 1))
				.when(t -> t == ChronoUnit.YEARS, __ -> DateUtils.addYears(to_adjust, 1))
				.otherwiseAssert()
				;
	}
}
