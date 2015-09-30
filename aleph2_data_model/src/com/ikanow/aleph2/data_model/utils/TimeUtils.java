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
package com.ikanow.aleph2.data_model.utils;

import java.util.Date;
import java.util.Optional;
import java.text.ParseException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.time.DateUtils;

import com.joestelmach.natty.CalendarSource;
import com.joestelmach.natty.DateGroup;
import com.joestelmach.natty.Parser;

import fj.data.Validation;

/** A collection of temporal utilities
 * @author Alex
 */
public class TimeUtils {

	/** The simplest date parsing utility - only handles daily/hourly/monthly type strings (1d, d, daily, day - etc). Note "m" is ambiguous and not supported, use "min" or "month"
	 * @param human_readable_period - daily/hourly/monthly type strings (1d, d, daily, day - etc). Note "m" is ambiguous and not supported, use "min" or "month"
	 * @return a ChronoUnit if successful, else a generic error string
	 */
	public static Validation<String, ChronoUnit> getTimePeriod(final String human_readable_period) {		
		return Patterns.match(Optional.ofNullable(human_readable_period).orElse("").toLowerCase().replaceAll("\\s+", ""))
				.<Validation<String, ChronoUnit>>andReturn()
				.when(d -> d.equals("1d"), __ -> Validation.success(ChronoUnit.DAYS))
				.when(d -> d.equals("d"), __ -> Validation.success(ChronoUnit.DAYS))
				.when(d -> d.equals("1day"), __ -> Validation.success(ChronoUnit.DAYS))
				.when(d -> d.equals("day"), __ -> Validation.success(ChronoUnit.DAYS))
				.when(d -> d.equals("daily"), __ -> Validation.success(ChronoUnit.DAYS))
				
				.when(d -> d.equals("1w"), __ -> Validation.success(ChronoUnit.WEEKS))
				.when(d -> d.equals("w"), __ -> Validation.success(ChronoUnit.WEEKS))
				.when(d -> d.equals("1wk"), __ -> Validation.success(ChronoUnit.WEEKS))
				.when(d -> d.equals("wk"), __ -> Validation.success(ChronoUnit.WEEKS))
				.when(d -> d.equals("1week"), __ -> Validation.success(ChronoUnit.WEEKS))
				.when(d -> d.equals("week"), __ -> Validation.success(ChronoUnit.WEEKS))
				.when(d -> d.equals("weekly"), __ -> Validation.success(ChronoUnit.WEEKS))
				
				.when(d -> d.equals("1month"), __ -> Validation.success(ChronoUnit.MONTHS))
				.when(d -> d.equals("month"), __ -> Validation.success(ChronoUnit.MONTHS))
				.when(d -> d.equals("monthly"), __ -> Validation.success(ChronoUnit.MONTHS))

				.when(d -> d.equals("1sec"), __ -> Validation.success(ChronoUnit.SECONDS))
				.when(d -> d.equals("sec"), __ -> Validation.success(ChronoUnit.SECONDS))
				.when(d -> d.equals("1s"), __ -> Validation.success(ChronoUnit.SECONDS))
				.when(d -> d.equals("s"), __ -> Validation.success(ChronoUnit.SECONDS))
				.when(d -> d.equals("1second"), __ -> Validation.success(ChronoUnit.SECONDS))
				.when(d -> d.equals("second"), __ -> Validation.success(ChronoUnit.SECONDS))
				
				.when(d -> d.equals("1min"), __ -> Validation.success(ChronoUnit.MINUTES))
				.when(d -> d.equals("min"), __ -> Validation.success(ChronoUnit.MINUTES))
				.when(d -> d.equals("1minute"), __ -> Validation.success(ChronoUnit.MINUTES))
				.when(d -> d.equals("minute"), __ -> Validation.success(ChronoUnit.MINUTES))
				
				.when(d -> d.equals("1h"), __ -> Validation.success(ChronoUnit.HOURS))
				.when(d -> d.equals("h"), __ -> Validation.success(ChronoUnit.HOURS))
				.when(d -> d.equals("1hour"), __ -> Validation.success(ChronoUnit.HOURS))
				.when(d -> d.equals("hour"), __ -> Validation.success(ChronoUnit.HOURS))
				.when(d -> d.equals("hourly"), __ -> Validation.success(ChronoUnit.HOURS))
				
				.when(d -> d.equals("1y"), __ -> Validation.success(ChronoUnit.YEARS))
				.when(d -> d.equals("y"), __ -> Validation.success(ChronoUnit.YEARS))
				.when(d -> d.equals("1year"), __ -> Validation.success(ChronoUnit.YEARS))
				.when(d -> d.equals("year"), __ -> Validation.success(ChronoUnit.YEARS))
				.when(d -> d.equals("1yr"), __ -> Validation.success(ChronoUnit.YEARS))
				.when(d -> d.equals("yr"), __ -> Validation.success(ChronoUnit.YEARS))
				.when(d -> d.equals("yearly"), __ -> Validation.success(ChronoUnit.YEARS))				
				
				.otherwise(__ -> Validation.fail(ErrorUtils.get(ErrorUtils.INVALID_DATETIME_FORMAT, human_readable_period)));
	}
	
	/** Returns the suffix of a time-based index given the grouping period
	 * @param grouping_period - the grouping period
	 * @param lowest_granularity
	 * @return the index suffix, ie added to the base index
	 */
	public static String getTimeBasedSuffix(final ChronoUnit grouping_period, final Optional<ChronoUnit> lowest_granularity) {
		return lowest_granularity
				.map(lg -> grouping_period.compareTo(lg) < 0  ? getTimeBasedSuffix(lg, Optional.empty()) : null )
				.orElse(
					Patterns.match(grouping_period).<String>andReturn()
						.when(p -> ChronoUnit.SECONDS == p, __ -> "yyyy-MM-dd-HH:mm:ss") 
						.when(p -> ChronoUnit.MINUTES == p, __ -> "yyyy-MM-dd-HH:mm") 
						.when(p -> ChronoUnit.HOURS == p, __ -> "yyyy-MM-dd-HH")
						.when(p -> ChronoUnit.DAYS == p, __ -> "yyyy-MM-dd")
						.when(p -> ChronoUnit.WEEKS == p, __ -> "YYYY.ww") // (deliberately 'Y' (week-year) not 'y' since 'w' is week-of-year 
						.when(p -> ChronoUnit.MONTHS == p, __ -> "yyyy-MM")
						.when(p -> ChronoUnit.YEARS == p, __ -> "yyyy")
						.otherwise(__ -> "")
					);
	}
	
	//TODO: need the opposite of the time-based suffix

	public static String[] SUPPORTED_DATE_SUFFIXES = {
		"yyyy-MM-dd-HH:mm:ss",
		"yyyy-MM-dd-HH:mm",
		"yyyy-MM-dd-HH",
		"yyyy-MM-dd",
		"YYYY.ww",
		"yyyy-MM",
		"yyyy"
	};
	
	/** Returns the date corresponding to a string in one of the formats returned by getTimeBasedSuffix
	 * @param suffix - the date string
	 * @return - either the date, or an error if the string is not correctly formatted
	 */
	public static Validation<String, Date> getDateFromSuffix(final String suffix) {
		try {
			return Validation.success(DateUtils.parseDateStrictly(suffix, SUPPORTED_DATE_SUFFIXES));
		} catch (ParseException e) {
			return Validation.fail(ErrorUtils.getLongForm("getDateFromSuffix {0}", e));
		}
	}
	
	/** Attempts to parse a (typically recurring) time  
	 * @param human_readable_duration - Uses some simple regexes (1h,d, 1month etc), and Natty (try examples http://natty.joestelmach.com/try.jsp#)
	 * @return the machine readable duration, or an error
	 */
	public static Validation<String, Duration> getDuration(final String human_readable_duration) {
		return getDuration(human_readable_duration, Optional.empty());
	}
	/** Attempts to parse a (typically recurring) time  
	 * @param human_readable_duration - Uses some simple regexes (1h,d, 1month etc), and Natty (try examples http://natty.joestelmach.com/try.jsp#)
	 * @param base_date - for relative date, locks the date to this origin (mainly for testing in this case?)
	 * @return the machine readable duration, or an error
	 */
	public static Validation<String, Duration> getDuration(final String human_readable_duration, Optional<Date> base_date) {
		// There's a few different cases:
		// - the validation from getTimePeriod
		// - a slightly more complicated version <N><p> where <p> == period from the above
		// - use Natty for more complex expressions
		
		final Validation<String, ChronoUnit> first_attempt = getTimePeriod(human_readable_duration);
		if (first_attempt.isSuccess()) {
			return Validation.success(Duration.of(first_attempt.success().getDuration().getSeconds(), ChronoUnit.SECONDS));
		}
		else { // Slightly more complex version
			final Matcher m = date_parser.matcher(human_readable_duration);
			if (m.matches()) {
				final Validation<String, Duration> candidate_ret = getTimePeriod(m.group(2))
						.map(cu -> {
							final LocalDateTime now = LocalDateTime.now();
							return Duration.between(now, now.plus(Integer.parseInt(m.group(1)), cu));
						});
				
				if (candidate_ret.isSuccess()) return candidate_ret;
			}
		}
		// If we're here then try Natty
		final Date now = base_date.orElse(new Date());
		return getSchedule(human_readable_duration, Optional.of(now)).map(d -> {
			final long duration = d.getTime() - now.getTime();
			return Duration.of(duration, ChronoUnit.MILLIS);
		});
	}
	
	/** Returns a date from a human readable date
	 * @param human_readable_date - the date expressed in words, eg "next wednesday".. Uses some simple regexes (1h,d, 1month etc), and Natty (try examples http://natty.joestelmach.com/try.jsp#)
	 * @param base_date - for relative date, locks the date to this origin
	 * @return the machine readable date, or an error
	 */
	public static Validation<String, Date> getSchedule(final String human_readable_date, Optional<Date> base_date) {
		try { // just read the first - note can ignore all the error checking here, just fail out using the try/catch
			CalendarSource.setBaseDate(base_date.orElse(new Date()));
			final Parser parser = new Parser();
			final List<DateGroup> l = parser.parse(human_readable_date);
			final DateGroup d = l.get(0);
			if (!d.getText().matches("^.*[a-zA-Z]+.*$")) { // only matches numbers, not allowed - must have missed a prefix
				return Validation.fail(ErrorUtils.get(ErrorUtils.INVALID_DATETIME_FORMAT, human_readable_date));
			}
			final List<Date> l2 = d.getDates();
			return Validation.success(l2.get(0));
		}
		catch (Exception e) {
			final Pattern numChronoPattern = Pattern.compile("^([\\d]+)(.*)");			
			final Matcher m = numChronoPattern.matcher(human_readable_date);
			return m.find() 
					? getTimePeriod(m.group(2)).map(c -> c.getDuration().get(ChronoUnit.SECONDS)).map(l -> new Date(base_date.orElse(new Date()).getTime() + Long.parseLong(m.group(1))*l*1000L))
					: getTimePeriod(human_readable_date).map(c -> c.getDuration().get(ChronoUnit.SECONDS)).map(l -> new Date(base_date.orElse(new Date()).getTime() + l*1000L));		
		}		
	}
	
	private static Pattern date_parser = Pattern.compile("^\\s*([0-9]+)\\s*([a-z][a-rt-z]*)s?\\s*$", Pattern.CASE_INSENSITIVE);
}
