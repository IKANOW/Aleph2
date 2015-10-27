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

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

import org.junit.Test;

import scala.Tuple2;

import com.google.common.collect.ImmutableMap;

import fj.data.Validation;

public class TestTimeUtils {

	@Test
	public void test_getTimePeriod() {
		
		final List<String> secs = Arrays.asList("s", "1s", "sec", "1sec", "second", "1second", "1 second");
		final List<String> mins = Arrays.asList("min", "1min", "minute", "1minute", "1 minute");
		final List<String> hours = Arrays.asList("h", "1h", "hour", "1hour", "hourly", "1 hour");
		final List<String> days = Arrays.asList("d", "1d", "day", "1day", "daily", "1 day");
		final List<String> weeks = Arrays.asList("w", "1w", "week", "1week", "weekly", "wk", "1wk", "1 week");
		final List<String> months = Arrays.asList("month", "1month", "monthly", "1 month");
		final List<String> years = Arrays.asList("y", "1y", "year", "1year", "yearly", "yr", "1yr", "1 year");
		final Map<ChronoUnit, List<String>> test_map = ImmutableMap.<ChronoUnit, List<String>>builder()
											.put(ChronoUnit.SECONDS, secs)
											.put(ChronoUnit.MINUTES, mins)
											.put(ChronoUnit.HOURS, hours)
											.put(ChronoUnit.DAYS, days)
											.put(ChronoUnit.WEEKS, weeks)
											.put(ChronoUnit.MONTHS, months)
											.put(ChronoUnit.YEARS, years)
											.build();
		
		for (Map.Entry<ChronoUnit, List<String>> kv: test_map.entrySet()) {
			for (String s: kv.getValue()) {
				final Validation<String, ChronoUnit> result = TimeUtils.getTimePeriod(s);
				assertTrue("Succeeded", result.isSuccess());
				assertEquals(kv.getKey().toString(), result.success().toString());
			}
		}
		// Check failure cases:
		final Validation<String, ChronoUnit> error = TimeUtils.getTimePeriod("banana");
		assertTrue("fails", error.isFail());
		assertEquals(ErrorUtils.get(ErrorUtils.INVALID_DATETIME_FORMAT, "banana"), error.fail());
	}

	@Test
	public void test_getSchedule() {
		// Check success
		
		final Date now = new Date(946706400000L); //6am 1 Jan 2000
		
		Validation<String, Date> result = TimeUtils.getSchedule("next wednesday", Optional.of(now));
		
		assertTrue("Passes", result.isSuccess());
		assertEquals(947052000000L, result.success().getTime());
		
		// Check success via duration
		
		Validation<String, Date> result2 = TimeUtils.getSchedule("hourly", Optional.of(now));
		
		assertTrue("Passes", result2.isSuccess());
		assertEquals(946710000000L, result2.success().getTime());
		
		// Check failure
		
		Validation<String, Date> error1 = TimeUtils.getSchedule("banana", Optional.of(now));
		assertTrue("Fails", error1.isFail());
		assertEquals(ErrorUtils.get(ErrorUtils.INVALID_DATETIME_FORMAT, "banana"), error1.fail());
		
		//check shorthand success
		Validation<String, Date> result3 = TimeUtils.getSchedule("5w", Optional.of(now));
		assertTrue("Passes", result3.isSuccess());
		assertEquals(949730400000L, result3.success().getTime());
		
		//check shorthand no number success
		Validation<String, Date> result4 = TimeUtils.getSchedule("w", Optional.of(now));		
		assertTrue("Passes", result4.isSuccess());
		assertEquals(947311200000L, result4.success().getTime());
		
		//check shorthand failure
		Validation<String, Date> error2 = TimeUtils.getSchedule("5nonsense", Optional.of(now));		
		assertTrue("Fails", error2.isFail());
		assertEquals(ErrorUtils.get(ErrorUtils.INVALID_DATETIME_FORMAT, "nonsense"), error2.fail());
	}
	
	@SuppressWarnings("unused")
	@Test
	public void test_timePeriods() {
		String s1, s2, s3, s4, s5, s6, s7;
		
		assertEquals("yyyy-MM-dd-HH:mm:ss", s1 = TimeUtils.getTimeBasedSuffix(ChronoUnit.SECONDS, Optional.empty()));
		assertEquals("yyyy-MM-dd-HH:mm", s2 = TimeUtils.getTimeBasedSuffix(ChronoUnit.MINUTES, Optional.empty()));
		assertEquals("yyyy-MM-dd-HH", s3 = TimeUtils.getTimeBasedSuffix(ChronoUnit.HOURS, Optional.empty()));
		assertEquals("yyyy-MM-dd", s4 = TimeUtils.getTimeBasedSuffix(ChronoUnit.DAYS, Optional.empty()));
		assertEquals("YYYY.ww", s5 = TimeUtils.getTimeBasedSuffix(ChronoUnit.WEEKS, Optional.empty()));
		assertEquals("yyyy-MM", s6 = TimeUtils.getTimeBasedSuffix(ChronoUnit.MONTHS, Optional.empty()));
		assertEquals("yyyy", s7 = TimeUtils.getTimeBasedSuffix(ChronoUnit.YEARS, Optional.empty()));
		assertEquals("yyyy-MM-dd-HH", s1 = TimeUtils.getTimeBasedSuffix(ChronoUnit.SECONDS, Optional.of(ChronoUnit.HOURS)));
		assertEquals("yyyy-MM-dd", s4 = TimeUtils.getTimeBasedSuffix(ChronoUnit.DAYS, Optional.of(ChronoUnit.HOURS)));
		assertEquals("", TimeUtils.getTimeBasedSuffix(ChronoUnit.CENTURIES, Optional.empty()));
	}		
	
	
	@Test
	public void test_getDuration() {
		// Check very simple
		{
			final Validation<String, Duration> res1 = TimeUtils.getDuration("1d");
			assertTrue("Passes", res1.isSuccess());
			assertEquals(res1.success().getSeconds(), 24*3600L);
			
			final Validation<String, Duration> err1 = TimeUtils.getDuration("1x");
			assertTrue("Fails", err1.isFail());
			assertEquals(ErrorUtils.get(ErrorUtils.INVALID_DATETIME_FORMAT, "x"), err1.fail());
		}
		// Check very simple
		{
			final Validation<String, Duration> res1 = TimeUtils.getDuration("2w");
			assertTrue("Passes", res1.isSuccess());
			assertEquals(res1.success().getSeconds(), 14*24*3600L);
		}
		
		// Check slightly more complex
		{
			final Validation<String, Duration> res1 = TimeUtils.getDuration("4 days");
			assertTrue("Passes", res1.isSuccess());
			assertEquals(res1.success().getSeconds(), 4*24*3600L);
			
			final Validation<String, Duration> err1 = TimeUtils.getDuration("1 bananas");
			
			assertTrue("Fails", err1.isFail());
			assertEquals(ErrorUtils.get(ErrorUtils.INVALID_DATETIME_FORMAT, "1 bananas"), err1.fail());
		}
		
		// Check getSchedule version
		{
			final Date now = new Date(946706400000L); //1am 1 Jan 2000
			
			Calendar calendar = GregorianCalendar.getInstance();
			calendar.setTime(now);			
			final Validation<String, Duration> res1 = TimeUtils.getDuration("3pm", Optional.of(now));
			assertTrue("Passes", res1.isSuccess());
			assertEquals((15 - calendar.get(Calendar.HOUR))*3600L, res1.success().getSeconds());
			
			final Validation<String, Duration> err1 = TimeUtils.getDuration("1 bananas");
			
			assertTrue("Fails", err1.isFail());
			assertEquals(ErrorUtils.get(ErrorUtils.INVALID_DATETIME_FORMAT, "1 bananas"), err1.fail());
		}
		
		// Check getSchedule version - definitely not in the format <number><period>
		{
			final Date now = new Date(946706400000L); //1am 1 Jan 2000 (Sat)
			
			final Validation<String, Duration> res1 = TimeUtils.getDuration("next monday", Optional.of(now));
			assertTrue("Passes", res1.isSuccess());
			assertEquals(2*24*3600L, res1.success().getSeconds());
			
			final Validation<String, Duration> err1 = TimeUtils.getDuration("1 bananas");
			
			assertTrue("Fails", err1.isFail());
			assertEquals(ErrorUtils.get(ErrorUtils.INVALID_DATETIME_FORMAT, "1 bananas"), err1.fail());
		}
	}
	
	@Test
	public void test_getDateFromSuffix() {
		{
			final String suffix = "2012-11-14-13:49:48";
			final Validation<String, Date> v = TimeUtils.getDateFromSuffix(suffix);
			assertTrue("Date was parsed: " + suffix, v.isSuccess());
			assertEquals("Wed Nov 14 13:49:48 2012", v.success().toString().replaceAll(" [A-Z]{3,} ", " "));
		}
		{
			final String suffix = "2012-11-14-13:49";
			final Validation<String, Date> v = TimeUtils.getDateFromSuffix(suffix);
			assertTrue("Date was parsed: " + suffix, v.isSuccess());
			assertEquals("Wed Nov 14 13:49:00 2012", v.success().toString().replaceAll(" [A-Z]{3,} ", " "));
		}
		{
			final String suffix = "2012-11-14-13";
			final Validation<String, Date> v = TimeUtils.getDateFromSuffix(suffix);
			assertTrue("Date was parsed: " + suffix, v.isSuccess());
			assertEquals("Wed Nov 14 13:00:00 2012", v.success().toString().replaceAll(" [A-Z]{3,} ", " "));
		}
		{
			final String suffix = "2012-11-14";
			final Validation<String, Date> v = TimeUtils.getDateFromSuffix(suffix);
			assertTrue("Date was parsed: " + suffix, v.isSuccess());
			assertEquals("Wed Nov 14 00:00:00 2012", v.success().toString().replaceAll(" [A-Z]{3,} ", " "));
		}
		{
			final String suffix = "2012.20";
			final Validation<String, Date> v = TimeUtils.getDateFromSuffix(suffix);
			assertTrue("Date was parsed: " + suffix, v.isSuccess());
			assertEquals("Sun May 13 00:00:00 2012", v.success().toString().replaceAll(" [A-Z]{3,} ", " "));
		}
		{
			final String suffix = "2012-11";
			final Validation<String, Date> v = TimeUtils.getDateFromSuffix(suffix);
			assertTrue("Date was parsed: " + suffix, v.isSuccess());
			assertEquals("Thu Nov 01 00:00:00 2012", v.success().toString().replaceAll(" [A-Z]{3,} ", " "));
		}
		{
			final String suffix = "2012";
			final Validation<String, Date> v = TimeUtils.getDateFromSuffix(suffix);
			assertTrue("Date was parsed: " + suffix, v.isSuccess());
			assertEquals("Sun Jan 01 00:00:00 2012", v.success().toString().replaceAll(" [A-Z]{3,} ", " "));
		}
		{
			final String suffix = "fail";
			final Validation<String, Date> v = TimeUtils.getDateFromSuffix(suffix);
			assertTrue("Date was parsed: " + suffix, v.isFail());
		}
	}
	
	@Test
	public void test_getTimeInfo() {
		{
			final String suffix = "2012-11-14-13:49:48";
			final Optional<Tuple2<String, ChronoUnit>> res = TimeUtils.getFormatInfoFromDateString(suffix);
			assertEquals(Optional.of(Tuples._2T("yyyy-MM-dd-HH:mm:ss", ChronoUnit.SECONDS)), res);
		}
		{
			final String suffix = "2012-11-14-13:49";
			final Optional<Tuple2<String, ChronoUnit>> res = TimeUtils.getFormatInfoFromDateString(suffix);
			assertEquals(Optional.of(Tuples._2T("yyyy-MM-dd-HH:mm", ChronoUnit.MINUTES)), res);
		}
		{
			final String suffix = "2012-11-14-13";
			final Optional<Tuple2<String, ChronoUnit>> res = TimeUtils.getFormatInfoFromDateString(suffix);
			assertEquals(Optional.of(Tuples._2T("yyyy-MM-dd-HH", ChronoUnit.HOURS)), res);
		}
		{
			final String suffix = "2012-11-14";
			final Optional<Tuple2<String, ChronoUnit>> res = TimeUtils.getFormatInfoFromDateString(suffix);
			assertEquals(Optional.of(Tuples._2T("yyyy-MM-dd", ChronoUnit.DAYS)), res);
		}
		{
			final String suffix = "2012.20";
			final Optional<Tuple2<String, ChronoUnit>> res = TimeUtils.getFormatInfoFromDateString(suffix);
			assertEquals(Optional.of(Tuples._2T("YYYY.ww", ChronoUnit.WEEKS)), res);
		}
		{
			final String suffix = "2012-11";
			final Optional<Tuple2<String, ChronoUnit>> res = TimeUtils.getFormatInfoFromDateString(suffix);
			assertEquals(Optional.of(Tuples._2T("yyyy-MM", ChronoUnit.MONTHS)), res);
		}
		{
			final String suffix = "2012";
			final Optional<Tuple2<String, ChronoUnit>> res = TimeUtils.getFormatInfoFromDateString(suffix);
			assertEquals(Optional.of(Tuples._2T("yyyy", ChronoUnit.YEARS)), res);
		}
		{
			final String suffix = "299";
			final Optional<Tuple2<String, ChronoUnit>> res = TimeUtils.getFormatInfoFromDateString(suffix);
			assertEquals(Optional.empty(), res);
		}
		{
			final String suffix = "fail";
			final Optional<Tuple2<String, ChronoUnit>> res = TimeUtils.getFormatInfoFromDateString(suffix);
			assertEquals(Optional.empty(), res);
		}
		
	}
}
