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
package com.ikanow.aleph2.analytics.utils;

import static org.junit.Assert.*;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.Test;

import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean.AnalyticThreadJobInputConfigBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;

public class TestTimeSliceDirUtils {

	
	@Test
	public void test_getQueryTimeRange() {
		
		final Date now = new Date();

		{
			AnalyticThreadJobInputConfigBean test =
					BeanTemplateUtils.build(AnalyticThreadJobInputConfigBean.class)
					.done().get();
			
			assertEquals(Tuples._2T(Optional.empty(), Optional.empty()), TimeSliceDirUtils.getQueryTimeRange(test, now));
			
		}
		{
			AnalyticThreadJobInputConfigBean test =
					BeanTemplateUtils.build(AnalyticThreadJobInputConfigBean.class)
						.with(AnalyticThreadJobInputConfigBean::time_min, "fail")
						.with(AnalyticThreadJobInputConfigBean::time_max, "3 days")
					.done().get();
			
			final Date expected = Date.from(now.toInstant().minus(3L, ChronoUnit.DAYS));
			assertEquals(Tuples._2T(Optional.empty(), Optional.of(expected)), TimeSliceDirUtils.getQueryTimeRange(test, now));			
		}
		{
			AnalyticThreadJobInputConfigBean test =
					BeanTemplateUtils.build(AnalyticThreadJobInputConfigBean.class)
						.with(AnalyticThreadJobInputConfigBean::time_min, "1 day")
						.with(AnalyticThreadJobInputConfigBean::time_max, "fail")
					.done().get();
			
			final Date expected = Date.from(now.toInstant().minus(1L, ChronoUnit.DAYS));
			assertEquals(Tuples._2T(Optional.of(expected), Optional.empty()), TimeSliceDirUtils.getQueryTimeRange(test, now));			
		}
		{
			AnalyticThreadJobInputConfigBean test =
					BeanTemplateUtils.build(AnalyticThreadJobInputConfigBean.class)
						.with(AnalyticThreadJobInputConfigBean::time_min, "1 day")
						.with(AnalyticThreadJobInputConfigBean::time_max, "yesterday")
					.done().get();
			
			final Date expected = Date.from(now.toInstant().minus(1L, ChronoUnit.DAYS));
			assertEquals(Tuples._2T(Optional.of(expected), Optional.of(expected)), TimeSliceDirUtils.getQueryTimeRange(test, now));			
		}
	}
	
	@Test
	public void test_annotateTimedDirectories() throws ParseException {
		
		final List<String> test_dirs = 
				Arrays.asList(
					"fail1", 
					"fail2_notdate",
					"works_2015",
					"works_2015-01",
					"works_2015-01-01",
					"works_2015-01-01-01",
					"works_2015.10"
					);
		
		final Date d1 = Date.from(LocalDateTime.of(2015, 1, 1, 0, 0).atZone(ZoneOffset.systemDefault()).toInstant());
		final Date d2 = Date.from(LocalDateTime.of(2016, 1, 1, 0, 0).atZone(ZoneOffset.systemDefault()).toInstant());
		final Date d3 = Date.from(LocalDateTime.of(2015, 2, 1, 0, 0).atZone(ZoneOffset.systemDefault()).toInstant());
		final Date d4 = Date.from(LocalDateTime.of(2015, 1, 2, 0, 0).atZone(ZoneOffset.systemDefault()).toInstant());
		final Date d5 = Date.from(LocalDateTime.of(2015, 1, 1, 1, 0).atZone(ZoneOffset.systemDefault()).toInstant());
		final Date d6 = Date.from(LocalDateTime.of(2015, 1, 1, 2, 0).atZone(ZoneOffset.systemDefault()).toInstant());
				
		final Date d7 = new SimpleDateFormat("YYYY.ww").parse("2015.10");
		final Date d8 = new SimpleDateFormat("YYYY.ww").parse("2015.11");
		
		assertEquals(Arrays.asList(
				Tuples._3T("works_2015", d1, d2),
				Tuples._3T("works_2015-01", d1, d3),
				Tuples._3T("works_2015-01-01", d1, d4),
				Tuples._3T("works_2015-01-01-01", d5, d6),
				Tuples._3T("works_2015.10", d7, d8)
				),
				TimeSliceDirUtils.annotateTimedDirectories(test_dirs.stream()).collect(Collectors.toList()));
	}
	
	@Test
	public void test_filterTimedDirectories() {
		
		final List<String> test_dirs = 
				Arrays.asList(
					"fail",
					"works_2015-06-01",
					"works_2015-06-03",
					"works_2015-01-01-01",
					"works_2035-01-01-01"
					);
		
		// No filters:
		{
			final Date now = new Date();
			
			final AnalyticThreadJobInputConfigBean test =
					BeanTemplateUtils.build(AnalyticThreadJobInputConfigBean.class)
					.done().get();
			
			final List<String> res = 
					TimeSliceDirUtils.filterTimedDirectories(
							TimeSliceDirUtils.annotateTimedDirectories(test_dirs.stream()), 
							TimeSliceDirUtils.getQueryTimeRange(test, now))
							.collect(Collectors.toList());
						
			assertEquals(
					Arrays.asList(
							"works_2015-06-01",
							"works_2015-06-03",
							"works_2015-01-01-01",
							"works_2035-01-01-01"
							)
					, 
					res);
		}
		// min filter
		{
			final Date now = Date.from(LocalDateTime.of(2015, 6, 12, 2, 0).atZone(ZoneOffset.systemDefault()).toInstant());
			
			AnalyticThreadJobInputConfigBean test =
					BeanTemplateUtils.build(AnalyticThreadJobInputConfigBean.class)
						.with(AnalyticThreadJobInputConfigBean::time_min, "10 days")
						.with(AnalyticThreadJobInputConfigBean::time_max, "fail")
					.done().get();
			
			final List<String> res = 
					TimeSliceDirUtils.filterTimedDirectories(
							TimeSliceDirUtils.annotateTimedDirectories(test_dirs.stream()), 
							TimeSliceDirUtils.getQueryTimeRange(test, now))
							.collect(Collectors.toList());
						
			assertEquals(
					Arrays.asList(
							"works_2015-06-03",
							"works_2035-01-01-01"
							)
					, 
					res);
		}
		// max filter
		{
			final Date now = Date.from(LocalDateTime.of(2015, 6, 12, 2, 0).atZone(ZoneOffset.systemDefault()).toInstant());
			
			AnalyticThreadJobInputConfigBean test =
					BeanTemplateUtils.build(AnalyticThreadJobInputConfigBean.class)
						.with(AnalyticThreadJobInputConfigBean::time_min, null)
						.with(AnalyticThreadJobInputConfigBean::time_max, "10 days")
					.done().get();
			
			final List<String> res = 
					TimeSliceDirUtils.filterTimedDirectories(
							TimeSliceDirUtils.annotateTimedDirectories(test_dirs.stream()), 
							TimeSliceDirUtils.getQueryTimeRange(test, now))
							.collect(Collectors.toList());
						
			assertEquals(
					Arrays.asList(
							"works_2015-06-01",
							"works_2015-01-01-01"
							)
					, 
					res);
		}
		// min + max
		{
			final Date now = Date.from(LocalDateTime.of(2015, 6, 12, 2, 0).atZone(ZoneOffset.systemDefault()).toInstant());
			
			AnalyticThreadJobInputConfigBean test =
					BeanTemplateUtils.build(AnalyticThreadJobInputConfigBean.class)
						.with(AnalyticThreadJobInputConfigBean::time_min, "12 days")
						.with(AnalyticThreadJobInputConfigBean::time_max, "8 days")
					.done().get();
			
			final List<String> res = 
					TimeSliceDirUtils.filterTimedDirectories(
							TimeSliceDirUtils.annotateTimedDirectories(test_dirs.stream()), 
							TimeSliceDirUtils.getQueryTimeRange(test, now))
							.collect(Collectors.toList());
						
			assertEquals(
					Arrays.asList(
							"works_2015-06-01",
							"works_2015-06-03"
							)
					, 
					res);
		}
		
		// Coverage!
		
		new TimeSliceDirUtils();
		
	}
	
}
