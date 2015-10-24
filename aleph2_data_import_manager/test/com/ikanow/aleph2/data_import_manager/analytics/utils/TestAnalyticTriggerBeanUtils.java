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
package com.ikanow.aleph2.data_import_manager.analytics.utils;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;

import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerStateBean;

public class TestAnalyticTriggerBeanUtils {

	@Test
	public void test_automaticTriggers() {
		
		final DataBucketBean auto_trigger_bucket = TestAnalyticTriggerCrudUtils.buildBucket("/test/trigger", false);
		
		final Stream<AnalyticTriggerStateBean> test_stream = AnalyticTriggerBeanUtils.generateTriggerStateStream(auto_trigger_bucket, false, Optional.empty());
		final List<AnalyticTriggerStateBean> test_list = test_stream.collect(Collectors.toList());
		
		System.out.println("Resources = \n" + 
				test_list.stream().map(t -> BeanTemplateUtils.toJson(t).toString()).collect(Collectors.joining("\n")));
		
		assertEquals(7, test_list.size()); // (3 inputs + 4 job deps:)
		assertEquals(3, test_list.stream().filter(trigger -> null == trigger.job_name()).count());
		assertEquals(4, test_list.stream().filter(trigger -> null != trigger.job_name()).count());
		
	}
	
}
