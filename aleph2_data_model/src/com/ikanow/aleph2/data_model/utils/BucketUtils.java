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

import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;

/**
 * Provides a set of util functions for DataBucketBean
 * 
 * @author Burch
 *
 */
public class BucketUtils {
	/**
	 * Returns a clone of the bean and modifies the full_name field to provide a
	 * test path instead (by prepending "/alphe2_testing/{user_id}" to the path).
	 * 
	 * @param original_bean
	 * @param user_id
	 * @return original_bean with the full_name field modified with a test path
	 */
	public static DataBucketBean convertDataBucketBeanToTest(final DataBucketBean original_bean, String user_id) {
		//TODO when creating a bucket do we need to block any attempt of
		//users to start with /test?
		final String new_full_name = "/aleph2_testing/" + user_id + "/" + original_bean.full_name();
		return BeanTemplateUtils.clone(original_bean)
				.with(DataBucketBean::full_name, new_full_name)
				.done();
	}
}
