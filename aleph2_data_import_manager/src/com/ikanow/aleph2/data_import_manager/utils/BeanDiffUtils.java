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
package com.ikanow.aleph2.data_import_manager.utils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.objects.data_import.BucketDiffBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;

/** Utilities for creating a difference bean between old and new buckets
 * @author Caleb
 */
public class BeanDiffUtils {


	public static Optional<BucketDiffBean> createDiffBean(
			final DataBucketBean updated_bucket, final DataBucketBean original_bucket) {
		return Optional.of(BeanTemplateUtils.build(BucketDiffBean.class)
				.with(BucketDiffBean::diffs, createDataBucketBeanDiffs(updated_bucket, original_bucket))
				.with(BucketDiffBean::lib_diffs, createSharedLibraryDiffs(updated_bucket, original_bucket))
				.done().get());
	}
	
	public static Map<String, Boolean> createDataBucketBeanDiffs(
			DataBucketBean updated_bucket, DataBucketBean original_bucket) {
		final JsonNode updated_bucket_jsonnode = BeanTemplateUtils.toJson(updated_bucket);
		final JsonNode original_bucket_jsonnode = BeanTemplateUtils.toJson(original_bucket);
		final Map<String, Boolean> diff = new HashMap<String, Boolean>();
		
		//this only reports back fields that are not null, so we have to run both directions
		original_bucket_jsonnode.fields().forEachRemaining(field -> {
			diff.put(field.getKey(), !(field.getValue().equals(updated_bucket_jsonnode.get(field.getKey()))));			
		});
		updated_bucket_jsonnode.fields().forEachRemaining(field -> {
			//if the field wasn't already checked, it means it only exists here, don't bother don't expensive compares
			if ( !diff.containsKey(field.getKey()) )
				diff.put(field.getKey(), true);			
		});
		
		return diff;
	}

	public static Set<String> createSharedLibraryDiffs(
			final DataBucketBean updated_bucket, final DataBucketBean original_bucket ) {
		//TODO shared lib diff (or are we handling this in the shared library crud in CMDB)
		return new HashSet<String>();
	}
}
