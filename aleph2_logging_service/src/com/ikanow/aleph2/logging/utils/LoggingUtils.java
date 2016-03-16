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
package com.ikanow.aleph2.logging.utils;

import java.util.HashMap;
import java.util.Optional;

import org.apache.logging.log4j.Level;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.ikanow.aleph2.core.shared.services.MultiDataService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.ManagementSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.ManagementSchemaBean.LoggingSchemaBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.BucketUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;

/**
 * @author Burch
 *
 */
public class LoggingUtils {
	private static final String EXTERNAL_PREFIX = "/external";
	private static final String DEFAULT_LEVEL_KEY = "__DEFAULT__";
	
	/**
	 * Builds a JsonNode log message object, contains fields for date, message, generated_by, bucket, subsystem, and severity
	 * 
	 * @param level
	 * @param bucket
	 * @param message
	 * @param isSystemMessage
	 * @return
	 */
	public static JsonNode createLogObject(final Level level, final DataBucketBean bucket, final BasicMessageBean message, final boolean isSystemMessage, final String date_field) {
		final ObjectMapper _mapper = new ObjectMapper();
		return Optional.ofNullable(message.details()).map(d -> _mapper.convertValue(d, ObjectNode.class)).orElseGet(() -> _mapper.createObjectNode())
				.put(date_field, message.date().getTime()) //TODO can I actually pass in a date object/need to?
				.put("message", ErrorUtils.show(message))
				.put("generated_by", isSystemMessage ? "system" : "user")
				.put("bucket", bucket.full_name())
				.put("subsystem", message.source())
				.put("severity", level.toString());			
	}
	
	/**
	 * Builds a minimal bucket pointing the full path to the external bucket/subsystem
	 * 
	 * @param subsystem
	 * @return
	 */
	public static DataBucketBean getExternalBucket(final String subsystem, final Level default_log_level) {
		return BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::full_name, EXTERNAL_PREFIX + "/" + subsystem + "/")	
				.with(DataBucketBean::management_schema, BeanTemplateUtils.build(ManagementSchemaBean.class)
							.with(ManagementSchemaBean::logging_schema, BeanTemplateUtils.build(LoggingSchemaBean.class)
										.with(LoggingSchemaBean::log_level, default_log_level)
									.done().get())
						.done().get())
				.done().get();
	}
	

	
	/**
	 * Returns back a IDataWriteService pointed at a logging output location for the given bucket
	 * @param bucket
	 * @return
	 */
	public static MultiDataService getLoggingServiceForBucket(final IServiceContext service_context, final DataBucketBean bucket) {
		//change the bucket.full_name to point to a logging location
		final DataBucketBean bucket_logging = BucketUtils.convertDataBucketBeanToLogging(bucket);
		//return crudservice pointing to this path
		return MultiDataService.getMultiWriter(bucket_logging, service_context); 
	}

	/**
	 * Creates a map of subsystem -> logging level for quick lookups.  Grabs the overrides from
	 * bucket.management_schema().logging_schema().log_level_overrides()
	 * 
	 * Returns an empty list if none exist there
	 * 
	 * @param bucket
	 * @return
	 */
	public static ImmutableMap<String, Level> getBucketLoggingThresholds(final DataBucketBean bucket) {
		//if overrides are set, create a map with them and the default
		if (bucket.management_schema() != null &&
				bucket.management_schema().logging_schema() != null ) {
			return new ImmutableMap.Builder<String, Level>()
		 	.put(DEFAULT_LEVEL_KEY, bucket.management_schema().logging_schema().log_level())
		 	.putAll(Optional.ofNullable(bucket.management_schema().logging_schema().log_level_overrides()).orElse(new HashMap<String, Level>()))
		 .build();
		} else {
			//otherwise just return an empty map
			return new ImmutableMap.Builder<String, Level>().build();
		}
	}
	
	/**
	 * Gets the minimal log level for the given subsystem by checking:
	 * 1. if logging_overrides has that subsystem as a key
	 * 2. if not, checks if there is a default level set
	 * 3. if not, uses default_log_level
	 * Then returns if the passed in log level is above the minimal log level or not.
	 * 
	 * @param level
	 * @param logging_overrides
	 * @param subsystem
	 * @param default_log_level
	 * @return
	 */
	public static boolean meetsLogLevelThreshold(final Level level, final ImmutableMap<String, Level> logging_overrides, final String subsystem, final Level default_log_level) {
		final Level curr_min_level = 
				Optional.ofNullable(logging_overrides.get(subsystem))
				.orElseGet(() -> (Optional.ofNullable(logging_overrides.get(DEFAULT_LEVEL_KEY))
						.orElse(default_log_level)));	
		return curr_min_level.isLessSpecificThan(level);
	}
	
	/**
	 * Creates an empty DataBucketBean to use during creation of a MultiWriter
	 * as a way to create a safe empty writer that will do nothing when invoked.
	 * @return
	 */
	public static DataBucketBean getEmptyBucket() {
		return BeanTemplateUtils.build(DataBucketBean.class).done().get();
	}
}
