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

import java.net.InetAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Level;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.ikanow.aleph2.core.shared.services.MultiDataService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.ManagementSchemaBean;
import com.ikanow.aleph2.data_model.objects.shared.ManagementSchemaBean.LoggingSchemaBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.BucketUtils;

/**
 * @author Burch
 *
 */
public class LoggingUtils {
	protected static ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());
	
	private static final String EXTERNAL_PREFIX = "/external";
	private static final String DEFAULT_LEVEL_KEY = "__DEFAULT__";
	public static final String LAST_LOG_TIMESTAMP_FIELD = "last_log_timestamp";
	public static final String LOG_COUNT_FIELD = "log_count";
	protected static final ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());
	
	/**
	 * Builds a JsonNode log message object, contains fields for date, message, generated_by, bucket, subsystem, and severity
	 * 
	 * @param level
	 * @param bucket
	 * @param message
	 * @param isSystemMessage
	 * @return
	 */
	public static JsonNode createLogObject(final Level level, final DataBucketBean bucket, final BasicMessageBean message, final boolean isSystemMessage, 
			final String date_field, final String hostname) {
		return Optional.ofNullable(message.details()).map(d -> _mapper.convertValue(d, ObjectNode.class)).orElseGet(() -> _mapper.createObjectNode())
				.put(date_field, message.date().getTime())
				.put("message", message.message())
				.put("generated_by", isSystemMessage ? "system" : "user")
				.put("bucket", bucket.full_name())
				.put("subsystem", message.source())
				.put("command", message.command())
				.put("severity", level.toString())		
				.put("hostname", hostname);
	}
	
	/** Returns the hostname
	 * @return
	 */
	public static String getHostname() {
		try {
			return InetAddress.getLocalHost().getHostName();
		} catch (Exception e) {
			return "UNKNOWN";
		}
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
										.with(LoggingSchemaBean::log_level, default_log_level.toString())
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
	 * @param default_system_level 
	 * @return
	 */
	public static ImmutableMap<String, Level> getBucketLoggingThresholds(final DataBucketBean bucket, final Level default_system_level) {
		//if overrides are set, create a map with them and the default
		if (bucket.management_schema() != null &&
				bucket.management_schema().logging_schema() != null ) {
			return new ImmutableMap.Builder<String, Level>()
		 	.put(DEFAULT_LEVEL_KEY, Optional.ofNullable(bucket.management_schema().logging_schema().log_level()).map(l->Level.valueOf(l)).orElse(default_system_level))
		 	.putAll(Optional.ofNullable(bucket.management_schema().logging_schema().log_level_overrides())
		 			.orElse(new HashMap<String, String>())
		 			.entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> Level.valueOf(e.getValue())))) //convert String Level to log4j.Level
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

	/**
	 * Updates a BMBs merge info object w/ an updated count (tracking every log message of this type sent in) and
	 * a timestamp of the last time a message was actually logged (i.e. not filtered via rule or log level).
	 * @param bmb
	 * @param of
	 * @return
	 */
	public static Tuple2<BasicMessageBean, Map<String, Object>> updateInfo(final Tuple2<BasicMessageBean, Map<String, Object>> merge_info, final Optional<Long> timestamp) {
		merge_info._2.merge(LOG_COUNT_FIELD, 1L, (n,o)->(Long)n+(Long)o);
		timestamp.ifPresent(t->merge_info._2.replace(LAST_LOG_TIMESTAMP_FIELD, t));
		return merge_info;
	}
	
	/**
	 * Merges the BMB message with an existing old entry (if it exists) otherwise merges with null.  If 
	 * an entry did not exist, creates new default Map in the tuple for storing merge info. 
	 * @param basicMessageBean
	 * @param merge_key
	 * @return
	 */
	public static Tuple2<BasicMessageBean, Map<String, Object>> getOrCreateMergeInfo(final Map<String, Tuple2<BasicMessageBean, Map<String,Object>>> merge_logs, final BasicMessageBean message, final String merge_key, final BiFunction<BasicMessageBean, BasicMessageBean, BasicMessageBean>[] merge_operations) {			
		return merge_logs.compute(merge_key, (k, v) -> {
			if ( v == null ) {
				//final BasicMessageBean bmb = merge_operations.apply(message, null);
				final BasicMessageBean bmb = Arrays.stream(merge_operations).reduce(null, (bmb_a,fn)->fn.apply(message, bmb_a), (bmb_a,bmb_b)->bmb_a);
				Map<String, Object> info = new HashMap<String, Object>();
				info.put(LOG_COUNT_FIELD, 0L);
				info.put(LAST_LOG_TIMESTAMP_FIELD, 0L);
				return new Tuple2<BasicMessageBean, Map<String,Object>>(bmb, info);
			} else {
				//merge with old entry
				//final BasicMessageBean bmb = merge_operations.apply(message, merge_logs.get(merge_key)._1);
				final BasicMessageBean bmb = Arrays.stream(merge_operations).reduce(merge_logs.get(merge_key)._1, (bmb_a,fn)->fn.apply(message, bmb_a), (bmb_a,bmb_b)->bmb_a);
				return new Tuple2<BasicMessageBean, Map<String,Object>>(bmb, v._2);					
			}
		});			
	}
	
	/**
	 * Returns the value in message.details.get(field) or any part of the access does not exist.
	 * @param <T>
	 * 
	 * @param message
	 * @param field
	 * @param clazz 
	 * @return
	 */	
	public static <T> T getDetailsMapValue(final BasicMessageBean message, final String field, final Class<T> clazz) {
		return Optional.ofNullable(message)
		.map(m->m.details())
		.map(d->d.get(field))
		.map(r->clazz.cast(r))
		.orElse(null);
	}
	
	/**
	 * Copies the details from copyInto overtop of the details from copyFrom (merging them).  Adds in
	 * key1, value1 after.
	 * @param copyFrom
	 * @param copyInto
	 * @param key1
	 * @param value1
	 * @return
	 */
	public static Map<String, Object> mergeDetailsAddValue(final BasicMessageBean copyFrom, final BasicMessageBean copyInto, final String key1, final Object value1) {
		final Map<String, Object> tempMap = new HashMap<String, Object>(Optional.ofNullable(copyFrom).map(c->c.details()).orElse(ImmutableMap.of())); //copy current map into modifiable map
		tempMap.putAll(Optional.ofNullable(copyInto.details()).orElse(ImmutableMap.of())); //copy newer map over top of old map
		if ( value1 != null ) tempMap.put(key1, value1);	//add in new value
		return ImmutableMap.copyOf(tempMap); //return an immutable map
	}
	/**
	 * Copies the details from copyInto overtop of the details from copyFrom (merging them).  Adds in
	 * key1=value1 and key2=value2 after.
	 * @param copyFrom
	 * @param copyInto
	 * @param key1
	 * @param value1
	 * @param key2
	 * @param value2
	 * @return
	 */
	public static Map<String, Object> mergeDetailsAddValue(final BasicMessageBean copyFrom, final BasicMessageBean copyInto, final String key1, final Object value1, final String key2, final Object value2) {
		final Map<String, Object> tempMap = new HashMap<String, Object>(Optional.ofNullable(copyFrom).map(c->c.details()).orElse(ImmutableMap.of())); //copy current map into modifiable map
		tempMap.putAll(Optional.ofNullable(copyInto.details()).orElse(ImmutableMap.of())); //copy newer map over top of old map
		if ( value1 != null ) tempMap.put(key1, value1);	//add in new value
		if ( value2 != null ) tempMap.put(key2, value2);
		return ImmutableMap.copyOf(tempMap); //return an immutable map
	}
}
