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
package com.ikanow.aleph2.logging.service;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IBasicMessageBeanSupplier;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IBucketLogger;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ILoggingService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBeanSupplier;
import com.ikanow.aleph2.data_model.objects.shared.ManagementSchemaBean.LoggingSchemaBean;
import com.ikanow.aleph2.data_model.utils.BucketUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.logging.utils.Log4JUtils;
import com.ikanow.aleph2.logging.utils.LoggingUtils;

/**
 * This logger is just a passthrough for log4j messages.
 * @author Burch
 *
 */
public class Log4JLoggingService implements ILoggingService {
	final static String date_field = "date";
	private static final BasicMessageBean LOG_MESSAGE_DID_NOT_MATCH_RULE = ErrorUtils.buildSuccessMessage(Log4JBucketLogger.class.getName(), "log", "Log message dropped, did not match rule");
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService#getUnderlyingArtefacts()
	 */
	@Override
	public Collection<Object> getUnderlyingArtefacts() {
		return Arrays.asList(this);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService#getUnderlyingPlatformDriver(java.lang.Class, java.util.Optional)
	 */
	@Override
	public <T> Optional<T> getUnderlyingPlatformDriver(Class<T> driver_class,
			Optional<String> driver_options) {
		return Optional.empty();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ILoggingService#getLogger(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean)
	 */
	@Override
	public IBucketLogger getLogger(DataBucketBean bucket) {
		return new Log4JBucketLogger(bucket, false);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ILoggingService#getSystemLogger(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean)
	 */
	@Override
	public IBucketLogger getSystemLogger(DataBucketBean bucket) {
		return new Log4JBucketLogger(bucket, true);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ILoggingService#getExternalLogger(java.lang.String)
	 */
	@Override
	public IBucketLogger getExternalLogger(String subsystem) {
		return new Log4JBucketLogger(LoggingUtils.getExternalBucket(subsystem, Level.ERROR), true);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ILoggingService#validateSchema(com.ikanow.aleph2.data_model.objects.data_import.ManagementSchemaBean.LoggingSchemaBean, com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean)
	 */
	@Override
	public Tuple2<String, List<BasicMessageBean>> validateSchema(
			LoggingSchemaBean schema, DataBucketBean bucket) {
		return Tuples._2T(BucketUtils.getUniqueSignature(BucketUtils.convertDataBucketBeanToLogging(bucket).full_name(), Optional.empty()),  Collections.emptyList());
	}

	public class Log4JBucketLogger implements IBucketLogger {
		private final Logger logger = LogManager.getLogger();
		private final DataBucketBean bucket;
		private final boolean isSystem;
		private final String hostname;
		final Map<String, Tuple2<BasicMessageBean, Map<String,Object>>> merge_logs;
		
		public Log4JBucketLogger(final DataBucketBean bucket, final boolean isSystem) {
			this.bucket = bucket;
			this.isSystem = isSystem;
			this.merge_logs = new HashMap<String, Tuple2<BasicMessageBean, Map<String,Object>>>();
			this.hostname = LoggingUtils.getHostname();
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IBucketLogger#log(org.apache.logging.log4j.Level, com.ikanow.aleph2.data_model.interfaces.shared_services.IBasicMessageBeanSupplier)
		 */
		@Override
		public CompletableFuture<?> log(Level level,
				IBasicMessageBeanSupplier message) {
			final JsonNode logObject = LoggingUtils.createLogObject(level, bucket, message.getBasicMessageBean(), isSystem, date_field, hostname);
			logger.log(level, Log4JUtils.getLog4JMessage(logObject, level, Thread.currentThread().getStackTrace()[2], date_field, Collections.emptyMap(), hostname));
			return CompletableFuture.completedFuture(true);
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IBucketLogger#log(org.apache.logging.log4j.Level, com.ikanow.aleph2.data_model.interfaces.shared_services.IBasicMessageBeanSupplier, java.lang.String, java.util.function.BiFunction, java.util.Optional)
		 */
		@Override
		public CompletableFuture<?> log(
				final Level level,
				final IBasicMessageBeanSupplier message,
				final String merge_key,
				final Collection<Function<Tuple2<BasicMessageBean, Map<String,Object>>, Boolean>> rule_functions,
				final Optional<Function<BasicMessageBean, BasicMessageBean>> formatter,
				@SuppressWarnings("unchecked") final BiFunction<BasicMessageBean, BasicMessageBean, BasicMessageBean>... merge_operations) {					
			//call operator and replace existing entry (if exists)
			final Tuple2<BasicMessageBean, Map<String,Object>> merge_info = LoggingUtils.getOrCreateMergeInfo(merge_logs, message.getBasicMessageBean(), merge_key, merge_operations);				
			if ( rule_functions.isEmpty() || rule_functions.stream().anyMatch(r->r.apply(merge_info))) {		
				//we are sending a msg, update bmb w/ timestamp and count
				final Tuple2<BasicMessageBean, Map<String,Object>> info = LoggingUtils.updateInfo(merge_info, Optional.of(new Date().getTime()));
				final Tuple2<BasicMessageBean, Map<String,Object>> toWrite = new Tuple2<BasicMessageBean, Map<String,Object>>(formatter.map(f->f.apply(info._1)).orElse(info._1), info._2); //format the message if needbe
				merge_logs.put(merge_key, toWrite);
				final JsonNode logObject = LoggingUtils.createLogObject(level, bucket, toWrite._1, isSystem, date_field, hostname);
				logger.log(level, Log4JUtils.getLog4JMessage(logObject, level, Thread.currentThread().getStackTrace()[2], date_field, Collections.emptyMap(), hostname));								
				return CompletableFuture.completedFuture(true);
			}
			//even if we didn't send a bmb, update the count
			merge_logs.put(merge_key, LoggingUtils.updateInfo(merge_info, Optional.empty()));
			return CompletableFuture.completedFuture(LOG_MESSAGE_DID_NOT_MATCH_RULE);
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IBucketLogger#flush()
		 */
		@Override
		public CompletableFuture<?> flush() {
			return CompletableFuture.completedFuture(true);
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IBucketLogger#inefficientLog(org.apache.logging.log4j.Level, com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean)
		 */
		@Override
		public CompletableFuture<?> inefficientLog(Level level,
				BasicMessageBean message) {
			return this.log(level, new BasicMessageBeanSupplier(message));
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IBucketLogger#log(org.apache.logging.log4j.Level, java.util.function.Supplier, java.util.function.Supplier)
		 */
		@Override
		public CompletableFuture<?> log(Level level, final boolean success, Supplier<String> message,
				Supplier<String> subsystem) {
			return log(level, new BasicMessageBeanSupplier(success, subsystem, ()->null, ()->null, message, ()->null));
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IBucketLogger#log(org.apache.logging.log4j.Level, java.util.function.Supplier, java.util.function.Supplier, java.util.function.Supplier)
		 */
		@Override
		public CompletableFuture<?> log(Level level, final boolean success, Supplier<String> message,
				Supplier<String> subsystem, Supplier<String> command) {
			return log(level, new BasicMessageBeanSupplier(success, subsystem, command, ()->null, message, ()->null));
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IBucketLogger#log(org.apache.logging.log4j.Level, java.util.function.Supplier, java.util.function.Supplier, java.util.function.Supplier, java.util.function.Supplier)
		 */
		@Override
		public CompletableFuture<?> log(Level level, final boolean success, Supplier<String> message,
				Supplier<String> subsystem, Supplier<String> command,
				Supplier<Integer> messageCode) {
			return log(level, new BasicMessageBeanSupplier(success, subsystem, command, messageCode, message, ()->null));
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IBucketLogger#log(org.apache.logging.log4j.Level, java.util.function.Supplier, java.util.function.Supplier, java.util.function.Supplier, java.util.function.Supplier, java.util.function.Supplier)
		 */
		@Override
		public CompletableFuture<?> log(Level level, final boolean success, Supplier<String> message,
				Supplier<String> subsystem, Supplier<String> command,
				Supplier<Integer> messageCode,
				Supplier<Map<String, Object>> details) {
			return log(level, new BasicMessageBeanSupplier(success, subsystem, command, messageCode, message, details));
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IBucketLogger#log(org.apache.logging.log4j.Level, com.ikanow.aleph2.data_model.interfaces.shared_services.IBasicMessageBeanSupplier, java.lang.String, java.util.function.BiFunction[])
		 */
		@Override
		public CompletableFuture<?> log(
				Level level,
				IBasicMessageBeanSupplier message,
				String merge_key,
				@SuppressWarnings("unchecked") BiFunction<BasicMessageBean, BasicMessageBean, BasicMessageBean>... merge_operations) {
			return this.log(level, message, merge_key, Collections.emptyList(), Optional.empty(), merge_operations);
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IBucketLogger#log(org.apache.logging.log4j.Level, com.ikanow.aleph2.data_model.interfaces.shared_services.IBasicMessageBeanSupplier, java.lang.String, java.util.function.Function, java.util.function.BiFunction[])
		 */
		@Override
		public CompletableFuture<?> log(
				Level level,
				IBasicMessageBeanSupplier message,
				String merge_key,
				Function<BasicMessageBean, BasicMessageBean> formatter,
				@SuppressWarnings("unchecked") BiFunction<BasicMessageBean, BasicMessageBean, BasicMessageBean>... merge_operations) {
			return this.log(level, message, merge_key, Collections.emptyList(), Optional.of(formatter), merge_operations);
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IBucketLogger#log(org.apache.logging.log4j.Level, com.ikanow.aleph2.data_model.interfaces.shared_services.IBasicMessageBeanSupplier, java.lang.String, java.util.Collection, java.util.function.BiFunction[])
		 */
		@Override
		public CompletableFuture<?> log(
				Level level,
				IBasicMessageBeanSupplier message,
				String merge_key,
				Collection<Function<Tuple2<BasicMessageBean, Map<String, Object>>, Boolean>> rule_functions,
				@SuppressWarnings("unchecked") BiFunction<BasicMessageBean, BasicMessageBean, BasicMessageBean>... merge_operations) {
			return this.log(level, message, merge_key, rule_functions, Optional.empty(), merge_operations);
		}
		
	}
}
