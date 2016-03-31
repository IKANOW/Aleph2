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
package com.ikanow.aleph2.logging.service;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.ikanow.aleph2.core.shared.services.MultiDataService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IBasicMessageBeanSupplier;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IBucketLogger;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IExtraDependencyLoader;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ILoggingService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.ManagementSchemaBean.LoggingSchemaBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBeanSupplier;
import com.ikanow.aleph2.data_model.utils.BucketUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.logging.data_model.LoggingServiceConfig;
import com.ikanow.aleph2.logging.data_model.LoggingServiceConfigBean;
import com.ikanow.aleph2.logging.module.LoggingServiceModule;
import com.ikanow.aleph2.logging.utils.Log4JUtils;
import com.ikanow.aleph2.logging.utils.LoggingUtils;

/**
 * Implementation of ILoggingService that reads the management schema of a data bucket and writes out
 * to those locations.
 * @author Burch
 *
 */
public class LoggingService implements ILoggingService, IExtraDependencyLoader {
	
	private final static Logger _logger = LogManager.getLogger();
	protected final static Cache<String, MultiDataService> bucket_writable_cache = CacheBuilder.newBuilder().expireAfterAccess(30, TimeUnit.MINUTES).build();
	private static final BasicMessageBean LOG_MESSAGE_BELOW_THRESHOLD = ErrorUtils.buildSuccessMessage(BucketLogger.class.getName(), "log", "Log message dropped, below threshold");
	private static final BasicMessageBean LOG_MESSAGE_DID_NOT_MATCH_RULE = ErrorUtils.buildSuccessMessage(BucketLogger.class.getName(), "log", "Log message dropped, did not match rule");
	
//	protected final LoggingServiceConfigBean properties;
	protected final LoggingServiceConfig properties_converted;
	protected final IServiceContext service_context;
	protected final IStorageService storage_service;
	
	@Inject
	public LoggingService(
			final LoggingServiceConfigBean properties, 
			final IServiceContext service_context) {
//		this.properties = properties;
		this.properties_converted = new LoggingServiceConfig(properties.default_time_field(), properties.default_system_log_level(), properties.default_user_log_level(), properties.system_mirror_to_log4j_level());
		this.service_context = service_context;
		this.storage_service = service_context.getStorageService();		
	}	

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ILoggingService#getLogger(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean)
	 */
	@Override
	public IBucketLogger getLogger(DataBucketBean bucket) {
		return getBucketLogger(bucket, false);		
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ILoggingService#getSystemLogger(java.util.Optional)
	 */
	@Override
	public IBucketLogger getSystemLogger(DataBucketBean bucket) {
		return getBucketLogger(bucket, true);
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ILoggingService#getExternalLogger(java.lang.String)
	 */
	@Override
	public IBucketLogger getExternalLogger(final String subsystem) {
		final DataBucketBean bucket = LoggingUtils.getExternalBucket(subsystem, Optional.ofNullable(properties_converted.default_system_log_level()).orElse(Level.OFF));		
		return getBucketLogger(bucket, true);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ILoggingService#validateSchema(com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.SearchIndexSchemaBean, com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean)
	 */
	@Override
	public Tuple2<String, List<BasicMessageBean>> validateSchema(final LoggingSchemaBean schema, final DataBucketBean bucket) {
		final LinkedList<BasicMessageBean> errors = new LinkedList<>();
		
		//check the fields are actually log4j levels and kick back any errors
		if ( schema.log_level() != null )
			try { Level.valueOf(schema.log_level()); } catch ( Exception ex ) { errors.add(ErrorUtils.buildErrorMessage(this.getClass().getSimpleName(), "log_level '" + schema.log_level() + " is not a log4j.Levels enum value", ex.getMessage())); }
		if ( schema.log_level_overrides() != null ) {
			schema.log_level_overrides().forEach((k,v) -> { try { Level.valueOf(v); } catch ( Exception ex ) { errors.add(ErrorUtils.buildErrorMessage(this.getClass().getSimpleName(), "log_level_override ["+k+"] '" + v + " is not a log4j.Levels enum value", ex.getMessage())); } });
		}
		return errors.isEmpty()
				? Tuples._2T(BucketUtils.getUniqueSignature(BucketUtils.convertDataBucketBeanToLogging(bucket).full_name(), Optional.empty()),  Collections.emptyList())
				: Tuples._2T("",  errors)
				;
	}
	
	/**
	 * Retrieves a writable for the given bucket, trys to find it in the cache first, creates a new one if it can't and adds it to the cache for future requests.
	 * 
	 * @param log_bucket
	 * @return
	 * @throws ExecutionException 
	 */
	private MultiDataService getWritable(final DataBucketBean log_bucket) {
		try {
			return bucket_writable_cache.get(getWritableCacheKey(log_bucket), () -> {
				return LoggingUtils.getLoggingServiceForBucket(service_context, log_bucket);
			});
		} catch (ExecutionException e) {
			//return an empty multiwriter when we've had a failure (this shouldn't occur, but justu to be safe)
			_logger.error("Error getting writable for bucket: " + log_bucket.full_name() + " return an empty logger instead that ignores requests", e);
			return LoggingUtils.getLoggingServiceForBucket(service_context, LoggingUtils.getEmptyBucket());
		}
	}	

	/**
	 * Creates the bucket logger for the given bucket.  First attempts to write the
	 * output path, if that fails returns an exceptioned completable.
	 * 
	 * @param bucket
	 * @param writable
	 * @param b
	 */
	private IBucketLogger getBucketLogger(final DataBucketBean bucket, final boolean isSystem) {
		return new BucketLogger(bucket, getWritable(bucket), isSystem);
	}
	
	/**
	 * Returns the key to cache writables on, currently "bucket.full_name:bucket.modified"
	 * @param bucket
	 * @return
	 */
	private static String getWritableCacheKey(final DataBucketBean bucket) { 
		return bucket.full_name() + ":" + Optional.ofNullable(bucket.modified()).map(d->d.toString());
	}
	
	/**
	 * Implementation of the IBucketLogger that just filters log messages based on the ManagementSchema in
	 * the DatabucketBean and pushes objects into a writable created from the same schema at initialization of this object.
	 * @author Burch
	 *
	 */
	private class BucketLogger implements IBucketLogger {				
		final MultiDataService logging_writable;
		final boolean isSystem;
		final DataBucketBean bucket;
		final String date_field;
		final Level default_log_level;  //holds the default log level for quick matching
		final Level log4j_level;
		final ImmutableMap<String, Level> bucket_logging_thresholds; //holds bucket logging overrides for quick matching
		final Map<String, Tuple2<BasicMessageBean, Map<String,Object>>> merge_logs;
		
		public BucketLogger(final DataBucketBean bucket, final MultiDataService logging_writable, final boolean isSystem) {
			this.bucket = bucket;
			this.logging_writable = logging_writable;
			this.isSystem = isSystem;
			this.log4j_level = isSystem ? properties_converted.system_mirror_to_log4j_level() : Level.OFF;
			this.bucket_logging_thresholds = LoggingUtils.getBucketLoggingThresholds(bucket);
			this.date_field = Optional.ofNullable(properties_converted.default_time_field()).orElse("date");
			this.default_log_level = isSystem ? Optional.ofNullable(properties_converted.default_system_log_level()).orElse(Level.OFF) : Optional.ofNullable(properties_converted.default_user_log_level()).orElse(Level.OFF);	
			this.merge_logs = new HashMap<String, Tuple2<BasicMessageBean, Map<String,Object>>>();
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IBucketLogger#flush()
		 */
		@Override
		public CompletableFuture<?> flush() {
			return logging_writable.flushBatchOutput();
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IBucketLogger#inefficientLog(org.apache.logging.log4j.Level, com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean)
		 */
		@Override
		public CompletableFuture<?> inefficientLog(Level level, BasicMessageBean message) {
			final boolean log_out = LoggingUtils.meetsLogLevelThreshold(level, bucket_logging_thresholds, message.source(), default_log_level); //need to log to multiwriter
			final boolean log_log4j = isSystem && log4j_level.isLessSpecificThan(level); //need to log to log4j
			if ( log_out || log_log4j ) {
				//create log message to output:				
				final JsonNode logObject = LoggingUtils.createLogObject(level, bucket, message, isSystem, date_field);				
				if ( log_log4j )					
					_logger.log(level, Log4JUtils.getLog4JMessage(logObject, level, Thread.currentThread().getStackTrace()[2], date_field, message.details()));
				if ( log_out )
					return CompletableFuture.completedFuture(logging_writable.batchWrite(logObject));				
			}
			return CompletableFuture.completedFuture(LOG_MESSAGE_BELOW_THRESHOLD);		
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IBucketLogger#log(org.apache.logging.log4j.Level, com.ikanow.aleph2.data_model.interfaces.shared_services.IBasicMessageBeanSupplier)
		 */
		@Override
		public CompletableFuture<?> log(Level level, IBasicMessageBeanSupplier message) {
			final boolean log_out = LoggingUtils.meetsLogLevelThreshold(level, bucket_logging_thresholds, message.getSubsystem(), default_log_level); //need to log to multiwriter
			final boolean log_log4j = isSystem && log4j_level.isLessSpecificThan(level); //need to log to log4j
			if ( log_out || log_log4j ) {
				//create log message to output:			
				final BasicMessageBean bmb = message.getBasicMessageBean();
				final JsonNode logObject = LoggingUtils.createLogObject(level, bucket, bmb, isSystem, date_field);				
				if ( log_log4j )					
					_logger.log(level, Log4JUtils.getLog4JMessage(logObject, level, Thread.currentThread().getStackTrace()[2], date_field, bmb.details()));
				if ( log_out )
					return CompletableFuture.completedFuture(logging_writable.batchWrite(logObject));				
			}
			return CompletableFuture.completedFuture(LOG_MESSAGE_BELOW_THRESHOLD);	
		}
		
		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IBucketLogger#log(org.apache.logging.log4j.Level, com.ikanow.aleph2.data_model.interfaces.shared_services.IBasicMessageBeanSupplier, java.lang.String, java.util.function.BiFunction)
		 */
		@Override
		public CompletableFuture<?> log(
				final Level level,
				final IBasicMessageBeanSupplier message,
				final String merge_key,				
				final Optional<Function<Tuple2<BasicMessageBean, Map<String,Object>>, Boolean>> rule_function,
				@SuppressWarnings("unchecked") final BiFunction<BasicMessageBean, BasicMessageBean, BasicMessageBean>... merge_operations) {
			final boolean log_out = LoggingUtils.meetsLogLevelThreshold(level, bucket_logging_thresholds, message.getSubsystem(), default_log_level); //need to log to multiwriter
			final boolean log_log4j = isSystem && log4j_level.isLessSpecificThan(level); //need to log to log4j
			if ( log_out || log_log4j ) {				
				//call operator and replace existing entry (if exists)
				final Tuple2<BasicMessageBean, Map<String,Object>> merge_info = LoggingUtils.getOrCreateMergeInfo(merge_logs, message.getBasicMessageBean(), merge_key, merge_operations);				
				if ( rule_function.map(r->r.apply(merge_info)).orElse(true) ) {					
					//we are sending a msg, update bmb w/ timestamp and count
					merge_logs.put(merge_key, LoggingUtils.updateInfo(merge_info, Optional.of(new Date().getTime())));
					final JsonNode logObject = LoggingUtils.createLogObject(level, bucket, merge_info._1, isSystem, date_field);
					if ( log_log4j )					
						_logger.log(level, Log4JUtils.getLog4JMessage(logObject, level, Thread.currentThread().getStackTrace()[2], date_field, merge_info._1.details()));
					if ( log_out )
						logging_writable.batchWrite(logObject);					
					return CompletableFuture.completedFuture(true);
				}
				//even if we didn't send a bmb, update the count
				merge_logs.put(merge_key, LoggingUtils.updateInfo(merge_info, Optional.empty()));
				return CompletableFuture.completedFuture(LOG_MESSAGE_DID_NOT_MATCH_RULE);
			}
			return CompletableFuture.completedFuture(LOG_MESSAGE_BELOW_THRESHOLD);	
		}	

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IBucketLogger#log(org.apache.logging.log4j.Level, java.util.function.Supplier, java.util.function.Supplier)
		 */
		@Override
		public CompletableFuture<?> log(Level level, final boolean success, Supplier<String> message, Supplier<String> subsystem) {
			return this.log(level, new BasicMessageBeanSupplier(success, subsystem, ()->null, ()->null, message, ()->null));
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IBucketLogger#log(org.apache.logging.log4j.Level, java.util.function.Supplier, java.util.function.Supplier, java.util.function.Supplier)
		 */
		@Override
		public CompletableFuture<?> log(Level level, final boolean success, Supplier<String> message,
				Supplier<String> subsystem, Supplier<String> command) {
			return this.log(level, new BasicMessageBeanSupplier(success, subsystem, command, ()->null, message, ()->null));
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IBucketLogger#log(org.apache.logging.log4j.Level, java.util.function.Supplier, java.util.function.Supplier, java.util.function.Supplier, java.util.function.Supplier)
		 */
		@Override
		public CompletableFuture<?> log(Level level, final boolean success, Supplier<String> message,
				Supplier<String> subsystem, Supplier<String> command,
				Supplier<Integer> messageCode) {
			return this.log(level, new BasicMessageBeanSupplier(success, subsystem, command, messageCode, message, ()->null));
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IBucketLogger#log(org.apache.logging.log4j.Level, java.util.function.Supplier, java.util.function.Supplier, java.util.function.Supplier, java.util.function.Supplier, java.util.function.Supplier)
		 */
		@Override
		public CompletableFuture<?> log(Level level, final boolean success, Supplier<String> message,
				Supplier<String> subsystem, Supplier<String> command,
				Supplier<Integer> messageCode,
				Supplier<Map<String, Object>> details) {
			return this.log(level, new BasicMessageBeanSupplier(success, subsystem, command, messageCode, message, details));
		}		
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService#getUnderlyingArtefacts()
	 */
	@Override
	public Collection<Object> getUnderlyingArtefacts() {
		return Stream.concat(Stream.of(this), service_context.getSearchIndexService().map(Stream::of).orElse(Stream.empty())).collect(Collectors.toList());
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
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IExtraDependencyLoader#youNeedToImplementTheStaticFunctionCalled_getExtraDependencyModules()
	 */
	@Override
	public void youNeedToImplementTheStaticFunctionCalled_getExtraDependencyModules() {
		//done see getExtraDependencyModules
	}
	
	/**
	 * Load the extra services (aka the config bean)
	 * @return
	 */
	public static List<Module> getExtraDependencyModules() {
        return Arrays.asList((Module)new LoggingServiceModule());
    }
}
