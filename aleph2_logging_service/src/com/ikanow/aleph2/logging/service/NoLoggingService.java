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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.logging.log4j.Level;

import scala.Tuple2;

import com.ikanow.aleph2.data_model.interfaces.shared_services.IBasicMessageBeanSupplier;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IBucketLogger;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ILoggingService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.ManagementSchemaBean.LoggingSchemaBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.BucketUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;

/**
 * This logger does absolutely nothing, is useful when you want to just turn off logging.
 * @author Burch
 *
 */
public class NoLoggingService implements ILoggingService {

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService#getUnderlyingArtefacts()
	 */
	@Override
	public Collection<Object> getUnderlyingArtefacts() {
		return Collections.emptyList();
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
		return new NoBucketLogger();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ILoggingService#getSystemLogger(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean)
	 */
	@Override
	public IBucketLogger getSystemLogger(DataBucketBean bucket) {
		return new NoBucketLogger();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ILoggingService#getExternalLogger(java.lang.String)
	 */
	@Override
	public IBucketLogger getExternalLogger(String subsystem) {
		return new NoBucketLogger();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ILoggingService#validateSchema(com.ikanow.aleph2.data_model.objects.data_import.ManagementSchemaBean.LoggingSchemaBean, com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean)
	 */
	@Override
	public Tuple2<String, List<BasicMessageBean>> validateSchema(
			LoggingSchemaBean schema, DataBucketBean bucket) {
		return Tuples._2T(BucketUtils.getUniqueSignature(BucketUtils.convertDataBucketBeanToLogging(bucket).full_name(), Optional.empty()),  Collections.emptyList());
	}

	public class NoBucketLogger implements IBucketLogger {

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
			return CompletableFuture.completedFuture(true);
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IBucketLogger#log(org.apache.logging.log4j.Level, com.ikanow.aleph2.data_model.interfaces.shared_services.IBasicMessageBeanSupplier)
		 */
		@Override
		public CompletableFuture<?> log(Level level,
				IBasicMessageBeanSupplier message) {
			return CompletableFuture.completedFuture(true);
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IBucketLogger#log(org.apache.logging.log4j.Level, com.ikanow.aleph2.data_model.interfaces.shared_services.IBasicMessageBeanSupplier, java.lang.String, java.util.function.BiFunction, java.util.Optional)
		 */
		@Override
		public CompletableFuture<?> log(
				Level level,
				IBasicMessageBeanSupplier message,
				String merge_key,
				BiFunction<BasicMessageBean, BasicMessageBean, BasicMessageBean> merge_operation,
				final Optional<Function<Tuple2<BasicMessageBean, Map<String,Object>>, Boolean>> rule_function) {
			// TODO Auto-generated method stub
			return null;
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IBucketLogger#log(org.apache.logging.log4j.Level, java.util.function.Supplier, java.util.function.Supplier)
		 */
		@Override
		public CompletableFuture<?> log(Level level, final boolean success, Supplier<String> message,
				Supplier<String> subsystem) {
			// TODO Auto-generated method stub
			return null;
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IBucketLogger#log(org.apache.logging.log4j.Level, java.util.function.Supplier, java.util.function.Supplier, java.util.function.Supplier)
		 */
		@Override
		public CompletableFuture<?> log(Level level, final boolean success, Supplier<String> message,
				Supplier<String> subsystem, Supplier<String> command) {
			// TODO Auto-generated method stub
			return null;
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IBucketLogger#log(org.apache.logging.log4j.Level, java.util.function.Supplier, java.util.function.Supplier, java.util.function.Supplier, java.util.function.Supplier)
		 */
		@Override
		public CompletableFuture<?> log(Level level, final boolean success, Supplier<String> message,
				Supplier<String> subsystem, Supplier<String> command,
				Supplier<Integer> messageCode) {
			// TODO Auto-generated method stub
			return null;
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IBucketLogger#log(org.apache.logging.log4j.Level, java.util.function.Supplier, java.util.function.Supplier, java.util.function.Supplier, java.util.function.Supplier, java.util.function.Supplier)
		 */
		@Override
		public CompletableFuture<?> log(Level level, final boolean success, Supplier<String> message,
				Supplier<String> subsystem, Supplier<String> command,
				Supplier<Integer> messageCode,
				Supplier<Map<String, Object>> details) {
			// TODO Auto-generated method stub
			return null;
		}
		
	}
}
