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
package com.ikanow.aleph2.data_model.interfaces.shared_services;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.logging.log4j.Level;

import scala.Tuple2;

import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;

/**
 * @author Burch
 *
 */
public interface IBucketLogger {
	//non merging (simple interfaces)
	public CompletableFuture<?> log(final Level level, final boolean success, final Supplier<String> message, final Supplier<String> subsystem);
	public CompletableFuture<?> log(final Level level, final boolean success, final Supplier<String> message, final Supplier<String> subsystem, final Supplier<String> command);
	public CompletableFuture<?> log(final Level level, final boolean success, final Supplier<String> message, final Supplier<String> subsystem, final Supplier<String> command, final Supplier<Integer> messageCode);
	public CompletableFuture<?> log(final Level level, final boolean success, final Supplier<String> message, final Supplier<String> subsystem, final Supplier<String> command, final Supplier<Integer> messageCode, final Supplier<Map<String,Object>> details);
	public CompletableFuture<?> log(final Level level, final IBasicMessageBeanSupplier message);
	
	//non merging, non supplier interface
	public CompletableFuture<?> inefficientLog(final Level level, final BasicMessageBean message);
		
	//merge interfaces	
	public CompletableFuture<?> log(final Level level, final IBasicMessageBeanSupplier message, final String merge_key, @SuppressWarnings("unchecked") final BiFunction<BasicMessageBean, BasicMessageBean, BasicMessageBean>... merge_operations);
	public CompletableFuture<?> log(final Level level, final IBasicMessageBeanSupplier message, final String merge_key, final Function<BasicMessageBean, BasicMessageBean> formatter, @SuppressWarnings("unchecked") final BiFunction<BasicMessageBean, BasicMessageBean, BasicMessageBean>... merge_operations);
	public CompletableFuture<?> log(final Level level, final IBasicMessageBeanSupplier message, final String merge_key, final Collection<Function<Tuple2<BasicMessageBean, Map<String,Object>>, Boolean>> rule_functions, @SuppressWarnings("unchecked") final BiFunction<BasicMessageBean, BasicMessageBean, BasicMessageBean>... merge_operations);
	public CompletableFuture<?> log(final Level level, final IBasicMessageBeanSupplier message, final String merge_key, final Collection<Function<Tuple2<BasicMessageBean, Map<String,Object>>, Boolean>> rule_functions, final Optional<Function<BasicMessageBean, BasicMessageBean>> formatter, @SuppressWarnings("unchecked") final BiFunction<BasicMessageBean, BasicMessageBean, BasicMessageBean>... merge_operations);
	
	//util interface
	public CompletableFuture<?> flush();
}
