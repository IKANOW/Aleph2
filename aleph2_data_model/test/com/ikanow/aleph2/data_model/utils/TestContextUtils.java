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

import static org.junit.Assert.*;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext;
import com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadStatusBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;

public class TestContextUtils {

	@Test
	public void testHarvestContextCreation() throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		
		// 1) class by itself
		IHarvestContext context1 = ContextUtils.getHarvestContext("com.ikanow.aleph2.data_model.utils.TestContextUtils$MockHarvestContext");

		assertTrue(context1 instanceof MockHarvestContext);
		assertTrue(context1 instanceof IHarvestContext);
		assertEquals(((MockHarvestContext)context1).dummySignature, null);
		
		// 2) class + configuration
		IHarvestContext context2 = ContextUtils.getHarvestContext("com.ikanow.aleph2.data_model.utils.TestContextUtils$MockHarvestContext:test_signature:includes:");
		
		assertTrue(context2 instanceof MockHarvestContext);
		assertTrue(context2 instanceof IHarvestContext);
		assertEquals(((MockHarvestContext)context2).dummySignature, "test_signature:includes:");
	}

	@Test
	public void testAnalyticsContextCreation() throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		
		// 1) class by itself
		IAnalyticsContext context1 = ContextUtils.getAnalyticsContext("com.ikanow.aleph2.data_model.utils.TestContextUtils$MockAnalyticsContext");

		assertTrue(context1 instanceof MockAnalyticsContext);
		assertTrue(context1 instanceof IAnalyticsContext);
		assertEquals(((MockAnalyticsContext)context1).dummySignature, null);
		
		// 2) class + configuration
		IAnalyticsContext context2 = ContextUtils.getAnalyticsContext("com.ikanow.aleph2.data_model.utils.TestContextUtils$MockAnalyticsContext:test_signature:includes:");
		
		assertTrue(context2 instanceof MockAnalyticsContext);
		assertTrue(context2 instanceof IAnalyticsContext);
		assertEquals(((MockAnalyticsContext)context2).dummySignature, "test_signature:includes:");
	}
	
	
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Test
	public void testHarvestContextCreationFailure() throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		
		exception.expect(ClassNotFoundException.class);
		
		IHarvestContext context1 = ContextUtils.getHarvestContext("com.ikanow.aleph2.data_model.utils.TestContextUtils$DOES_NOT_EXIST");

		context1.getClass();
	}
	
	@Test
	public void testAnalyticsContextCreationFailure() throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		
		exception.expect(ClassNotFoundException.class);
		
		IAnalyticsContext context1 = ContextUtils.getAnalyticsContext("com.ikanow.aleph2.data_model.utils.TestContextUtils$DOES_NOT_EXIST");

		context1.getClass();
	}
	
	// Needed for the tests
	
	public static class MockHarvestContext implements IHarvestContext {

		public MockHarvestContext() {}
		
		String dummySignature = null;
		
		@Override
		public List<String> getHarvestContextLibraries(
				Optional<Set<Class<?>>> services) {
			return null;
		}

		@Override
		public String getHarvestContextSignature(Optional<DataBucketBean> bucket) {
			return null;
		}

		@Override
		public <I> Optional<I> getService(Class<I> service_clazz, Optional<String> service_name) {
			return Optional.empty();
		}

		@Override
		public void initializeNewContext(String signature) {
			dummySignature = signature;
		}

		@Override
		public Future<JsonNode> getGlobalHarvestTechnologyConfiguration() {
			return null;
		}

		@Override
		public Future<Map<String, String>> getHarvestLibraries(
				Optional<DataBucketBean> bucket) {
			return null;
		}

		@Override
		public Future<DataBucketStatusBean> getBucketStatus(
				Optional<DataBucketBean> bucket) {
			return null;
		}

		@Override
		public void logStatusForBucketOwner(Optional<DataBucketBean> bucket,
				@NonNull BasicMessageBean message, boolean roll_up_duplicates) {
		}

		@Override
		public void logStatusForBucketOwner(Optional<DataBucketBean> bucket,
				@NonNull BasicMessageBean message) {
		}

		@Override
		public String getTempOutputLocation(Optional<DataBucketBean> bucket) {
			return null;
		}

		@Override
		public String getFinalOutputLocation(Optional<DataBucketBean> bucket) {
			return null;
		}

		@Override
		public void emergencyDisableBucket(Optional<DataBucketBean> bucket) {
			
		}

		@Override
		public void emergencyQuarantineBucket(Optional<DataBucketBean> bucket,
				@NonNull String quarantineDuration) {
			
		}

		@Override
		public void sendObjectToStreamingPipeline(
				Optional<DataBucketBean> bucket, @NonNull JsonNode object) {
			
		}

		@Override
		public <T> void sendObjectToStreamingPipeline(
				Optional<DataBucketBean> bucket, @NonNull T object) {
			
		}

		@Override
		public void sendObjectToStreamingPipeline(
				Optional<DataBucketBean> bucket,
				@NonNull Map<String, Object> object) {
			
		}

		@Override
		public <S> ICrudService<S> getBucketObjectStore(Class<S> clazz,
				Optional<DataBucketBean> bucket, Optional<String> sub_collection, boolean auto_prepend_prefix) {
			return null;
		}
		
	}
	public static class MockAnalyticsContext implements IAnalyticsContext {
		
		public MockAnalyticsContext() {}
		
		String dummySignature = null;

		@Override
		public <I> Optional<I> getService(@NonNull Class<I> service_clazz,
				@NonNull Optional<String> service_name) {
			return null;
		}

		@Override
		public Future<BasicMessageBean> subscribeToBucket(
				@NonNull DataBucketBean bucket,
				@NonNull Optional<String> stage,
				Consumer<JsonNode> on_new_object_callback) {
			return null;
		}

		@Override
		public Future<BasicMessageBean> subscribeToAnalyticThread(
				@NonNull AnalyticThreadBean analytic_thread,
				@NonNull Optional<String> stage,
				Consumer<JsonNode> on_new_object_callback) {
			return null;
		}

		@Override
		public Future<Stream<JsonNode>> getObjectStreamFromBucket(
				@NonNull DataBucketBean bucket, @NonNull Optional<String> stage) {
			return null;
		}

		@Override
		public Stream<JsonNode> getObjectStreamFromAnalyticThread(
				@NonNull AnalyticThreadBean analytic_thread,
				@NonNull Optional<String> stage) {
			return null;
		}

		@Override
		public List<String> getAnalyticsContextLibraries(
				@NonNull Optional<Set<Class<?>>> services) {
			return null;
		}

		@Override
		public String getAnalyticsContextSignature(
				@NonNull Optional<AnalyticThreadBean> analytic_thread) {
			return null;
		}

		@Override
		public Future<JsonNode> getGlobalAnalyticsTechnologyConfiguration() {
			return null;
		}

		@Override
		public Future<Map<String, String>> getAnalyticsLibraries(
				@NonNull Optional<AnalyticThreadBean> analytic_thread) {
			return null;
		}

		@Override
		public <S> ICrudService<S> getThreadObjectStore(
				@NonNull Class<S> clazz,
				@NonNull Optional<AnalyticThreadBean> analytic_thread,
				@NonNull Optional<String> sub_collection,
				boolean auto_apply_prefix) {
			return null;
		}

		@Override
		public Future<AnalyticThreadStatusBean> getThreadStatus(
				@NonNull Optional<AnalyticThreadBean> analytic_thread) {
			return null;
		}

		@Override
		public void logStatusForThreadOwner(
				@NonNull Optional<AnalyticThreadBean> analytic_thread,
				@NonNull BasicMessageBean message, boolean roll_up_duplicates) {
		}

		@Override
		public void logStatusForThreadOwner(
				@NonNull Optional<AnalyticThreadBean> analytic_thread,
				@NonNull BasicMessageBean message) {
		}

		@Override
		public void emergencyDisableThread(
				@NonNull Optional<AnalyticThreadBean> analytic_thread) {
		}

		@Override
		public void emergencyQuarantineThread(
				@NonNull Optional<AnalyticThreadBean> analytic_thread,
				@NonNull String quarantine_duration) {
		}

		@Override
		public void initializeNewContext(@NonNull String signature) {
			dummySignature = signature;			
		}
	}
	
}
